"""
ICS 核心调度引擎
- 保底席位（guaranteed）：每个客户端 min 个，预先保留，始终可用
- 弹性席位（dynamic）：共享池 = TOTAL_CAPACITY - Σmin，按需分配
- 优先级：保底等待请求(P1) > 弹性等待请求(P2)
"""

import asyncio
import heapq
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from .models import SlotType

logger = logging.getLogger("ics.scheduler")


# ── 内部数据结构 ───────────────────────────────────────────────────────────────

@dataclass
class ClientState:
    client_id: str
    client_name: Optional[str]
    min: int
    max: int
    timeout_ms: int
    in_use_guaranteed: int = 0
    in_use_dynamic: int = 0
    wait_queue_depth: int = 0
    acq_total: int = 0
    acq_ok: int = 0


@dataclass
class Token:
    token_id: str
    client_id: str
    slot_type: SlotType
    issued_at: float
    expire_at: float


@dataclass
class _Waiter:
    priority: int      # 1=保底请求, 2=弹性请求
    seq: int           # 全局序号，保证堆内稳定排序
    event: asyncio.Event = field(default_factory=asyncio.Event)
    cancelled: bool = False


# ── 调度引擎 ──────────────────────────────────────────────────────────────────

class Scheduler:
    def __init__(self, total_capacity: int):
        self.total_capacity = total_capacity
        self._clients: Dict[str, ClientState] = {}
        self._tokens: Dict[str, Token] = {}
        self._lock = asyncio.Lock()
        self._heap: List[tuple] = []   # (priority, seq, _Waiter)
        self._seq = 0
        self._event_buf: List[dict] = []

    # ── 事件发射 ─────────────────────────────────────────────────────────────

    def _emit(self, ev_type: str, client_id: str, slot: str, token_id: str = ""):
        self._event_buf.append({
            "type": ev_type,
            "client_id": client_id,
            "slot": slot,
            "token_id": token_id,
        })

    def pop_events(self) -> list:
        evts = self._event_buf[:]
        self._event_buf.clear()
        return evts

    # ── 只读属性 ─────────────────────────────────────────────────────────────

    @property
    def _sum_min(self) -> int:
        return sum(c.min for c in self._clients.values())

    @property
    def dynamic_pool_total(self) -> int:
        return self.total_capacity - self._sum_min

    @property
    def _dynamic_in_use(self) -> int:
        return sum(c.in_use_dynamic for c in self._clients.values())

    @property
    def dynamic_pool_available(self) -> int:
        return self.dynamic_pool_total - self._dynamic_in_use

    # ── 注册 / 更新 / 注销 ───────────────────────────────────────────────────

    async def register(
        self,
        client_id: str,
        client_name: Optional[str],
        min_c: int,
        max_c: int,
        timeout_ms: int,
    ) -> dict:
        async with self._lock:
            if client_id in self._clients:
                raise ValueError(f"1003: client_id '{client_id}' already registered")
            if self._sum_min + min_c > self.total_capacity:
                raise ValueError(
                    f"1001: guaranteed capacity exceeded "
                    f"(need {self._sum_min + min_c}, total {self.total_capacity})"
                )
            if max_c > self.total_capacity:
                raise ValueError(f"1002: max exceeds total_capacity {self.total_capacity}")
            self._clients[client_id] = ClientState(
                client_id=client_id,
                client_name=client_name or client_id,
                min=min_c,
                max=max_c,
                timeout_ms=timeout_ms,
            )
            logger.info("registered client=%s min=%d max=%d", client_id, min_c, max_c)
            return {"client_id": client_id, "dynamic_pool_remaining": self.dynamic_pool_available}

    async def update(
        self,
        client_id: str,
        client_name: Optional[str] = None,
        min_c: Optional[int] = None,
        max_c: Optional[int] = None,
        timeout_ms: Optional[int] = None,
    ):
        async with self._lock:
            c = self._clients.get(client_id)
            if not c:
                raise KeyError(f"client '{client_id}' not found")
            new_min = min_c if min_c is not None else c.min
            new_max = max_c if max_c is not None else c.max
            if new_max < new_min:
                raise ValueError("max must be >= min")
            new_sum = self._sum_min - c.min + new_min
            if new_sum > self.total_capacity:
                raise ValueError(f"1001: new min would exceed guaranteed capacity")
            if new_max > self.total_capacity:
                raise ValueError(f"1002: new max exceeds total_capacity")
            if client_name is not None:
                c.client_name = client_name
            c.min = new_min
            c.max = new_max
            if timeout_ms is not None:
                c.timeout_ms = timeout_ms

    async def unregister(self, client_id: str, force: bool = False):
        async with self._lock:
            c = self._clients.get(client_id)
            if not c:
                raise KeyError(f"client '{client_id}' not found")
            active = [t for t in self._tokens.values() if t.client_id == client_id]
            if active and not force:
                raise RuntimeError(
                    f"client has {len(active)} active token(s); release them first or use force=true"
                )
            for tok in active:
                del self._tokens[tok.token_id]
            del self._clients[client_id]
            logger.info("unregistered client=%s (force=%s)", client_id, force)

    # ── 获取令牌 ─────────────────────────────────────────────────────────────

    async def acquire(self, client_id: str, wait_timeout_ms: int = 5000) -> Token:
        deadline = time.monotonic() + wait_timeout_ms / 1000.0
        waiter: Optional[_Waiter] = None
        first = True

        while True:
            async with self._lock:
                c = self._clients.get(client_id)
                if not c:
                    raise KeyError(f"2002: client '{client_id}' not registered")

                if first:
                    c.acq_total += 1
                    first = False

                # 清理上一轮等待（被唤醒后重试）
                if waiter is not None:
                    c.wait_queue_depth = max(0, c.wait_queue_depth - 1)
                    waiter = None

                # Step1: 尝试保底席位
                if c.in_use_guaranteed < c.min:
                    c.in_use_guaranteed += 1
                    c.acq_ok += 1
                    tok = self._make_token(client_id, SlotType.guaranteed, c.timeout_ms)
                    logger.debug("acquire guaranteed client=%s token=%s", client_id, tok.token_id)
                    self._emit("acquire", client_id, "guaranteed", tok.token_id)
                    return tok

                # Step2: 尝试弹性席位
                if (c.in_use_guaranteed + c.in_use_dynamic < c.max
                        and self.dynamic_pool_available > 0):
                    c.in_use_dynamic += 1
                    c.acq_ok += 1
                    tok = self._make_token(client_id, SlotType.dynamic, c.timeout_ms)
                    logger.debug("acquire dynamic   client=%s token=%s", client_id, tok.token_id)
                    self._emit("acquire", client_id, "dynamic", tok.token_id)
                    return tok

                # Step3: 无可用资源，进入等待队列
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("2001: resource exhausted, wait timeout")

                priority = 1 if c.in_use_guaranteed < c.min else 2
                self._seq += 1
                waiter = _Waiter(priority=priority, seq=self._seq)
                heapq.heappush(self._heap, (priority, self._seq, waiter))
                c.wait_queue_depth += 1
                logger.debug("queued client=%s priority=%d depth=%d", client_id, priority, c.wait_queue_depth)
                self._emit("queue", client_id, "guaranteed" if priority == 1 else "dynamic", "")

            # ── 锁外等待 ──────────────────────────────────────────────────────
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                await self._cancel_waiter(client_id, waiter)
                raise TimeoutError("2001: resource exhausted, wait timeout")

            try:
                await asyncio.wait_for(waiter.event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                await self._cancel_waiter(client_id, waiter)
                raise TimeoutError("2001: resource exhausted, wait timeout")

    async def _cancel_waiter(self, client_id: str, waiter: _Waiter):
        async with self._lock:
            waiter.cancelled = True
            c = self._clients.get(client_id)
            if c:
                c.wait_queue_depth = max(0, c.wait_queue_depth - 1)

    # ── 归还令牌 ─────────────────────────────────────────────────────────────

    async def release(self, client_id: str, token_id: str):
        async with self._lock:
            tok = self._tokens.get(token_id)
            if not tok:
                raise KeyError(f"3001: token '{token_id}' not found or already expired")
            if tok.client_id != client_id:
                raise PermissionError(f"3002: token does not belong to '{client_id}'")

            del self._tokens[token_id]
            c = self._clients[client_id]
            if tok.slot_type == SlotType.guaranteed:
                c.in_use_guaranteed = max(0, c.in_use_guaranteed - 1)
            else:
                c.in_use_dynamic = max(0, c.in_use_dynamic - 1)

            logger.debug("release %s client=%s", tok.slot_type, client_id)
            self._emit("release", client_id, tok.slot_type.value, token_id)
            self._wake_next()

    # ── 超时回收（后台任务调用）─────────────────────────────────────────────

    async def expire_tokens(self) -> int:
        now = time.time()
        async with self._lock:
            expired = [t for t in self._tokens.values() if t.expire_at < now]
            for tok in expired:
                del self._tokens[tok.token_id]
                c = self._clients.get(tok.client_id)
                if c:
                    if tok.slot_type == SlotType.guaranteed:
                        c.in_use_guaranteed = max(0, c.in_use_guaranteed - 1)
                    else:
                        c.in_use_dynamic = max(0, c.in_use_dynamic - 1)
                logger.warning("token expired client=%s token=%s", tok.client_id, tok.token_id)
                self._emit("timeout", tok.client_id, tok.slot_type.value, tok.token_id)
            if expired:
                self._wake_next()
        return len(expired)

    # ── 状态查询 ─────────────────────────────────────────────────────────────

    def get_status(self) -> dict:
        return {
            "total_capacity": self.total_capacity,
            "dynamic_pool_total": self.dynamic_pool_total,
            "dynamic_pool_available": self.dynamic_pool_available,
            "clients": [
                {
                    "client_id": c.client_id,
                    "client_name": c.client_name,
                    "min": c.min,
                    "max": c.max,
                    "in_use_guaranteed": c.in_use_guaranteed,
                    "in_use_dynamic": c.in_use_dynamic,
                    "wait_queue_depth": c.wait_queue_depth,
                    "acq_total": c.acq_total,
                    "acq_ok": c.acq_ok,
                }
                for c in self._clients.values()
            ],
        }

    # ── 内部工具 ─────────────────────────────────────────────────────────────

    def _make_token(self, client_id: str, slot_type: SlotType, timeout_ms: int) -> Token:
        tok = Token(
            token_id=f"tok_{uuid.uuid4().hex[:12]}",
            client_id=client_id,
            slot_type=slot_type,
            issued_at=time.time(),
            expire_at=time.time() + timeout_ms / 1000.0,
        )
        self._tokens[tok.token_id] = tok
        return tok

    def _wake_next(self):
        """唤醒优先级最高的等待者（P1保底 > P2弹性），惰性跳过已取消的。"""
        while self._heap:
            _, _, waiter = heapq.heappop(self._heap)
            if not waiter.cancelled:
                waiter.event.set()
                return
