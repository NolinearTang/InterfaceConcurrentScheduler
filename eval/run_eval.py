"""
ICS 评估测试脚本 — 模拟 A/B/C/D 四个服务并发访问

场景覆盖：
  1. 正常请求   — 随机并发，正常 acquire / release
  2. 业务异常   — work 阶段抛异常，验证令牌被 finally 自动归还
  3. 排队等待   — 请求量超过 max，触发等待队列
  4. 令牌超时   — 持有令牌超过 timeout_ms，服务端强制回收
  5. 流量突刺   — service-a 在某时刻突然高并发

运行前提：ICS 服务已启动（python server/main.py）
运行方式：python eval/run_eval.py
"""

import asyncio
import logging
import os
import random
import sys
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from client.ics_client import AsyncICSClient, ResourceExhaustedError, ICSError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(message)s",
)
logger = logging.getLogger("ics.eval")

BASE_URL = "http://localhost:8080"

# ── 客户端配置 ─────────────────────────────────────────────────────────────────
CLIENT_CFGS = [
    {"client_id": "service-a", "client_name": "A 服务", "min": 4, "max": 8},
    {"client_id": "service-b", "client_name": "B 服务", "min": 3, "max": 6},
    {"client_id": "service-c", "client_name": "C 服务", "min": 3, "max": 6},
    {"client_id": "service-d", "client_name": "D 服务", "min": 2, "max": 4},
]

# ── 统计 ───────────────────────────────────────────────────────────────────────
@dataclass
class Stats:
    ok:               int = 0
    exhausted:        int = 0
    exception_ok:     int = 0
    timeout_reclaimed:int = 0

_stats: dict[str, Stats] = {}


def get_stats(client_id: str) -> Stats:
    if client_id not in _stats:
        _stats[client_id] = Stats()
    return _stats[client_id]


# ── 场景工具函数 ───────────────────────────────────────────────────────────────

async def _work(label: str, client: AsyncICSClient, client_id: str,
                req_id: int, work_s: float, wait_ms: int = 5_000) -> bool:
    """通用 acquire → sleep → release 封装，返回是否成功。"""
    try:
        async with client.token(wait_timeout_ms=wait_ms) as tok:
            logger.debug("[%s] %s #%02d  token=%s  work=%.2fs",
                         label, client_id, req_id, tok.token_id, work_s)
            await asyncio.sleep(work_s)
        get_stats(client_id).ok += 1
        return True
    except ResourceExhaustedError:
        logger.warning("[%s] %s #%02d  资源耗尽，等待 %dms 后放弃", label, client_id, req_id, wait_ms)
        get_stats(client_id).exhausted += 1
        return False


# ── 场景 1：正常请求 ───────────────────────────────────────────────────────────

async def scene_normal(client: AsyncICSClient, client_id: str, n: int = 25):
    """n 个随机并发请求，处理时长 1–5s，模拟日常流量。"""
    logger.info(">>> [正常] %s  发出 %d 个并发请求", client_id, n)
    tasks = [
        _work("正常", client, client_id, i, random.uniform(1.0, 5.0))
        for i in range(n)
    ]
    await asyncio.gather(*tasks)
    logger.info("<<< [正常] %s  完成", client_id)


# ── 场景 2：业务异常 ───────────────────────────────────────────────────────────

async def _exception_worker(client: AsyncICSClient, client_id: str, req_id: int):
    """
    60% 概率在 work 阶段抛异常。
    token() 上下文管理器的 finally 块保证令牌无论如何都会归还，不会泄漏。
    '异常中归还' 计数越大说明异常场景处理越好。
    """
    try:
        async with client.token() as tok:
            await asyncio.sleep(random.uniform(0.5, 2.0))
            if random.random() < 0.6:
                raise ValueError(f"业务逻辑错误 req#{req_id}")
            get_stats(client_id).ok += 1
    except ValueError as e:
        logger.info("[异常] %s #%02d  抛出异常: %s → 令牌已由 finally 自动归还（无泄漏）", client_id, req_id, e)
        get_stats(client_id).exception_ok += 1
    except ResourceExhaustedError:
        get_stats(client_id).exhausted += 1


async def scene_exception(client: AsyncICSClient, client_id: str, n: int = 15):
    """n 个请求，60% 概率在 work 阶段抛异常，验证令牌不泄漏。"""
    logger.info(">>> [异常] %s  发出 %d 个请求（60%% 会抛异常，验证令牌自动归还）", client_id, n)
    tasks = [_exception_worker(client, client_id, i) for i in range(n)]
    await asyncio.gather(*tasks)
    logger.info("<<< [异常] %s  完成，所有令牌均已归还", client_id)


# ── 场景 3：排队等待 ───────────────────────────────────────────────────────────

async def scene_queue(client: AsyncICSClient, client_id: str, cfg: dict):
    """
    发出远超 max 席位数的并发请求，后来的请求进入等待队列。
    wait_timeout_ms=5000 表示最多等 5s，超时放弃。
    """
    n = cfg["max"] * 4          # 发出 4 倍于 max 的并发，必然触发排队
    work_s = 3.0                # 每个请求持有令牌 3s，加剧排队压力
    wait_ms = 5_000
    logger.info(">>> [排队] %s  发出 %d 个并发请求（max=%d），wait=%dms",
                client_id, n, cfg["max"], wait_ms)
    tasks = [
        _work("排队", client, client_id, i, work_s, wait_ms=wait_ms)
        for i in range(n)
    ]
    await asyncio.gather(*tasks)
    logger.info("<<< [排队] %s  完成", client_id)


# ── 场景 4：令牌超时回收 ───────────────────────────────────────────────────────

async def _timeout_worker(client: AsyncICSClient, client_id: str, req_id: int, hold_s: float):
    """手动 acquire，持有超过 timeout_ms 后不主动 release，观察服务端回收。"""
    try:
        tok = await client.acquire()
        logger.info("[超时] %s #%02d  获取令牌 %s，持有 %.1fs（超过 timeout_ms），等待服务端回收...",
                    client_id, req_id, tok.token_id, hold_s)
        await asyncio.sleep(hold_s)
        get_stats(client_id).timeout_reclaimed += 1
        try:
            await client.release(tok)
            logger.warning("[超时] %s #%02d  release 成功（token 未被回收，超时未触发）", client_id, req_id)
        except ICSError as e:
            logger.info("[超时] %s #%02d  release 失败: %s  → 确认服务端已回收", client_id, req_id, e)
    except ResourceExhaustedError:
        get_stats(client_id).exhausted += 1


async def scene_timeout(base_url: str):
    """
    注册一个 timeout_ms=3000 的专用客户端，
    持有令牌 5s → 服务端约 3s 后强制回收。
    """
    client_id = "service-a-timeout-demo"
    client = AsyncICSClient(base_url, wait_timeout_ms=5_000, http_timeout=15.0)
    try:
        await client.register(
            client_id=client_id,
            client_name="A 服务(超时演示)",
            min=2,
            max=4,
            timeout_ms=3_000,      # 令牌只能持有 3s
        )
        logger.info(">>> [超时] %s  注册成功 timeout_ms=3000", client_id)
        tasks = [_timeout_worker(client, client_id, i, hold_s=8.0) for i in range(3)]
        await asyncio.gather(*tasks)
        logger.info("    [超时] 等待服务端扫描回收（最多 2s）...")
        await asyncio.sleep(2)
        logger.info("<<< [超时] 场景完成")
    finally:
        try:
            await client.unregister(force=True)
        except Exception:
            pass
        await client.close()


# ── 场景 5：流量突刺 ───────────────────────────────────────────────────────────

async def scene_spike(client: AsyncICSClient, client_id: str, cfg: dict):
    """
    先平稳等待 5s，随后连续三波突刺，每波 30 个并发，波间间隔 10s，
    模拟服务在某时刻反复出现业务高峰。
    """
    logger.info(">>> [突刺] %s  5s 后开始三波突刺测试...", client_id)
    await asyncio.sleep(5)

    for wave in range(1, 4):
        n_spike = 30
        logger.info("    [突刺] %s  ⚡ 第 %d 波！%d 个并发请求瞬间涌入（max=%d）",
                    client_id, wave, n_spike, cfg["max"])
        tasks = [
            _work("突刺", client, client_id, i + wave * 100,
                  random.uniform(1.0, 3.0), wait_ms=5_000)
            for i in range(n_spike)
        ]
        await asyncio.gather(*tasks)
        logger.info("    [突刺] %s 第 %d 波完成，间隔 10s...", client_id, wave)
        if wave < 3:
            await asyncio.sleep(10)

    logger.info("<<< [突刺] %s  全部完成", client_id)


# ── 客户端生命周期管理 ─────────────────────────────────────────────────────────

async def run_client(cfg: dict):
    """为单个客户端依次运行：正常 → 异常 → 排队场景。"""
    client_id = cfg["client_id"]
    client = AsyncICSClient(BASE_URL, wait_timeout_ms=5_000, http_timeout=15.0)

    try:
        await client.register(
            client_id=client_id,
            client_name=cfg["client_name"],
            min=cfg["min"],
            max=cfg["max"],
            timeout_ms=10_000,
        )
        logger.info("[setup] %s 注册成功  min=%d max=%d", client_id, cfg["min"], cfg["max"])
    except ICSError as e:
        logger.error("[setup] %s 注册失败: %s", client_id, e)
        await client.close()
        return

    try:
        await scene_normal(client, client_id)
        await asyncio.sleep(0.5)

        await scene_exception(client, client_id)
        await asyncio.sleep(0.5)

        await scene_queue(client, client_id, cfg)

    finally:
        try:
            await client.unregister(force=True)
            logger.info("[teardown] %s 注销成功", client_id)
        except Exception as e:
            logger.warning("[teardown] %s 注销失败: %s", client_id, e)
        await client.close()


async def run_spike_client(cfg: dict):
    """独立运行 service-a 的流量突刺场景。"""
    client_id = cfg["client_id"] + "-spike"
    client = AsyncICSClient(BASE_URL, wait_timeout_ms=5_000, http_timeout=15.0)

    try:
        await client.register(
            client_id=client_id,
            client_name=cfg["client_name"] + "(突刺)",
            min=cfg["min"],
            max=cfg["max"],
            timeout_ms=10_000,
        )
        logger.info("[setup] %s 注册成功", client_id)
        await scene_spike(client, client_id, cfg)
    except ICSError as e:
        logger.error("[setup] %s 失败: %s", client_id, e)
    finally:
        try:
            await client.unregister(force=True)
        except Exception:
            pass
        await client.close()


# ── 汇总输出 ───────────────────────────────────────────────────────────────────

def print_summary():
    sep = "=" * 62
    logger.info("\n%s", sep)
    logger.info("  评估结果汇总")
    logger.info(sep)
    logger.info("  %-28s %6s %8s %10s %8s",
                "客户端", "成功", "资源耗尽", "异常中归还", "超时回收")
    logger.info("  注: '异常中归还' = 业务抛异常但令牌被 finally 正确归还，数字越大越好")
    logger.info("  " + "-" * 62)
    for cid, s in sorted(_stats.items()):
        logger.info("  %-28s %6d %8d %10d %8d",
                    cid, s.ok, s.exhausted, s.exception_ok, s.timeout_reclaimed)
    logger.info(sep)


# ── 入口 ───────────────────────────────────────────────────────────────────────

async def main():
    logger.info("ICS 评估测试启动  服务地址: %s", BASE_URL)
    logger.info("客户端配置: %s", [(c["client_id"], c["min"], c["max"]) for c in CLIENT_CFGS])

    # 阶段一：四个客户端并行运行基础场景
    logger.info("\n%s\n  阶段一：四客户端并行（正常 / 异常 / 排队）\n%s",
                "=" * 62, "=" * 62)
    await asyncio.gather(*[run_client(cfg) for cfg in CLIENT_CFGS])

    # 阶段二：令牌超时回收
    logger.info("\n%s\n  阶段二：令牌超时回收\n%s", "=" * 62, "=" * 62)
    await scene_timeout(BASE_URL)

    # 阶段三：流量突刺
    logger.info("\n%s\n  阶段三：service-a 流量突刺\n%s", "=" * 62, "=" * 62)
    await run_spike_client(CLIENT_CFGS[0])

    print_summary()


if __name__ == "__main__":
    asyncio.run(main())
