"""
ICS 客户端 SDK

同步用法:
    client = ICSClient("http://localhost:8080")
    client.register("service-a", min=3, max=8)

    with client.token() as tok:
        # tok.token_id 可用于日志追踪
        call_upstream_api()

异步用法:
    client = AsyncICSClient("http://localhost:8080")
    await client.register("service-a", min=3, max=8)

    async with client.token() as tok:
        await call_upstream_api()
"""

import logging
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from typing import Optional

import httpx

logger = logging.getLogger("ics.client")


# ── 数据类 ────────────────────────────────────────────────────────────────────

@dataclass
class Token:
    token_id: str
    slot_type: str
    expire_at: float

    def __repr__(self):
        return f"<Token {self.token_id} [{self.slot_type}]>"


class ICSError(Exception):
    def __init__(self, code: int, message: str):
        super().__init__(message)
        self.code = code


class ResourceExhaustedError(ICSError):
    pass


class NotRegisteredError(ICSError):
    pass


# ── 同步客户端 ────────────────────────────────────────────────────────────────

class ICSClient:
    """
    同步 ICS 客户端，底层使用 httpx 同步模式。

    推荐通过 token() 上下文管理器使用，确保令牌在异常时也能归还。
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8080",
        client_id: Optional[str] = None,
        wait_timeout_ms: int = 5_000,
        http_timeout: float = 10.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.client_id = client_id
        self.wait_timeout_ms = wait_timeout_ms
        self._http = httpx.Client(timeout=http_timeout)

    # ── 注册 / 注销 ──────────────────────────────────────────────────────────

    def register(
        self,
        client_id: str,
        min: int,
        max: int,
        client_name: Optional[str] = None,
        timeout_ms: int = 120_000,
    ) -> dict:
        self.client_id = client_id
        r = self._http.post(
            f"{self.base_url}/api/v1/clients/register",
            json={
                "client_id": client_id,
                "client_name": client_name,
                "min": min,
                "max": max,
                "timeout_ms": timeout_ms,
            },
        )
        return self._parse(r)

    def unregister(self, force: bool = False) -> dict:
        self._ensure_registered()
        r = self._http.delete(
            f"{self.base_url}/api/v1/clients/{self.client_id}",
            params={"force": force},
        )
        return self._parse(r)

    # ── 令牌操作 ─────────────────────────────────────────────────────────────

    def acquire(self, wait_timeout_ms: Optional[int] = None) -> Token:
        self._ensure_registered()
        r = self._http.post(
            f"{self.base_url}/api/v1/tokens/acquire",
            json={
                "client_id": self.client_id,
                "wait_timeout_ms": wait_timeout_ms or self.wait_timeout_ms,
            },
        )
        data = self._parse(r)
        tok = Token(
            token_id=data["token_id"],
            slot_type=data["slot_type"],
            expire_at=data["expire_at"],
        )
        logger.debug("acquired %s", tok)
        return tok

    def release(self, token: Token) -> dict:
        self._ensure_registered()
        r = self._http.post(
            f"{self.base_url}/api/v1/tokens/release",
            json={"client_id": self.client_id, "token_id": token.token_id},
        )
        result = self._parse(r)
        logger.debug("released %s", token)
        return result

    @contextmanager
    def token(self, wait_timeout_ms: Optional[int] = None):
        """
        上下文管理器，自动 acquire / release 令牌。

        with client.token() as tok:
            do_something()
        """
        tok = self.acquire(wait_timeout_ms)
        try:
            yield tok
        finally:
            try:
                self.release(tok)
            except Exception as e:
                logger.warning("release failed: %s", e)

    # ── 状态查询 ─────────────────────────────────────────────────────────────

    def status(self) -> dict:
        r = self._http.get(f"{self.base_url}/api/v1/status")
        return self._parse(r)

    # ── 内部工具 ─────────────────────────────────────────────────────────────

    def _ensure_registered(self):
        if not self.client_id:
            raise RuntimeError("call register() first")

    @staticmethod
    def _parse(r: httpx.Response) -> dict:
        try:
            body = r.json()
        except Exception:
            r.raise_for_status()
            raise
        if r.status_code >= 400:
            detail = body.get("detail", {})
            if isinstance(detail, dict):
                code = detail.get("code", r.status_code)
                msg  = detail.get("message", r.text)
            else:
                code, msg = r.status_code, str(detail)
            if code == 2001:
                raise ResourceExhaustedError(code, msg)
            if code == 2002:
                raise NotRegisteredError(code, msg)
            raise ICSError(code, msg)
        return body.get("data") or body

    def close(self):
        self._http.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


# ── 异步客户端 ────────────────────────────────────────────────────────────────

class AsyncICSClient:
    """
    异步 ICS 客户端，底层使用 httpx 异步模式。

    async with AsyncICSClient("http://localhost:8080") as client:
        await client.register("service-a", min=3, max=8)
        async with client.token() as tok:
            await do_something()
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8080",
        client_id: Optional[str] = None,
        wait_timeout_ms: int = 5_000,
        http_timeout: float = 10.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.client_id = client_id
        self.wait_timeout_ms = wait_timeout_ms
        self._http = httpx.AsyncClient(timeout=http_timeout)

    async def register(
        self,
        client_id: str,
        min: int,
        max: int,
        client_name: Optional[str] = None,
        timeout_ms: int = 120_000,
    ) -> dict:
        self.client_id = client_id
        r = await self._http.post(
            f"{self.base_url}/api/v1/clients/register",
            json={
                "client_id": client_id,
                "client_name": client_name,
                "min": min,
                "max": max,
                "timeout_ms": timeout_ms,
            },
        )
        return ICSClient._parse(r)

    async def unregister(self, force: bool = False) -> dict:
        self._ensure_registered()
        r = await self._http.delete(
            f"{self.base_url}/api/v1/clients/{self.client_id}",
            params={"force": force},
        )
        return ICSClient._parse(r)

    async def acquire(self, wait_timeout_ms: Optional[int] = None) -> Token:
        self._ensure_registered()
        r = await self._http.post(
            f"{self.base_url}/api/v1/tokens/acquire",
            json={
                "client_id": self.client_id,
                "wait_timeout_ms": wait_timeout_ms or self.wait_timeout_ms,
            },
        )
        data = ICSClient._parse(r)
        tok = Token(
            token_id=data["token_id"],
            slot_type=data["slot_type"],
            expire_at=data["expire_at"],
        )
        logger.debug("acquired %s", tok)
        return tok

    async def release(self, token: Token) -> dict:
        self._ensure_registered()
        r = await self._http.post(
            f"{self.base_url}/api/v1/tokens/release",
            json={"client_id": self.client_id, "token_id": token.token_id},
        )
        result = ICSClient._parse(r)
        logger.debug("released %s", token)
        return result

    @asynccontextmanager
    async def token(self, wait_timeout_ms: Optional[int] = None):
        """
        异步上下文管理器，自动 acquire / release。

        async with client.token() as tok:
            await do_something()
        """
        tok = await self.acquire(wait_timeout_ms)
        try:
            yield tok
        finally:
            try:
                await self.release(tok)
            except Exception as e:
                logger.warning("release failed: %s", e)

    async def status(self) -> dict:
        r = await self._http.get(f"{self.base_url}/api/v1/status")
        return ICSClient._parse(r)

    def _ensure_registered(self):
        if not self.client_id:
            raise RuntimeError("call register() first")

    async def close(self):
        await self._http.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.close()
