import asyncio
import json
import logging
from typing import List

from fastapi import APIRouter, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

from .models import (
    AcquireRequest,
    RegisterRequest,
    ReleaseRequest,
    UpdateRequest,
)
from .scheduler import Scheduler

logger = logging.getLogger("ics.api")

router = APIRouter()


def _scheduler(request: Request) -> Scheduler:
    return request.app.state.scheduler


# ── 注册 / 更新 / 注销 ────────────────────────────────────────────────────────

@router.post("/api/v1/clients/register")
async def register(body: RegisterRequest, request: Request):
    sched = _scheduler(request)
    try:
        data = await sched.register(
            client_id=body.client_id,
            client_name=body.client_name,
            min_c=body.min,
            max_c=body.max,
            timeout_ms=body.timeout_ms,
        )
    except ValueError as e:
        code = int(str(e).split(":")[0]) if str(e)[0].isdigit() else 1000
        raise HTTPException(status_code=400, detail={"code": code, "message": str(e)})
    return {"code": 0, "message": "ok", "data": data}


@router.put("/api/v1/clients/{client_id}")
async def update_client(client_id: str, body: UpdateRequest, request: Request):
    sched = _scheduler(request)
    try:
        await sched.update(
            client_id=client_id,
            client_name=body.client_name,
            min_c=body.min,
            max_c=body.max,
            timeout_ms=body.timeout_ms,
        )
    except KeyError as e:
        raise HTTPException(status_code=404, detail={"code": 4040, "message": str(e)})
    except ValueError as e:
        raise HTTPException(status_code=400, detail={"code": 1000, "message": str(e)})
    return {"code": 0, "message": "ok"}


@router.delete("/api/v1/clients/{client_id}")
async def unregister(client_id: str, request: Request, force: bool = False):
    sched = _scheduler(request)
    try:
        await sched.unregister(client_id, force=force)
    except KeyError as e:
        raise HTTPException(status_code=404, detail={"code": 4040, "message": str(e)})
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail={"code": 4090, "message": str(e)})
    return {"code": 0, "message": "ok"}


# ── 获取 / 归还令牌 ───────────────────────────────────────────────────────────

@router.post("/api/v1/tokens/acquire")
async def acquire(body: AcquireRequest, request: Request):
    sched = _scheduler(request)
    try:
        tok = await sched.acquire(
            client_id=body.client_id,
            wait_timeout_ms=body.wait_timeout_ms,
        )
    except KeyError as e:
        raise HTTPException(status_code=404, detail={"code": 2002, "message": str(e)})
    except TimeoutError as e:
        raise HTTPException(status_code=503, detail={"code": 2001, "message": str(e)})
    return {
        "code": 0,
        "data": {
            "token_id": tok.token_id,
            "slot_type": tok.slot_type,
            "expire_at": tok.expire_at,
        },
    }


@router.post("/api/v1/tokens/release")
async def release(body: ReleaseRequest, request: Request):
    sched = _scheduler(request)
    try:
        await sched.release(client_id=body.client_id, token_id=body.token_id)
    except KeyError as e:
        raise HTTPException(status_code=404, detail={"code": 3001, "message": str(e)})
    except PermissionError as e:
        raise HTTPException(status_code=403, detail={"code": 3002, "message": str(e)})
    return {"code": 0, "message": "released"}


# ── 状态查询 ─────────────────────────────────────────────────────────────────

@router.get("/api/v1/status")
async def status(request: Request):
    sched = _scheduler(request)
    return {"code": 0, "data": sched.get_status()}


# ── WebSocket 实时推送（供监控页面使用）──────────────────────────────────────

class _WSManager:
    def __init__(self):
        self._conns: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self._conns.append(ws)

    def disconnect(self, ws: WebSocket):
        self._conns.remove(ws)

    async def broadcast(self, payload: dict):
        dead = []
        msg = json.dumps({"code": 0, "data": payload})
        for ws in self._conns:
            try:
                await ws.send_text(msg)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self._conns.remove(ws)


ws_manager = _WSManager()


@router.websocket("/ws/status")
async def ws_status(ws: WebSocket):
    sched = ws.app.state.scheduler
    await ws_manager.connect(ws)
    try:
        while True:
            events = sched.pop_events()
            await ws.send_text(json.dumps({
                "code": 0,
                "data": sched.get_status(),
                "events": events,
            }))
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        ws_manager.disconnect(ws)


# ── 监控页面（直接访问 / 时返回 HTML）───────────────────────────────────────

@router.get("/", response_class=HTMLResponse)
async def monitor_page():
    try:
        with open("monitor.html", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return "<h2>monitor.html not found. Place it in the working directory.</h2>"
