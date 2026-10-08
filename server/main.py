"""
ICS 服务端入口

启动方式:
    # 方式1：直接运行（推荐）
    python server/main.py

    # 方式2：uvicorn 命令（需在项目根目录）
    uvicorn server.main:app --host 0.0.0.0 --port 8080 --reload

环境变量:
    ICS_TOTAL_CAPACITY   调度器总并发数，默认 100
    ICS_LOG_LEVEL        日志级别，默认 INFO
    ICS_HOST             监听地址，默认 0.0.0.0
    ICS_PORT             监听端口，默认 8080
"""

import sys
import os

# ── 路径修复：必须在所有业务 import 之前 ──────────────────────────────────────
# 将项目根目录加入 sys.path，使 python server/main.py 直接运行时
# 也能以 "server.xxx" 形式解析包内模块（同时不影响 uvicorn 模块启动方式）
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import asyncio
import logging

from dotenv import load_dotenv

_env_base = os.path.join(_PROJECT_ROOT, ".env")
load_dotenv(_env_base)
_ics_env = os.getenv("ICS_ENV", "dev")
load_dotenv(os.path.join(_PROJECT_ROOT, f".env.{_ics_env}"), override=True)

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from server.scheduler import Scheduler
from server.api import router

logging.basicConfig(
    level=os.getenv("ICS_LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("ics.main")

TOTAL_CAPACITY = int(os.getenv("ICS_TOTAL_CAPACITY", "100"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.scheduler = Scheduler(total_capacity=TOTAL_CAPACITY)
    logger.info("ICS started, total_capacity=%d", TOTAL_CAPACITY)

    # 后台任务：每秒扫描超时令牌并回收
    async def _expire_loop():
        while True:
            await asyncio.sleep(1)
            try:
                n = await app.state.scheduler.expire_tokens()
                if n:
                    logger.warning("expired %d token(s)", n)
            except Exception as e:
                logger.error("expire loop error: %s", e)

    task = asyncio.create_task(_expire_loop())
    yield
    task.cancel()
    logger.info("ICS stopped")


app = FastAPI(
    title="ICS — 接口并发调度器",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "server.main:app",
        host=os.getenv("ICS_HOST", "0.0.0.0"),
        port=int(os.getenv("ICS_PORT", "8080")),
        reload=True,
    )
