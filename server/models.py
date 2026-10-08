from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field, model_validator


class SlotType(str, Enum):
    guaranteed = "guaranteed"
    dynamic = "dynamic"


# ── Request models ─────────────────────────────────────────────────────────────

class RegisterRequest(BaseModel):
    client_id: str = Field(..., description="全局唯一客户端标识")
    client_name: Optional[str] = Field(None, description="可读名称")
    min: int = Field(..., ge=1, description="保底并发数")
    max: int = Field(..., description="弹性最大并发数")
    timeout_ms: int = Field(120_000, ge=1000, description="令牌持有超时(ms)，默认2分钟")

    @model_validator(mode="after")
    def check_min_max(self):
        if self.max < self.min:
            raise ValueError("max must be >= min")
        return self


class UpdateRequest(BaseModel):
    client_name: Optional[str] = None
    min: Optional[int] = Field(None, ge=1)
    max: Optional[int] = None
    timeout_ms: Optional[int] = Field(None, ge=1000)

    @model_validator(mode="after")
    def check_min_max(self):
        if self.min is not None and self.max is not None:
            if self.max < self.min:
                raise ValueError("max must be >= min")
        return self


class AcquireRequest(BaseModel):
    client_id: str
    wait_timeout_ms: int = Field(5_000, ge=0, description="等待超时(ms)")


class ReleaseRequest(BaseModel):
    client_id: str
    token_id: str


# ── Response models ────────────────────────────────────────────────────────────

class TokenResponse(BaseModel):
    token_id: str
    client_id: str
    slot_type: SlotType
    expire_at: float


class ClientStatusItem(BaseModel):
    client_id: str
    client_name: Optional[str]
    min: int
    max: int
    in_use_guaranteed: int
    in_use_dynamic: int
    wait_queue_depth: int
    acq_total: int
    acq_ok: int


class SchedulerStatus(BaseModel):
    total_capacity: int
    dynamic_pool_total: int
    dynamic_pool_available: int
    clients: List[ClientStatusItem]


class Resp(BaseModel):
    code: int = 0
    message: str = "ok"
    data: Optional[dict] = None
