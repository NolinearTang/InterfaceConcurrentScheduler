# 接口并发调度器 (ICS)

> 统一管理多客户端对上游资源的并发访问，提供保底并发保障与弹性并发扩展。

---

## 目录

- [项目结构](#项目结构)
- [快速启动](#快速启动)
- [Docker 部署](#docker-部署)
- [环境变量](#环境变量)
- [客户端接入](#客户端接入)
- [API 接口](#api-接口)
- [监控页面](#监控页面)
- [超时机制](#超时机制)
- [核心概念](#核心概念)

---

## 项目结构

```
InterfaceConcurrentScheduler/
├── server/
│   ├── main.py          # FastAPI 入口，后台超时回收任务
│   ├── scheduler.py     # 核心调度引擎（保底/弹性分配，优先级队列）
│   ├── models.py        # Pydantic 请求/响应模型
│   ├── api.py           # REST 路由 + WebSocket 实时推送
│   └── requirements.txt
├── client/
│   ├── ics_client.py    # 客户端 SDK（同步 + 异步）
│   └── example.py       # 接入示例
├── monitor.html         # 实时监控页面（浏览器直接打开）
├── Dockerfile
├── .env                 # 环境选择器（ICS_ENV=dev/prod/test）
├── .env.dev             # 开发环境配置
├── .env.prod            # 生产环境配置
├── .env.test            # 测试环境配置
└── 设计文档.md
```

---

## 快速启动

### 1. 安装依赖

```bash
pip install -r server/requirements.txt
```

> 依赖清单：`fastapi` `uvicorn[standard]` `pydantic>=2` `websockets`

### 2. 启动服务端

**方式 1：直接运行（推荐）**

在项目根目录（`InterfaceConcurrentScheduler/`）下执行：

```bash
python server/main.py
```

**方式 2：uvicorn 命令**

```bash
uvicorn server.main:app --host 0.0.0.0 --port 8080 --reload
```

启动成功后终端输出：

```
INFO  ICS started, total_capacity=100
INFO  Uvicorn running on http://0.0.0.0:8080
```

### 3. 验证服务

```bash
curl http://localhost:8080/api/v1/status
```

返回示例：

```json
{
  "code": 0,
  "data": {
    "total_capacity": 100,
    "dynamic_pool_total": 100,
    "dynamic_pool_available": 100,
    "clients": []
  }
}
```

---

## Docker 部署

### 构建镜像

```bash
docker build -t ics:latest .
```

### 启动容器

**开发环境**

```bash
docker run -d \
  --name ics \
  -p 8080:8080 \
  -e ICS_ENV=dev \
  ics:latest
```

**生产环境**

```bash
docker run -d \
  --name ics \
  -p 8080:8080 \
  -e ICS_ENV=prod \
  ics:latest
```

**测试环境**

```bash
docker run -d \
  --name ics \
  -p 18080:18080 \
  -e ICS_ENV=test \
  ics:latest
```

> `ICS_ENV` 决定加载哪个 `.env.{ICS_ENV}` 配置文件，其余参数（端口、并发数、日志级别）均由对应配置文件控制。

### 查看日志

```bash
docker logs -f ics
```

### 停止 / 删除

```bash
docker stop ics && docker rm ics
```

---

## 环境变量

配置文件位于项目根目录，`.env` 仅作环境选择器，实际参数在对应环境文件中维护：

| 文件 | 用途 |
|------|------|
| `.env` | 设置 `ICS_ENV`，决定加载哪个环境文件 |
| `.env.dev` | 开发环境参数 |
| `.env.prod` | 生产环境参数 |
| `.env.test` | 测试环境参数 |

各文件支持的参数：

| 变量名 | 说明 | dev 默认 | prod 默认 |
|--------|------|----------|----------|
| `ICS_TOTAL_CAPACITY` | 最大全局并发数 | `20` | `100` |
| `ICS_LOG_LEVEL` | 日志级别 `DEBUG/INFO/WARNING/ERROR` | `DEBUG` | `INFO` |
| `ICS_HOST` | 监听地址 | `127.0.0.1` | `0.0.0.0` |
| `ICS_PORT` | 监听端口 | `8080` | `8080` |

---

## 客户端接入

将 `client/ics_client.py` 复制到业务服务中，或直接引用。

依赖：`httpx>=0.27`（`pip install httpx`）

### 同步接入（推荐普通服务）

```python
from ics_client import ICSClient, ResourceExhaustedError

# 服务启动时初始化并注册（全局单例）
ics = ICSClient("http://localhost:8080")
ics.register(
    client_id  = "service-a",   # 全局唯一，建议用服务名
    min        = 3,              # 保底并发数
    max        = 8,              # 最大并发数（含保底）
    client_name= "A 服务",
    timeout_ms = 120_000,        # 令牌持有超时 2 分钟（异常兜底）
)

# 每次调用上游接口时
def call_upstream():
    with ics.token() as tok:
        # tok.token_id 可写入日志用于追踪
        result = upstream_api.call(...)
    return result

# 资源耗尽时的处理
def call_with_fallback():
    try:
        with ics.token(wait_timeout_ms=3000) as tok:
            return upstream_api.call(...)
    except ResourceExhaustedError:
        return {"error": "service busy, please retry"}
```

### 异步接入（asyncio 服务）

```python
import asyncio
from ics_client import AsyncICSClient

ics = AsyncICSClient("http://localhost:8080")

# 服务启动时注册
async def startup():
    await ics.register("service-b", min=2, max=5)

# 每次调用上游接口时
async def call_upstream():
    async with ics.token() as tok:
        result = await async_upstream_api.call(...)
    return result
```

### 上下文管理器说明

`token()` / `async with token()` 会在 `finally` 块中归还令牌，即使业务代码抛出异常也能保证归还，**强烈推荐使用此方式**，避免令牌泄漏。

---

## API 接口

### 注册客户端

```
POST /api/v1/clients/register
```

```json
{
  "client_id":   "service-a",
  "client_name": "A 服务",
  "min":         3,
  "max":         8,
  "timeout_ms":  120000
}
```

| 字段 | 必填 | 说明 |
|------|------|------|
| `client_id` | 是 | 全局唯一标识 |
| `min` | 是 | 保底并发数（`≥1`） |
| `max` | 是 | 最大并发数（`≥min`，`≤TOTAL_CAPACITY`） |
| `timeout_ms` | 否 | 令牌持有超时，默认 120000（2 分钟） |

---

### 更新注册信息

```
PUT /api/v1/clients/{client_id}
```

请求体字段均为可选，仅传需要修改的字段。

---

### 注销客户端

```
DELETE /api/v1/clients/{client_id}?force=false
```

- `force=false`（默认）：有活跃令牌时返回 409，需先归还
- `force=true`：强制回收所有令牌并注销

---

### 获取令牌

```
POST /api/v1/tokens/acquire
```

```json
{
  "client_id":       "service-a",
  "wait_timeout_ms": 5000
}
```

成功响应：

```json
{
  "code": 0,
  "data": {
    "token_id":  "tok_abc123xyz456",
    "slot_type": "guaranteed",
    "expire_at": 1727600460.0
  }
}
```

`slot_type` 取值：`guaranteed`（保底席位）/ `dynamic`（弹性席位）

---

### 归还令牌

```
POST /api/v1/tokens/release
```

```json
{
  "client_id": "service-a",
  "token_id":  "tok_abc123xyz456"
}
```

---

### 查询调度器状态

```
GET /api/v1/status
```

---

### 错误码

| code | HTTP | 含义 |
|------|------|------|
| 1001 | 400 | 保底资源不足，`Σmin` 超出 `TOTAL_CAPACITY` |
| 1002 | 400 | `max` 超出 `TOTAL_CAPACITY` |
| 1003 | 400 | `client_id` 已注册 |
| 1004 | 400 | 参数非法 |
| 2001 | 503 | 等待超时，资源耗尽 |
| 2002 | 404 | 客户端未注册 |
| 3001 | 404 | token 不存在或已过期 |
| 3002 | 403 | token 不属于该客户端 |

---

## 监控页面

通过浏览器访问 `http://localhost:8080/` 加载监控页面，默认连接真实服务。也可直接用浏览器打开 `monitor.html` 文件。

**连接策略：**

1. 优先尝试 WebSocket `ws://localhost:8080/ws/status`（实时推送，含事件日志）
2. WebSocket 失败则降级为每秒轮询 `GET http://localhost:8080/api/v1/status`

### KPI 卡片

| 指标 | 计算方式 | 说明 |
|------|---------|------|
| **总容量** | `ICS_TOTAL_CAPACITY`（配置项） | 系统允许的最大全局并发令牌数，固定值 |
| **当前占用** | `Σ in_use_guaranteed + Σ in_use_dynamic`（全客户端求和） | 当前被持有的令牌总数；颜色：绿色 <70%，黄色 70–90%，红色 ≥90% |
| **弹性池可用** | `dynamic_pool_total - Σ in_use_dynamic` | 弹性池剩余可分配席位数；弹性池总量 = `总容量 - Σmin` |
| **注册客户端** | `len(clients)`（服务端 `_clients` 字典长度） | 当前已注册且未注销的客户端数，`register` +1，`unregister` -1 |
| **等待队列** | `Σ wait_queue_depth`（全客户端求和） | 当前所有客户端正在排队等待席位的请求总数 |

### 全局并发进度条

```
|████████████░░░░░░░░░░░░░░░|  12 / 20
 ←蓝色（保底占用）→←紫色（弹性占用）→
```

| 色块 | 计算 |
|------|------|
| 蓝色宽度 | `Σ in_use_guaranteed / 总容量 × 100%` |
| 紫色宽度 | `Σ in_use_dynamic / 总容量 × 100%` |
| 中间数字 | `当前占用 / 总容量` |

### 客户端状态表

每行对应一个已注册客户端，各列含义：

| 列 | 计算方式 | 颜色规则 |
|----|---------|---------|
| **保底进度条** | `in_use_guaranteed / min × 100%` | 蓝 <60%，黄 60–90%，红 ≥90% |
| **弹性进度条** | `in_use_dynamic / (max - min) × 100%` | 紫 <60%，黄 60–90%，红 ≥90% |
| **等待队列** | `wait_queue_depth`（该客户端排队数） | 有排队时徽章变红 |
| **令牌获取率** | `acq_ok / acq_total × 100%` | 绿色 ≥90%，红色 <90%，`--` 表示尚无数据 |
| **状态** | 见下表 | 彩色圆点 |

状态判断逻辑（优先级从高到低）：

| 状态 | 条件 |
|------|------|
| 🔴 排队 | `wait_queue_depth > 0` |
| 🟡 繁忙 | `in_use_guaranteed + in_use_dynamic >= max` |
| 🟢 活跃 | `in_use_guaranteed + in_use_dynamic > 0` |
| ⚪ 空闲 | 无令牌被持有 |

### 近期峰值并发图

浏览器本地追踪自页面加载以来每个客户端的历史最高并发数（**刷新页面后清零**）。

| 指标 | 计算方式 |
|------|---------|
| 峰值 | `max(in_use_guaranteed + in_use_dynamic)`，取历史最大值 |
| 进度条 | `峰值 / max × 100%` |

> 峰值数据仅在当前浏览器会话内有效，不持久化到服务端。

### 实时事件日志

通过 WebSocket 推送，最多保留最近 60 条，超出后自动移除最旧一条。

| 事件类型 | 触发时机 |
|---------|---------|
| `获取令牌` | `acquire` 成功，拿到保底或弹性席位 |
| `归还令牌` | `release` 成功 |
| `超时回收` | 令牌超过 `timeout_ms` 被服务端后台强制回收 |
| `排队等待` | 无空闲席位，请求进入等待队列 |

---

## 超时机制

系统共有三个独立的超时参数，职责不同：

| 参数 | 默认值 | 设置位置 | 职责 |
|------|--------|---------|------|
| `timeout_ms` | 120000ms（2分钟） | `register()` 时传入 | 令牌持有超时，服务端强制回收（居底安全网） |
| `wait_timeout_ms` | 5000ms（5秒） | `acquire()` 时传入 | 无空闲席位时在队列中等待的最大时长 |
| `http_timeout` | 10.0s | 客户端初始化时传入 | 单次 HTTP 请求的网络超时 |

### acquire 请求全流程

```
客户端调用 acquire()
│
├─ [1] HTTP 请求发出
│       └─ http_timeout = 10s
│           超时 → 网络异常，客户端报错
│
├─ [2] 服务端检查席位
│       ├─ 有空闲席位 → 立刻返回令牌
│       └─ 无空闲席位 → 进入等待队列
│               └─ wait_timeout_ms = 5s
│                   超时 → HTTP 503 + ResourceExhaustedError
│
└─ [3] 获得令牌，开始业务处理
        └─ timeout_ms = 120s
            超时未归还 → 服务端后台每秒扫描并强制回收
```

### 关键约束

- **`http_timeout` 必须大于 `wait_timeout_ms`**：否则客户端网络超时会先触发，拿到的是网络错误而非 `ResourceExhaustedError`，调用方无法区分"服务忙"和"服务挂了"
- **`timeout_ms` 应远大于业务处理时间**：令牌应由 `release()` 主动归还，`timeout_ms` 仅为客户端崩溃/挂起时的兜底保障
- **正常路径**：`acquire()` → 业务处理 → `release()`，令牌生命周期由业务掌控，`timeout_ms` 不应被触发

---

## 核心概念

| 概念 | 说明 |
|------|------|
| **保底并发（guaranteed）** | 每个客户端注册的 `min` 个席位，由调度器预留，任何时刻均可获取 |
| **弹性并发（dynamic）** | 共享资源池 = `TOTAL_CAPACITY - Σmin`，空闲时按需分配，上限为 `max` |
| **令牌（Token）** | 持有令牌代表占用一个并发席位；请求结束后必须归还 |
| **优先级** | 等待队列中：保底请求(P1) > 弹性请求(P2)，保底永远优先被满足 |
| **超时回收** | 令牌超过 `timeout_ms` 未归还，由服务端后台任务自动回收（异常兜底） |
