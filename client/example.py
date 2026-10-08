"""
ICS 客户端使用示例
"""

import asyncio
import time
from ics_client import AsyncICSClient, ICSClient, ResourceExhaustedError

ICS_URL = "http://localhost:8080"


# ═══════════════════════════════════════════════════════════
# 示例 1：同步用法
# ═══════════════════════════════════════════════════════════

def sync_example():
    print("\n── 同步示例 ──────────────────────")
    with ICSClient(ICS_URL) as client:
        # 1. 注册
        client.register("service-a", min=2, max=5, client_name="A 服务")
        print("注册成功")

        # 2. 通过上下文管理器访问（推荐）
        with client.token() as tok:
            print(f"  获取令牌: {tok}")
            time.sleep(0.5)   # 模拟业务调用
        print("  令牌已归还")

        # 3. 手动 acquire / release
        tok = client.acquire()
        print(f"  手动获取: {tok}")
        client.release(tok)
        print(f"  手动归还: {tok.token_id}")

        # 4. 资源耗尽时的异常处理
        try:
            toks = [client.acquire() for _ in range(10)]   # 超出 max
        except ResourceExhaustedError as e:
            print(f"  预期异常: {e}")

        # 5. 查询状态
        st = client.status()
        print(f"  调度器状态: 总容量={st['total_capacity']}, 弹性池可用={st['dynamic_pool_available']}")


# ═══════════════════════════════════════════════════════════
# 示例 2：异步并发用法
# ═══════════════════════════════════════════════════════════

async def async_example():
    print("\n── 异步并发示例 ──────────────────")
    async with AsyncICSClient(ICS_URL) as client:
        await client.register("service-b", min=2, max=4, client_name="B 服务")
        print("注册成功")

        async def worker(idx: int):
            try:
                async with client.token(wait_timeout_ms=3000) as tok:
                    print(f"  Worker-{idx} 获取: {tok}")
                    await asyncio.sleep(0.3)
                print(f"  Worker-{idx} 归还完成")
            except ResourceExhaustedError:
                print(f"  Worker-{idx} 资源耗尽，跳过")

        # 并发启动 6 个 worker（max=4，有 2 个会等待或超时）
        await asyncio.gather(*[worker(i) for i in range(6)])


# ═══════════════════════════════════════════════════════════
# 示例 3：多客户端保底隔离验证
# ═══════════════════════════════════════════════════════════

async def isolation_example():
    print("\n── 保底隔离验证 ──────────────────")
    client_a = AsyncICSClient(ICS_URL)
    client_b = AsyncICSClient(ICS_URL)
    try:
        # A: min=3 max=5,  B: min=2 max=3
        await client_a.register("iso-a", min=3, max=5, client_name="隔离测试A")
        await client_b.register("iso-b", min=2, max=3, client_name="隔离测试B")

        # A 占满弹性席位
        toks_a = [await client_a.acquire() for _ in range(5)]
        print(f"  A 占用 {len(toks_a)} 个席位")

        # B 的保底请求仍应成功（等当前请求结束后释放弹性席位，保底不受影响）
        tok_b = await client_b.acquire(wait_timeout_ms=500)
        print(f"  B 保底获取成功: {tok_b}")
        await client_b.release(tok_b)

        for t in toks_a:
            await client_a.release(t)
        print("  A 全部归还")
    finally:
        await client_a.unregister(force=True)
        await client_b.unregister(force=True)
        await client_a.close()
        await client_b.close()


# ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    sync_example()
    asyncio.run(async_example())
    asyncio.run(isolation_example())
