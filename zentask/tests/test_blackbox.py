import asyncio
import time
import pytest
from zentask.core import ZenBaseTask, ZenTaskManager, TaskStatus


class TestBlackBox:
    """黑盒测试：验证业务场景与异常处理。"""

    @pytest.mark.asyncio
    async def test_sync_async_mixed_execution(self):
        """同步/异步混合执行测试。"""
        manager = ZenTaskManager(global_max_workers=5)
        results = []

        class SyncTask(ZenBaseTask):
            def action(self, **kwargs):
                return f"sync_{kwargs['id']}"
            
            async def on_success(self):
                results.append(self.result)

        class AsyncTask(ZenBaseTask):
            async def action(self, **kwargs):
                await asyncio.sleep(0.01)
                return f"async_{kwargs['id']}"
            
            async def on_success(self):
                results.append(self.result)

        manager.register(SyncTask)
        manager.register(AsyncTask)

        manager.enqueue(SyncTask, id=1)
        manager.enqueue(AsyncTask, id=2)

        await manager.start()
        # 增加超时保护，最多等待 5 秒
        timeout = 5.0
        start_time = time.time()
        while (len(results) < 2 or manager.running_count > 0) and (time.time() - start_time < timeout):
            await asyncio.sleep(0.02)
        await manager.graceful_shutdown()

        assert "sync_1" in results
        assert "async_2" in results

    @pytest.mark.asyncio
    async def test_exception_isolation(self):
        """异常隔离测试：中间任务失败不应影响其他任务。"""
        manager = ZenTaskManager(global_max_workers=5)
        statuses = []

        class ErrorTask(ZenBaseTask):
            def action(self, **kwargs):
                if kwargs['id'] == 2:
                    raise ValueError("Intentional Error")
                return "OK"
            
            async def on_complete(self):
                statuses.append((self.kwargs['id'], self.status))

        manager.register(ErrorTask)
        
        for i in range(1, 4):
            manager.enqueue(ErrorTask, id=i)

        await manager.start()
        # 增加超时保护
        timeout = 5.0
        start_time = time.time()
        while len(statuses) < 3 and (time.time() - start_time < timeout):
            await asyncio.sleep(0.02)
        await manager.graceful_shutdown()

        status_map = {s[0]: s[1] for s in statuses}
        assert status_map[1] == TaskStatus.SUCCESS
        assert status_map[2] == TaskStatus.FAILED
        assert status_map[3] == TaskStatus.SUCCESS

    @pytest.mark.asyncio
    async def test_retry_mechanism(self):
        """重试机制测试：失败任务应按指数退避重试，且 attempts 计数正确。"""
        manager = ZenTaskManager(global_max_workers=2)
        attempts = []

        class RetryTask(ZenBaseTask):
            max_retries = 3  # 增加到 3 次，确保有足够的时间重试
            retry_delay = 0.1  # 增加延迟，避免太快
            
            def action(self, **kwargs):
                attempts.append(self.attempts)
                if self.attempts < 3:
                    raise ValueError(f"Fail attempt {self.attempts}")
                return "Success"

        manager.register(RetryTask)
        manager.enqueue(RetryTask, id="test")

        await manager.start()
        # 增加等待时间以覆盖重试延迟 (0.1 + 0.2 + 0.4 = 0.7s)
        timeout = 10.0
        start_time = time.time()
        # 等待直到 attempts 达到 3 或者超时
        while len(attempts) < 3 and (time.time() - start_time < timeout):
            await asyncio.sleep(0.05)
        
        # 确保所有任务都处理完毕
        await manager.graceful_shutdown()

        # 应该执行 3 次：第 1 次失败，第 2 次失败，第 3 次成功
        assert len(attempts) == 3, f"Expected 3 attempts, got {len(attempts)}: {attempts}"
        # 验证 attempts 计数是否正确递增
        assert attempts == [1, 2, 3], f"Expected [1, 2, 3], got {attempts}"

    @pytest.mark.asyncio
    async def test_global_limit_avalanche_prevention(self):
        """全局上限防雪崩测试：任意时刻运行数不超过 global_max_workers。"""
        max_observed_running = 0
        manager = ZenTaskManager(global_max_workers=3)

        class HeavyTask(ZenBaseTask):
            def action(self, **kwargs):
                time.sleep(0.2)
                return "Heavy Done"

        manager.register(HeavyTask)
        
        async def monitor():
            nonlocal max_observed_running
            for _ in range(20):
                await asyncio.sleep(0.05)
                count = manager.running_count
                if count > max_observed_running:
                    max_observed_running = count

        for i in range(50):
            manager.enqueue(HeavyTask, id=i)

        await manager.start()
        monitor_task = asyncio.create_task(monitor())
        
        while manager.running_count > 0 or len(manager.queue) > 0:
            await asyncio.sleep(0.05)
            
        await manager.graceful_shutdown()
        await monitor_task

        assert max_observed_running <= 3
