import asyncio
import tracemalloc
import pytest
from zentask.core import ZenBaseTask, ZenTaskManager, CancellationToken


class TestWhiteBox:
    """白盒测试：验证底层机制与性能。"""

    @pytest.mark.asyncio
    async def test_dual_track_scheduling(self):
        """验证双轨制调度：保底轨和弹性轨的正确性。"""
        manager = ZenTaskManager(global_max_workers=5)
        
        results = []

        class TaskA(ZenBaseTask):
            priority = 10
            min_slots = 2
            max_slots = 5
            def action(self, **kwargs): 
                results.append(('A', kwargs['id']))
                return "A done"

        class TaskB(ZenBaseTask):
            priority = 99
            min_slots = 2
            max_slots = 5
            def action(self, **kwargs): 
                results.append(('B', kwargs['id']))
                return "B done"

        manager.register(TaskA)
        manager.register(TaskB)

        for i in range(2): manager.enqueue(TaskA, id=i)
        for i in range(3): manager.enqueue(TaskB, id=i)

        await manager.start()
        while len(results) < 5 or manager.running_count > 0:
            await asyncio.sleep(0.1)
        await manager.graceful_shutdown()

        a_count = sum(1 for r in results if r[0] == 'A')
        b_count = sum(1 for r in results if r[0] == 'B')

        assert a_count >= 2
        assert b_count >= 2
        assert b_count == 3

    @pytest.mark.asyncio
    async def test_memory_leak_prevention(self):
        """内存防泄漏测试：连续 enqueue 100,000 个任务，监控内存增长。"""
        tracemalloc.start()
        manager = ZenTaskManager(global_max_workers=10)
        
        class LightTask(ZenBaseTask):
            def action(self, **kwargs): pass

        manager.register(LightTask)
        snapshot1 = tracemalloc.take_snapshot()
        
        for i in range(100000):
            manager.enqueue(LightTask, data="x" * 100)
            
        snapshot2 = tracemalloc.take_snapshot()
        top_stats = snapshot2.compare_to(snapshot1, 'lineno')
        total_diff = sum(stat.size_diff for stat in top_stats)
        
        assert total_diff < 50 * 1024 * 1024
        tracemalloc.stop()

    @pytest.mark.asyncio
    async def test_cancellation_penetration(self):
        """取消穿透测试：调用 token.cancel() 后任务应感知并退出。"""
        manager = ZenTaskManager(global_max_workers=1)
        cancelled_detected = False

        class LongTask(ZenBaseTask):
            def action(self, **kwargs):
                nonlocal cancelled_detected
                for _ in range(50):
                    if self.token.is_cancelled:
                        cancelled_detected = True
                        raise asyncio.CancelledError()
                    import time
                    time.sleep(0.01)
                return "Done"

        manager.register(LongTask)
        token = CancellationToken()
        # 直接操作底层队列以获取 token 引用
        manager.queue.append((LongTask, {}, token))
        
        await manager.start()
        
        # 确保任务已进入运行状态
        await asyncio.sleep(0.2)
        
        # 取消任务
        token.cancel()
        
        while manager.running_count > 0 or len(manager.queue) > 0:
            await asyncio.sleep(0.05)
        await manager.graceful_shutdown()
        
        assert cancelled_detected is True
