import asyncio
import logging
import random
import pytest
from zentask.core.state_machine import AsyncStateMachine
from zentask.core import ZenTaskManager, ZenBaseTask

logging.basicConfig(level=logging.INFO)


class MockAsyncWorker:
    """模拟 ZenTask 黑盒行为，1-3秒随机异步延迟"""
    def __init__(self, state_machine: AsyncStateMachine):
        self.sm = state_machine

    async def execute(self, task_id: str, job_id: str):
        await asyncio.sleep(random.uniform(0.01, 0.03))
        await self.sm.push(task_id, {"job_id": job_id, "status": "ok"})
        self.sm.mark_job_done(task_id, job_id)


class TestStateMachine:
    """状态机核心功能测试。"""

    @pytest.mark.asyncio
    async def test_normal_flow(self):
        """正常流转：提交 50 个 mock 任务，验证 SSE 收到 50 条数据 + 1 条 done 信号。"""
        sm = AsyncStateMachine()
        finished_count = 0
        
        async def on_finish(tid):
            nonlocal finished_count
            finished_count += 1

        task_id = await sm.create_task(timeout=10, on_finish=on_finish)
        worker = MockAsyncWorker(sm)
        
        job_ids = {f"job_{i}" for i in range(50)}
        sm.set_expected_jobs(task_id, job_ids)
        
        # 启动消费者
        consumer_task = asyncio.ensure_future(self._consume_sse(sm, task_id))
        
        tasks = [worker.execute(task_id, jid) for jid in job_ids]
        await asyncio.gather(*tasks)
        
        # 等待消费者结束
        received_data = await consumer_task
        
        assert len([d for d in received_data if d.get("job_id")]) == 50
        assert finished_count == 1
        assert received_data[-1]["type"] == "finished"

    async def _consume_sse(self, sm, task_id):
        received_data = []
        async for data in sm.subscribe(task_id):
            received_data.append(data)
            if data.get("type") == "finished":
                break
        return received_data

    @pytest.mark.asyncio
    async def test_hook_race_condition(self):
        """钩子抢跑：先触发 mark_job_done，后调用 set_expected_jobs。"""
        sm = AsyncStateMachine()
        task_id = await sm.create_task(timeout=5)
        
        worker = MockAsyncWorker(sm)
        await worker.execute(task_id, "job_early")
        
        sm.set_expected_jobs(task_id, {"job_early"})
        
        async for data in sm.subscribe(task_id):
            if data.get("type") == "finished":
                assert data["reason"] == "success"
                break

    @pytest.mark.asyncio
    async def test_timeout_exception(self):
        """超时异常：创建 timeout=0.2s 的任务，mock worker 延迟 1s。"""
        sm = AsyncStateMachine()
        timeout_triggered = False
        
        async def on_timeout(tid, reason):
            nonlocal timeout_triggered
            timeout_triggered = True

        task_id = await sm.create_task(timeout=0.2, on_timeout=on_timeout)
        
        async def slow_worker():
            await asyncio.sleep(1)
            # 超时后任务已结束，push 应该静默丢弃
            await sm.push(task_id, {"status": "too_late"})
            sm.mark_job_done(task_id, "job_slow")

        sm.set_expected_jobs(task_id, {"job_slow"})
        asyncio.ensure_future(slow_worker())
        
        async for data in sm.subscribe(task_id):
            if data.get("type") == "finished":
                assert data["reason"] == "timeout"
                break
        
        assert timeout_triggered is True
        # 验证内存清理（增加一点等待时间确保 _finish_task 执行完毕）
        await asyncio.sleep(0.1)
        assert task_id not in sm._states

    @pytest.mark.asyncio
    async def test_id_concurrency_safety(self):
        """ID 并发安全：asyncio.gather 并发调用 100 次 create_task。"""
        sm = AsyncStateMachine()
        ids = await asyncio.gather(*(sm.create_task(timeout=5) for _ in range(100)))
        assert len(set(ids)) == 100

    @pytest.mark.asyncio
    async def test_whitebox_memory_cleanup(self):
        """白盒边界：验证 finish 后内存彻底释放；验证 finish 后 push 静默丢弃。"""
        sm = AsyncStateMachine()
        task_id = await sm.create_task(timeout=5)
        sm.set_expected_jobs(task_id, {"j1"})
        sm.mark_job_done(task_id, "j1")
        
        # 等待对账完成并清理
        await asyncio.sleep(0.3)
        
        assert task_id not in sm._states
        assert task_id not in sm._queues
        
        # 尝试向已结束任务 push
        await sm.push(task_id, {"data": "ghost"}) # 不应抛出异常


class TestZenTaskRegression:
    """ZenTask 回归测试：确保兼容性修改未破坏既有逻辑。"""

    @pytest.mark.asyncio
    async def test_enqueue_returns_id(self):
        """验证 enqueue 现在返回唯一的 task_id。"""
        manager = ZenTaskManager(global_max_workers=2)
        
        class DummyTask(ZenBaseTask):
            def action(self, **kwargs): return "OK"

        manager.register(DummyTask)
        tid1 = manager.enqueue(DummyTask, id=1)
        tid2 = manager.enqueue(DummyTask, id=2)
        
        assert tid1 != tid2
        assert isinstance(tid1, str) and len(tid1) > 0

    @pytest.mark.asyncio
    async def test_instance_has_task_id(self):
        """验证在生命周期钩子内可以读取到 task_id。"""
        manager = ZenTaskManager(global_max_workers=2)
        captured_ids = []
        
        class IDCheckTask(ZenBaseTask):
            async def on_complete(self):
                captured_ids.append(getattr(self, 'task_id', None))

        manager.register(IDCheckTask)
        expected_id = manager.enqueue(IDCheckTask, id=99)
        
        await manager.start()
        while manager.running_count > 0 or len(manager.queue) > 0:
            await asyncio.sleep(0.05)
        await manager.graceful_shutdown()
        
        assert expected_id in captured_ids
