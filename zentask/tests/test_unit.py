import asyncio
import pytest
from zentask.core import ZenBaseTask, ZenTaskManager, RetryContext


class TestUnit:
    """单元测试：验证核心组件的基础功能。"""

    def test_queue_enqueue_memory(self):
        """验证 enqueue 10万次参数，内存中 deque 长度正确，且没有实例化 Task 对象。"""
        manager = ZenTaskManager(global_max_workers=5)
        
        class DummyTask(ZenBaseTask):
            def action(self, **kwargs): pass

        manager.register(DummyTask)
        
        for i in range(100000):
            manager.enqueue(DummyTask, id=i)
            
        assert len(manager.queue) == 100000
        assert not any(isinstance(item, DummyTask) for item in manager.queue)

    def test_hook_short_circuit(self):
        """验证钩子短路标志位是否正确。"""
        class WithHook(ZenBaseTask):
            def action(self): pass
            async def on_success(self): pass

        class WithoutHook(ZenBaseTask):
            def action(self): pass

        assert WithHook._has_on_success is True
        assert WithoutHook._has_on_success is False

    def test_reflection_cache(self):
        """验证同步和异步 action 的反射缓存值。"""
        manager = ZenTaskManager()

        class SyncTask(ZenBaseTask):
            def action(self): pass

        class AsyncTask(ZenBaseTask):
            async def action(self): pass

        manager.register(SyncTask)
        manager.register(AsyncTask)

        assert manager.registered_classes[SyncTask]["is_coro"] is False
        assert manager.registered_classes[AsyncTask]["is_coro"] is True

    def test_retry_context_safety(self):
        """验证 RetryContext 的安全读写能力。"""
        ctx = RetryContext({"url": "http://old.com"})
        assert ctx.get("url") == "http://old.com"
        assert ctx.get("missing", "default") == "default"
        
        ctx.set("url", "http://new.com")
        assert ctx.to_dict()["url"] == "http://new.com"
        assert ctx.is_modified is True
