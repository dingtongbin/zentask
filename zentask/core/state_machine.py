import asyncio
import logging
import uuid
from typing import Any, AsyncGenerator, Callable, Dict, Optional, Set

logger = logging.getLogger(__name__)


class AsyncStateMachine:
    """纯异步内存状态机，用于追踪批量任务进度并支持 SSE 数据流推送。"""

    def __init__(self):
        self._states: Dict[str, dict] = {}
        self._queues: Dict[str, asyncio.Queue] = {}
        self._lock = asyncio.Lock()
        self._expected_jobs: Dict[str, Set[str]] = {}
        self._completed_jobs: Dict[str, Set[str]] = {}

    async def create_task(self, timeout: int, on_start: Optional[Callable] = None, on_finish: Optional[Callable] = None, on_timeout: Optional[Callable] = None) -> str:
        """创建任务，返回全局唯一且并发安全的 task_id。立即启动后台看门狗。"""
        loop = asyncio.get_running_loop()
        task_id = str(uuid.uuid4())
        async with self._lock:
            self._states[task_id] = {
                "status": "running",
                "timeout": timeout,
                "start_time": loop.time(),
                "hooks": {"on_start": on_start, "on_finish": on_finish, "on_timeout": on_timeout}
            }
            self._queues[task_id] = asyncio.Queue()
            self._expected_jobs[task_id] = set()
            self._completed_jobs[task_id] = set()

        if on_start:
            asyncio.ensure_future(on_start(task_id))
        
        # 启动看门狗
        asyncio.ensure_future(self._watchdog(task_id))
        return task_id

    async def push(self, task_id: str, data: dict) -> None:
        """非阻塞推入数据。若任务已结束或不存在，静默丢弃，不抛异常。"""
        queue = None
        async with self._lock:
            if task_id in self._states and self._states[task_id]["status"] == "running":
                queue = self._queues.get(task_id)
        
        if queue:
            try:
                await queue.put(data)
            except (asyncio.CancelledError, Exception):
                pass

    async def subscribe(self, task_id: str) -> AsyncGenerator[dict, None]:
        """异步生成器，供 SSE 消费。自动处理心跳与结束信号。"""
        while True:
            try:
                queue = self._queues.get(task_id)
                if not queue:
                    break
                # 增加超时时间，避免在高频数据下频繁触发心跳检查导致逻辑复杂化
                data = await asyncio.wait_for(queue.get(), timeout=0.2)
                yield data
                if data.get("type") == "finished":
                    break
            except asyncio.TimeoutError:
                async with self._lock:
                    if task_id not in self._states or self._states[task_id]["status"] != "running":
                        break
                    yield {"type": "heartbeat"}
            except KeyError:
                break

    def set_expected_jobs(self, task_id: str, job_ids: Set[str]) -> None:
        """循环结束后一次性移交期望的 Job ID 集合，触发首次对账。"""
        if task_id in self._expected_jobs:
            self._expected_jobs[task_id].update(job_ids)
            # 确保在更新期望任务后，如果对账已完成则触发结束
            self._reconcile(task_id)

    def mark_job_done(self, task_id: str, job_id: str) -> None:
        """被动检查入口。标记单个 job 完成，触发增量对账。"""
        if task_id in self._completed_jobs:
            self._completed_jobs[task_id].add(job_id)
            logger.info(f"Job {job_id} marked done for task {task_id}. Completed: {len(self._completed_jobs[task_id])}/{len(self._expected_jobs.get(task_id, set()))}")
            # 确保在标记完成后，如果对账已完成则触发结束
            self._reconcile(task_id)

    def _reconcile(self, task_id: str) -> None:
        """内部对账逻辑：检查是否所有期望任务均已完成。"""
        expected = self._expected_jobs.get(task_id, set())
        completed = self._completed_jobs.get(task_id, set())
        
        if expected and expected.issubset(completed):
            # 使用 create_task 替代 ensure_future 以确保任务被调度执行
            asyncio.create_task(self._finish_task(task_id, "success"))

    async def _watchdog(self, task_id: str) -> None:
        """后台看门狗：监控任务超时。"""
        loop = asyncio.get_running_loop()
        while True:
            await asyncio.sleep(0.1)
            async with self._lock:
                state = self._states.get(task_id)
                if not state or state["status"] != "running":
                    break
                
                elapsed = loop.time() - state["start_time"]
                if elapsed > state["timeout"]:
                    hook = state["hooks"].get("on_timeout")
                    # 释放锁执行 finish，避免死锁
                    asyncio.create_task(self._finish_task(task_id, "timeout", hook))
                    return

    async def _finish_task(self, task_id: str, reason: str, hook: Optional[Callable] = None) -> None:
        """统一的任务结束与清理流程。"""
        async with self._lock:
            if task_id not in self._states:
                return
            self._states[task_id]["status"] = reason
            
            # 触发钩子
            if hook:
                try:
                    await hook(task_id, reason)
                except Exception as e:
                    logger.error(f"Hook error for task {task_id}: {e}")
            
            # 触发 on_finish 钩子（如果是正常结束）
            if reason == "success" and self._states[task_id]["hooks"].get("on_finish"):
                try:
                    await self._states[task_id]["hooks"]["on_finish"](task_id)
                except Exception as e:
                    logger.error(f"Finish hook error for task {task_id}: {e}")

            # 发送结束信号
            queue = self._queues.get(task_id)
            if queue:
                await queue.put({"type": "finished", "reason": reason})
                # 等待一小段时间确保数据被消费，但不要太长以免阻塞清理
                await asyncio.sleep(0.05)

            # 内存清理
            del self._states[task_id]
            if task_id in self._queues:
                del self._queues[task_id]
            self._expected_jobs.pop(task_id, None)
            self._completed_jobs.pop(task_id, None)
