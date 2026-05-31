import asyncio
import inspect
import multiprocessing
import pickle
import time
import traceback
from collections import deque
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, Future, TimeoutError as FuturesTimeoutError
from enum import Enum
from functools import partial
from typing import Any, Callable, Dict, Optional, Type


class TaskStatus(Enum):
    PENDING = "PENDING"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class RetryContext:
    """安全重试上下文，用于在 on_retry 中干预下一次重试的参数。"""
    def __init__(self, kwargs: dict):
        self._kwargs = kwargs.copy()
        self._modified = False

    def get(self, key: str, default=None):
        return self._kwargs.get(key, default)

    def set(self, key: str, value: Any):
        self._kwargs[key] = value
        self._modified = True

    def to_dict(self) -> dict:
        return self._kwargs

    @property
    def is_modified(self) -> bool:
        return self._modified


class CancellationToken:
    def __init__(self):
        self._cancelled = False

    def cancel(self):
        self._cancelled = True

    @property
    def is_cancelled(self) -> bool:
        return self._cancelled


class ZenBaseTask:
    """
    任务基类。用户应继承此类并实现 action 方法。
    支持同步/异步 action、超时控制、自动重试及多进程隔离。
    """
    priority: int = 10
    min_slots: int = 2
    max_slots: int = 50
    max_retries: int = 0
    retry_delay: float = 1.0
    timeout: Optional[float] = None
    use_process: bool = False

    # 元编程缓存标志
    _has_on_start: bool = False
    _has_on_success: bool = False
    _has_on_error: bool = False
    _has_on_retry: bool = False
    _has_on_complete: bool = False
    _is_coro_action: bool = False

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls._has_on_start = 'on_start' in cls.__dict__
        cls._has_on_success = 'on_success' in cls.__dict__
        cls._has_on_error = 'on_error' in cls.__dict__
        cls._has_on_retry = 'on_retry' in cls.__dict__
        cls._has_on_complete = 'on_complete' in cls.__dict__

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.result: Any = None
        self.error: Optional[Exception] = None
        self.status: TaskStatus = TaskStatus.PENDING
        self.token: CancellationToken = CancellationToken()
        self.retry_ctx: RetryContext = RetryContext(kwargs)
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.attempts: int = 0

    async def on_start(self):
        pass

    async def on_success(self):
        pass

    async def on_error(self):
        pass

    async def on_retry(self, exception: Exception, retry_ctx: RetryContext, next_delay: float):
        pass

    async def on_complete(self):
        pass

    def action(self, **kwargs):
        raise NotImplementedError("Subclasses must implement action")


class ZenTaskManager:
    """
    ZenTask 调度器核心。
    负责全局并发控制、双轨制调度、延迟实例化、多进程物理超度及优雅关闭。
    """

    def __init__(self, global_max_workers: int = 20):
        self.global_max_workers = global_max_workers
        self.thread_pool = ThreadPoolExecutor(max_workers=global_max_workers)
        self.process_pool = ProcessPoolExecutor(max_workers=min(4, global_max_workers))
        
        self.queue: deque = deque()
        self.registered_classes: Dict[Type[ZenBaseTask], dict] = {}
        self.running_tasks: Dict[Future, ZenBaseTask] = {}
        self.running_count: int = 0
        self.class_running_counts: Dict[Type[ZenBaseTask], int] = {}

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._scheduler_task: Optional[asyncio.Task] = None
        self._is_started = False
        self._is_shutting_down = False

    def register(self, task_cls: Type[ZenBaseTask]):
        if task_cls not in self.registered_classes:
            # 预检多进程模式的序列化能力
            if task_cls.use_process:
                for k, v in task_cls.__dict__.items():
                    if not k.startswith('_'):
                        try:
                            pickle.dumps(v)
                        except Exception:
                            raise TypeError(f"Class attribute {k} in {task_cls.__name__} is not picklable for process mode.")

            self.registered_classes[task_cls] = {
                "priority": task_cls.priority,
                "min_slots": task_cls.min_slots,
                "max_slots": task_cls.max_slots,
                "is_coro": inspect.iscoroutinefunction(task_cls.action),
                "use_process": task_cls.use_process,
                "timeout": task_cls.timeout,
            }
            self.class_running_counts[task_cls] = 0

    def enqueue(self, task_cls: Type[ZenBaseTask], **kwargs):
        if self._is_shutting_down:
            raise RuntimeError("Cannot enqueue tasks during shutdown.")
        
        if task_cls not in self.registered_classes:
            self.register(task_cls)
        
        # 多进程模式预检参数序列化
        if task_cls.use_process:
            try:
                pickle.dumps(kwargs)
            except Exception as e:
                raise ZenTaskPickleError(f"Arguments for {task_cls.__name__} are not picklable: {e}")

        token = CancellationToken()
        self.queue.append((task_cls, kwargs, token))
        
        if self._is_started and self._loop and self._loop.is_running():
            asyncio.run_coroutine_threadsafe(self._schedule(), self._loop)

    async def start(self):
        self._loop = asyncio.get_running_loop()
        self._is_started = True
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())

    async def graceful_shutdown(self):
        """优雅关闭：停止接收新任务，等待队列清空及所有在途任务执行完毕。"""
        self._is_started = False
        
        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass

        # 等待队列清空（包括重试任务）
        # 增加一个小延迟，确保 call_later 的任务有机会入队
        while self.queue or self.running_count > 0:
            await asyncio.sleep(0.1)
        
        self._is_shutting_down = True # 在所有任务完成后才设置
        self.thread_pool.shutdown(wait=True)
        self.process_pool.shutdown(wait=True)

    async def force_shutdown(self):
        """强制退出：核弹级操作，瞬间清空并刺杀所有任务。"""
        self._is_shutting_down = True
        self._is_started = False
        self.queue.clear()
        
        for future in list(self.running_tasks.keys()):
            future.cancel()
        
        self.thread_pool.shutdown(wait=False)
        self.process_pool.shutdown(wait=False)

    async def _scheduler_loop(self):
        while self._is_started:
            await self._schedule()
            await asyncio.sleep(0.01)

    async def _schedule(self):
        if not self.queue or self.running_count >= self.global_max_workers:
            return

        current_class_counts = dict(self.class_running_counts)
        queued_classes = set(cls for cls, _, _ in self.queue)

        # 保底轨
        candidates_for_min = []
        for cls in queued_classes:
            info = self.registered_classes[cls]
            if current_class_counts.get(cls, 0) < info["min_slots"]:
                candidates_for_min.append(cls)
        
        candidates_for_min.sort(key=lambda c: self.registered_classes[c]["priority"])

        for cls in candidates_for_min:
            if self.running_count >= self.global_max_workers:
                break
            if current_class_counts.get(cls, 0) < self.registered_classes[cls]["min_slots"]:
                self._dispatch_task(cls)
                current_class_counts[cls] = current_class_counts.get(cls, 0) + 1
                self.running_count += 1

        # 弹性轨
        while self.queue and self.running_count < self.global_max_workers:
            best_cls = None
            best_priority = -1
            
            temp_queue = list(self.queue)
            for c, _, _ in temp_queue:
                if c in self.registered_classes:
                    p = self.registered_classes[c]["priority"]
                    if p > best_priority:
                        if current_class_counts.get(c, 0) < self.registered_classes[c]["max_slots"]:
                            best_priority = p
                            best_cls = c
            
            if best_cls:
                self._dispatch_task(best_cls)
                current_class_counts[best_cls] = current_class_counts.get(best_cls, 0) + 1
                self.running_count += 1
            else:
                break

    def _dispatch_task(self, task_cls: Type[ZenBaseTask]):
        index_to_remove = None
        for i, (cls, kwargs, token) in enumerate(self.queue):
            if cls == task_cls:
                index_to_remove = i
                break
        
        if index_to_remove is None:
            return

        cls, kwargs, token = self.queue.popleft() if index_to_remove == 0 else self._remove_at(index_to_remove)
        
        task_instance = cls(**kwargs)
        task_instance.status = TaskStatus.RUNNING
        task_instance.start_time = time.time()
        # 从 retry_ctx 中获取之前的尝试次数，如果是首次则为 0
        prev_attempts = kwargs.get('_attempts', 0)
        task_instance.attempts = prev_attempts + 1
        task_instance.token = token # 使用入队时生成的 token
        
        info = self.registered_classes[task_cls]
        # 确保 action 接收到的是最新的 kwargs（支持重试时的参数修改）
        # 移除内部使用的 _attempts 字段，避免传递给用户代码
        clean_kwargs = {k: v for k, v in task_instance.kwargs.items() if not k.startswith('_')}
        func = partial(task_instance.action, **clean_kwargs)
        
        executor = self.process_pool if info["use_process"] else self.thread_pool
        future = executor.submit(self._run_wrapper, task_instance, func, info["is_coro"], info["timeout"])
        
        self.running_tasks[future] = task_instance
        
        if task_cls._has_on_start:
            asyncio.ensure_future(task_instance.on_start())

        future.add_done_callback(self._on_task_done)

    def _remove_at(self, index: int):
        items = list(self.queue)
        item = items.pop(index)
        self.queue.clear()
        self.queue.extend(items)
        return item

    def _run_wrapper(self, task: ZenBaseTask, func: Callable, is_coro: bool, timeout: Optional[float]):
        try:
            if task.token.is_cancelled:
                raise asyncio.CancelledError()
            
            if is_coro:
                loop = asyncio.new_event_loop()
                try:
                    coro = func()
                    if timeout:
                        coro = asyncio.wait_for(coro, timeout=timeout)
                    task.result = loop.run_until_complete(coro)
                finally:
                    loop.close()
            else:
                task.result = func()
                
            if task.token.is_cancelled:
                task.status = TaskStatus.CANCELLED
            else:
                task.status = TaskStatus.SUCCESS
        except FuturesTimeoutError:
            task.error = TimeoutError("Task timed out")
            task.status = TaskStatus.FAILED
        except asyncio.CancelledError:
            task.status = TaskStatus.CANCELLED
        except Exception as e:
            task.error = e
            task.status = TaskStatus.FAILED

    def _on_task_done(self, future: Future):
        task = self.running_tasks.pop(future)
        self.running_count -= 1
        self.class_running_counts[type(task)] -= 1
        task.end_time = time.time()

        # 使用管理器保存的 loop 引用
        loop = self._loop

        # 处理重试逻辑
        if task.status == TaskStatus.FAILED and task.attempts <= type(task).max_retries:
            delay = type(task).retry_delay * (2 ** (task.attempts - 1))
            new_kwargs = task.retry_ctx.to_dict()
            # 将当前尝试次数存入 kwargs，供下次实例化时使用
            new_kwargs['_attempts'] = task.attempts
            
            if loop and loop.is_running():
                # 触发 on_retry 钩子
                if type(task)._has_on_retry:
                    asyncio.ensure_future(task.on_retry(task.error, task.retry_ctx, delay), loop=loop)
                
                # 延迟后重新入队
                def re_enqueue():
                    new_token = CancellationToken()
                    self.queue.append((type(task), new_kwargs, new_token))
                    # 触发一次调度
                    if self._loop and self._loop.is_running():
                        asyncio.run_coroutine_threadsafe(self._schedule(), self._loop)
                
                loop.call_later(delay, re_enqueue)
                return # 提前返回，不触发其他钩子
        # 非重试状态下的钩子触发
        if task.status == TaskStatus.SUCCESS:
            if loop and loop.is_running() and type(task)._has_on_success:
                asyncio.ensure_future(task.on_success(), loop=loop)
        elif task.status == TaskStatus.FAILED:
            if loop and loop.is_running() and type(task)._has_on_error:
                asyncio.ensure_future(task.on_error(), loop=loop)
        
        # 触发 on_complete
        if loop and loop.is_running() and type(task)._has_on_complete:
            asyncio.ensure_future(task.on_complete(), loop=loop)

        # 再次触发调度
        if self._is_started and self._loop and self._loop.is_running():
            asyncio.run_coroutine_threadsafe(self._schedule(), self._loop)


class ZenTaskPickleError(Exception):
    """多进程模式下参数序列化失败异常。"""
    pass
