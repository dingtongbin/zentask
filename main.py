import asyncio
import logging
import random
import time
from zentask.core import ZenTaskManager, ZenBaseTask
from zentask.core.state_machine import AsyncStateMachine

logging.basicConfig(level=logging.INFO)


class UnstableTask(ZenBaseTask):
    """不稳定任务：随机抛出异常、重试 3 次、随机彻底失败"""
    
    priority = 50
    min_slots = 2
    max_slots = 5
    max_retries = 3  # 最多重试 3 次
    retry_delay = 0.5  # 基础重试延迟 0.5 秒
    
    def action(self, **kwargs):
        task_id = kwargs.get('id')
        
        # 模拟随机异常（30% 概率抛出异常）
        if random.random() < 0.3:
            error_types = [
                ValueError(f"Task {task_id} 数据验证失败"),
                TimeoutError(f"Task {task_id} 超时"),
                ConnectionError(f"Task {task_id} 连接失败"),
            ]
            raise random.choice(error_types)
        
        # 模拟随机彻底失败（10% 概率，即使重试也会失败）
        if random.random() < 0.1:
            raise RuntimeError(f"Task {task_id} 发生不可恢复的错误，将被抛弃")
        
        # 正常执行：等待随机时间
        wait_time = random.uniform(0.5, 1.5)
        time.sleep(wait_time)
        print(f"[Task {task_id}] ✅ 成功完成，耗时 {wait_time:.2f} 秒")
        return f"Success: {kwargs}"

    async def on_start(self):
        print(f"[Task {self.kwargs.get('id')}] 🚀 开始执行 (尝试第 {self.attempts} 次)")

    async def on_success(self):
        print(f"  -> [Task {self.kwargs.get('id')}] ✨ 最终成功！结果: {self.result}")

    async def on_retry(self, exception, retry_ctx, next_delay):
        task_id = self.kwargs.get('id')
        print(f"  -> [Task {task_id}] ⚠️  第 {self.attempts} 次失败: {type(exception).__name__}: {exception}")
        print(f"     🔁 将在 {next_delay:.2f} 秒后重试...")

    async def on_error(self):
        task_id = self.kwargs.get('id')
        print(f"  -> [Task {task_id}] ❌ 重试耗尽，任务被抛弃！最后错误: {self.error}")

    async def on_complete(self):
        task_id = self.kwargs.get('id')
        status_icon = "✅" if self.status.name == "SUCCESS" else "❌"
        print(f"  -> [Task {task_id}] {status_icon} 任务终结，状态: {self.status.value}, 总尝试次数: {self.attempts}")


async def main():
    # 初始化状态机与调度器
    sm = AsyncStateMachine()
    manager = ZenTaskManager(global_max_workers=5)

    print("=" * 60)
    print("🚀 ZenTask + AsyncStateMachine 联动模拟")
    print("=" * 60)
    
    # 1. 先在内存状态机中创建任务（模拟先请求状态机）
    task_id = await sm.create_task(timeout=10)
    print(f"\n[State Machine] 任务已创建: {task_id}")
    
    # 2. 定义 Ping 任务类
    class PingTask(ZenBaseTask):
        priority = 50
        min_slots = 2
        max_slots = 5
        timeout = 5.0

        def action(self, **kwargs):
            job_id = kwargs.get('job_id')
            delay = random.uniform(1.0, 3.0)
            print(f"  [Worker] 🏃‍♂️ {job_id} 开始执行，预计延迟 {delay:.2f}s")
            time.sleep(delay)
            return f"Ping result after {delay:.2f}s"

        async def on_complete(self):
            # 任务结束时，将结果传递给状态机
            job_id = self.kwargs.get('job_id')
            # 从 kwargs 中获取状态机的大任务 ID
            sm_task_id = self.kwargs.get('sm_task_id')
            print(f"  [Hook] 🏁 {job_id} 执行完毕，正在汇报给状态机任务: {sm_task_id}")
            if self.status.name == "SUCCESS":
                await sm.push(sm_task_id, {"job_id": job_id, "status": "success", "result": self.result})
                sm.mark_job_done(sm_task_id, job_id)
            else:
                await sm.push(sm_task_id, {"job_id": job_id, "status": "failed", "error": str(self.error)})
                sm.mark_job_done(sm_task_id, job_id)

    manager.register(PingTask)

    # 3. 模拟 10 个 ping，收集所有任务 ID
    job_ids = set()
    for i in range(10):
        job_id = f"ping_{i}"
        job_ids.add(job_id)
        # 关键修复：把状态机的大 task_id 传给每个子任务
        manager.enqueue(PingTask, job_id=job_id, sm_task_id=task_id)
        print(f"  📥 已提交 Ping 任务: {job_id}")

    # 4. 把任务 ID 一次性提交给内存状态机
    sm.set_expected_jobs(task_id, job_ids)
    print(f"\n[State Machine] 期望 Job 集合已更新: {len(job_ids)} 个任务")

    # 5. 启动后台监控：实时打印状态机数据
    async def monitor_state_machine():
        while True:
            if task_id not in sm._states:
                print("\n[Monitor] 状态机任务已结束，监控停止。")
                break
            
            expected = sm._expected_jobs.get(task_id, set())
            completed = sm._completed_jobs.get(task_id, set())
            # 使用 print 而不是 \r，确保每行都能看到
            print(f"[Monitor] 进度: {len(completed)}/{len(expected)} | 已完成: {completed}")
            await asyncio.sleep(0.5)

    monitor_task = asyncio.ensure_future(monitor_state_machine())

    # 6. 启动调度器并等待完成
    await manager.start()
    print("\n⏳ 调度器已启动，任务正在执行中...\n")
    
    # 等待所有任务执行完毕
    start_time = time.time()
    while (manager.running_count > 0 or len(manager.queue) > 0) and (time.time() - start_time < 30):
        await asyncio.sleep(0.5)
    
    print("\n🛑 所有任务执行完毕，正在关闭调度器...")
    await manager.graceful_shutdown()
    
    # 等待监控任务自然结束
    try:
        await asyncio.wait_for(monitor_task, timeout=2.0)
    except asyncio.TimeoutError:
        monitor_task.cancel()
    print("\n" + "=" * 60)
    print("🎉 模拟结束！")
    print("=" * 60)


if __name__ == '__main__':
    asyncio.run(main())
