import asyncio
import random
import time
from zentask.core import ZenTaskManager, ZenBaseTask


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
    # 初始化调度器，全局最大并发数为 5
    manager = ZenTaskManager(global_max_workers=5)
    manager.register(UnstableTask)

    print("=" * 60)
    print("🎲 ZenTask 黑盒测试：随机异常 + 重试 + 抛弃机制")
    print("=" * 60)
    print("\n开始提交 10 个不稳定任务...\n")
    
    # 循环 10 次提交任务
    for i in range(10):
        manager.enqueue(UnstableTask, id=i, data=f"payload_{i}")
        print(f"  📥 已提交任务 {i}")

    print("\n" + "-" * 60)
    print("⏳ 启动调度器，观察执行过程...\n")
    
    # 启动调度器
    await manager.start()

    # 等待所有任务执行完毕
    while manager.running_count > 0 or len(manager.queue) > 0:
        print(f"  📊 进度: 运行中={manager.running_count}, 队列中={len(manager.queue)}")
        await asyncio.sleep(0.5)

    # 优雅关闭
    await manager.graceful_shutdown()
    
    print("\n" + "=" * 60)
    print("🎉 所有任务已完成！")
    print("=" * 60)


if __name__ == '__main__':
    asyncio.run(main())
