import asyncio
import time
from zentask.core import ZenTaskManager, ZenBaseTask


class CrawlTask(ZenBaseTask):
    priority = 50
    min_slots = 2
    max_slots = 8
    max_retries = 2
    retry_delay = 1.0
    timeout = 5.0

    def action(self, url: str):
        print(f"正在抓取: {url}")
        # 模拟网络请求
        time.sleep(0.5)
        return f"Data from {url}"

    async def on_success(self):
        print(f" 抓取成功，结果: {self.result}")

    async def on_retry(self, exception, retry_ctx, next_delay):
        print(f" 抓取失败: {exception}, {next_delay}秒后重试...")


async def main():
    # 1. 初始化全局调度大脑 (限制全局最大并发为 10)
    manager = ZenTaskManager(global_max_workers=10)

    # 2. 无脑投递任务
    print("Submitting tasks...")
    for i in range(20):
        manager.enqueue(CrawlTask, url=f"http://example.com/{i}")
    
    # 3. 等待所有任务执行完毕并优雅关闭
    await manager.start()
    while manager.running_count > 0 or len(manager.queue) > 0:
        print(f"Progress: Running={manager.running_count}, Queued={len(manager.queue)}")
        await asyncio.sleep(1)
    
    await manager.graceful_shutdown()
    print("All tasks completed.")


if __name__ == "__main__":
    asyncio.run(main())
