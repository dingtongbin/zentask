# ZenTask (禅意任务调度器)

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/)

ZenTask 是一个专为 Python 打造的**零外部依赖**、**工业级**、**本地后台任务并发调度器**。

它的设计哲学是 **“底层如钢铁般坚硬，上层如流水般柔软”**。对于业务开发者，它提供了极简的 API 和防呆设计，无需理解 GIL、协程切换或线程池原理；对于架构师，它在底层实现了延迟实例化、双轨制调度、元编程钩子短路和多进程物理超度，足以应对千万级吞吐量的生产环境。

## ✨ 核心优势

*   **零依赖**：仅使用 Python 3.9+ 标准库，无需安装 Redis、RabbitMQ 或 Celery。
*   **极致内存**：采用“延迟实例化”架构，百万级任务入队内存占用不超过 50MB。
*   **防雪崩调度**：独创“双轨制调度算法”，完美解决高优任务霸占资源与低优任务饿死的问题。
*   **立体容错**：内置指数退避重试、安全上下文篡改、超时逻辑抛弃与多进程物理超度。

## 🚀 快速开始

只需 3 步，即可让小白开发者跑起高并发任务：

```python
import asyncio
from zentask.core import ZenTaskManager, ZenBaseTask

# 1. 初始化全局调度大脑 (限制全局最大并发为 10)
manager = ZenTaskManager(global_max_workers=10)

# 2. 定义你的业务任务
class CrawlTask(ZenBaseTask):
    priority = 50          # 优先级 (越大越高)
    min_slots = 2          # 保底并发槽位
    max_slots = 8          # 最大并发槽位
    max_retries = 2        # 失败最多重试 2 次
    retry_delay = 1.0      # 基础重试延迟 1 秒
    timeout = 5.0          # 单任务超时 5 秒

    # 核心业务逻辑 (支持同步或异步)
    def action(self, url: str):
        print(f"正在抓取: {url}")
        return f"Data from {url}"

    # 成功后的钩子
    async def on_success(self):
        print(f" 抓取成功，结果: {self.result}")

# 3. 无脑投递任务
async def main():
    for i in range(100):
        manager.enqueue(CrawlTask, url=f"http://example.com/{i}")
    
    # 等待所有任务执行完毕并优雅关闭
    await manager.graceful_shutdown()

asyncio.run(main())
```

## 📖 核心概念

### 1. 任务状态机
每个任务在调度器眼中，严格遵循以下 6 态流转：
`PENDING` → `QUEUED` → `RUNNING` → `SUCCESS / FAILED / CANCELLED`

### 2. 双轨制调度算法 (Dual-Track Scheduling)
ZenTask 摒弃了简单的 FIFO 或纯优先级队列，采用双轨制：
1.  **保底轨 (Min-Slots Track)**：无论优先级多低，只要该类任务的运行数 < `min_slots`，调度器必定为其分配槽位，彻底杜绝低优任务饿死。
2.  **弹性轨 (Elastic Track)**：当所有任务都满足保底条件后，剩余的全局空闲槽位将严格按照 `priority` 降序分配，且单类任务不得超过 `max_slots`。

### 3. 延迟实例化与内存优化
调用 `manager.enqueue()` 时，框架绝对不会实例化 Task 对象，而是仅将 `kwargs` (参数字典) 压入底层 `collections.deque`。只有当全局线程池有空位时，才 `popleft` 并实例化。执行完毕后立刻交由 GC 回收。

## 🛠️ 进阶指南

### 失败重试与安全上下文
当任务失败时，框架采用异步延迟重新入队 (Re-enqueue with Backoff) 策略。

```python
class ScanTask(ZenBaseTask):
    max_retries = 3
    
    async def on_retry(self, exception, retry_ctx, next_delay):
        # 安全读取：不存在绝不报 KeyError
        old_proxy = retry_ctx.get('proxy') 
        
        # 安全篡改：修改 next_kwargs，影响下一次重试的入参
        if "403 Forbidden" in str(exception):
            retry_ctx.set('proxy', 'http://new_ip_pool') 
            print(f"IP被封，已切换代理，{next_delay}秒后重试...")
```

### 多进程隔离与物理超度
对于极易死锁、死循环或调用老旧 C 库的“危险任务”，开启 `use_process = True`。

```python
class DangerTask(ZenBaseTask):
    use_process = True  # 启用多进程模式
    timeout = 3.0       # 必须配合 timeout 使用

    def action(self):
        while True: pass # 即使死循环，触发 timeout 后也会被 OS 层面强杀
```

## 🧪 测试与运行

本项目包含完整的单元测试、白盒测试和黑盒测试。

```bash
# 安装依赖（仅测试需要）
pip install pytest pytest-asyncio

# 运行全量测试
python -m pytest zentask/tests/ -v

# 运行示例程序
python main.py
```

## 📄 许可证

本项目采用 [Apache License 2.0](LICENSE) 开源协议。

---

**ZenTask** —— 让你在并发调度的世界里，也能保持一份“禅意”。🧘‍♂️
