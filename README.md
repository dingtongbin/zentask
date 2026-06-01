# ZenTask (禅意任务调度器)

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![GitHub](https://img.shields.io/github/stars/dingtongbin/zentask?style=social)](https://github.com/dingtongbin/zentask)

ZenTask 是一个专为 Python 打造的**零外部依赖**、**工业级**、**本地后台任务并发调度器**。

它的设计哲学是 **“底层如钢铁般坚硬，上层如流水般柔软”**。对于业务开发者，它提供了极简的 API 和防呆设计，无需理解 GIL、协程切换或线程池原理；对于架构师，它在底层实现了延迟实例化、双轨制调度、元编程钩子短路和多进程物理超度，足以应对千万级吞吐量的生产环境。

## 核心优势

*   **零依赖**：仅使用 Python 3.10+ 标准库，无需安装 Redis、RabbitMQ 或 Celery。
*   **极致内存**：采用“延迟实例化”架构，百万级任务入队内存占用不超过 50MB。
*   **防雪崩调度**：独创“双轨制调度算法”，完美解决高优任务霸占资源与低优任务饿死的问题。
*   **立体容错**：内置指数退避重试、安全上下文篡改、超时逻辑抛弃与多进程物理超度。
*   **异步状态机**：配套纯异步内存状态机，支持批量任务对账、SSE 实时进度推送与超时看门狗。

## 快速开始

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

## 核心概念

### 1. 任务状态机 (State Machine)
每个任务在调度器眼中，严格遵循以下流转过程。理解这些状态有助于你编写更健壮的业务逻辑：

*   **PENDING**：任务刚被 `enqueue` 提交，但尚未进入底层队列。
*   **QUEUED**：任务已入队，正在等待全局线程池分配槽位。
*   **RUNNING**：任务已被实例化并正在执行 `action` 方法。
*   **SUCCESS**：任务执行成功且无异常抛出。
*   **FAILED**：任务重试次数耗尽或发生了不可恢复的错误。
*   **CANCELLED**：任务在执行前被 `cancel()` 调用取消。

### 2. 钩子函数详解与代码演示 (Lifecycle Hooks)
ZenTask 提供了全生命周期的异步钩子。你可以重写这些方法来处理业务副作用（如记录日志、发送通知）。

#### on_start: 启动瞬间
触发时机：任务即将开始执行 `action` 之前。
```python
async def on_start(self):
    self.start_time = time.time()
    print(f"任务 {self.kwargs['id']} 开始运行")
```

#### on_success: 凯旋而归
触发时机：任务执行成功并返回结果后。
```python
async def on_success(self):
    # self.result 包含了 action 的返回值
    await save_to_db(self.kwargs['id'], self.result)
```

#### on_retry: 屡败屡战
触发时机：任务失败准备重新入队时。这里可以干预下一次重试的行为。
```python
async def on_retry(self, exception, retry_ctx, next_delay):
    if "timeout" in str(exception):
        # 动态增加超时时间
        retry_ctx.set('timeout', self.timeout * 2)
```

#### on_error: 彻底放弃
触发时机：任务彻底失败（重试耗尽）后。
```python
async def on_error(self):
    # self.error 包含了最后的异常对象
    send_alert(f"任务 {self.kwargs['id']} 最终失败: {self.error}")
```

#### on_cancel: 紧急叫停
触发时机：任务被手动 `manager.cancel(task_id)` 取消时。
```python
async def on_cancel(self):
    print("任务被用户取消，正在清理临时文件...")
    os.remove(self.temp_file)
```

#### on_complete: 尘埃落定
触发时机：无论成功、失败还是取消都会触发，是最终的收尾工作。
```python
async def on_complete(self):
    duration = time.time() - self.start_time
    print(f"任务终结，状态: {self.status.value}, 耗时: {duration:.2f}s")
```

### 3. 双轨制调度算法 (Dual-Track Scheduling)
ZenTask 摒弃了简单的 FIFO 或纯优先级队列，采用双轨制：
1.  **保底轨 (Min-Slots Track)**：无论优先级多低，只要该类任务的运行数 < `min_slots`，调度器必定为其分配槽位，彻底杜绝低优任务饿死。
2.  **弹性轨 (Elastic Track)**：当所有任务都满足保底条件后，剩余的全局空闲槽位将严格按照 `priority` 降序分配，且单类任务不得超过 `max_slots`。

### 4. 延迟实例化与内存优化
调用 `manager.enqueue()` 时，框架绝对不会实例化 Task 对象，而是仅将 `kwargs` (参数字典) 压入底层 `collections.deque`。只有当全局线程池有空位时，才 `popleft` 并实例化。执行完毕后立刻交由 GC 回收。

## 进阶指南

### 场景一：失败重试与安全上下文篡改
当任务失败时，框架会自动计算指数退避延迟并重新入队。你可以在 `on_retry` 中干预下一次重试的行为：

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

### 场景二：多进程隔离与物理超度
对于极易死锁、死循环或调用老旧 C 库的“危险任务”，开启 `use_process = True`。此时任务会在独立的子进程中运行，即使发生崩溃也不会影响主程序：

```python
class DangerTask(ZenBaseTask):
    use_process = True  # 启用多进程模式
    timeout = 3.0       # 必须配合 timeout 使用

    def action(self):
        while True: pass # 即使死循环，触发 timeout 后也会被 OS 层面强杀
```

### 场景三：优雅关闭与防并发逃逸
在生产环境中，通常需要在服务器重启时确保当前运行的任务不丢失：

```python
# 停止接收新任务，并等待所有运行中和队列中的任务完成
await manager.graceful_shutdown()
```

### 场景四：任务取消与惩罚机制
如果某个高优任务不再需要执行，可以将其取消。取消的任务会触发 `on_cancel` 钩子，并计入该类的取消惩罚计数，暂时降低其后续任务的调度优先级：

```python
task_id = manager.enqueue(MyTask, ...)
manager.cancel(task_id) # 立即从队列移除或标记为取消
```

## 联动 AsyncStateMachine (异步状态机)

ZenTask 现已集成配套的纯异步内存状态机，用于追踪批量任务进度并支持 SSE 数据流推送。

### 为什么需要状态机？
*   **批量对账**：当你一次性提交 100 个子任务时，状态机能帮你统计“已完成/总数”。
*   **SSE 推送**：通过 `subscribe` 接口，前端可以实时收到任务进度的心跳和数据更新。
*   **超时看门狗**：为整个批量任务设置一个总超时时间，防止部分子任务卡死导致整体无法结束。

### 联动示例
```python
from zentask.core.state_machine import AsyncStateMachine

async def batch_process():
    sm = AsyncStateMachine()
    manager = ZenTaskManager(global_max_workers=5)

    # 1. 先在状态机创建一个总任务
    task_id = await sm.create_task(timeout=60) 

    class SubTask(ZenBaseTask):
        async def on_complete(self):
            # 子任务完成后向状态机汇报
            job_id = self.kwargs['job_id']
            sm.mark_job_done(task_id, job_id)

    # 2. 提交子任务并告知状态机期望的 Job ID 集合
    job_ids = {f"job_{i}" for i in range(10)}
    for jid in job_ids:
        manager.enqueue(SubTask, job_id=jid)
    
    sm.set_expected_jobs(task_id, job_ids)

    # 3. 启动监控
    async for data in sm.subscribe(task_id):
        print(f"收到进度: {data}")
        if data.get("type") == "finished":
            break

    await manager.graceful_shutdown()
```

## 测试与运行

本项目包含完整的单元测试、白盒测试和黑盒测试。

```bash
# 安装依赖（仅测试需要）
pip install pytest pytest-asyncio

# 运行全量测试
python -m pytest zentask/tests/ -v

# 运行示例程序
python main.py
```

## 许可证

本项目采用 [Apache License 2.0](LICENSE) 开源协议。

---

**ZenTask** —— 让你在并发调度的世界里，也能保持一份“禅意”。
