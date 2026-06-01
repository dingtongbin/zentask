"""
ZenTask: A zero-dependency, industrial-grade local background task scheduler.

Author: dingtongbin
License: Apache-2.0
"""
from zentask.core.manager import ZenTaskManager, ZenBaseTask, TaskStatus, CancellationToken, RetryContext

__version__ = "0.1.0"
__all__ = ["ZenTaskManager", "ZenBaseTask", "TaskStatus", "CancellationToken", "RetryContext"]
