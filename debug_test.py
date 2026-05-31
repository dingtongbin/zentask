import asyncio
import time
from zentask.core import ZenTaskManager, ZenBaseTask

results = []

class SyncTask(ZenBaseTask):
    def action(self, **kwargs):
        time.sleep(0.1)
        return f"sync_{kwargs['id']}"
    
    async def on_success(self):
        results.append(self.result)

class AsyncTask(ZenBaseTask):
    async def action(self, **kwargs):
        await asyncio.sleep(0.1)
        return f"async_{kwargs['id']}"
    
    async def on_success(self):
        results.append(self.result)

async def main():
    m = ZenTaskManager(global_max_workers=5)
    m.register(SyncTask)
    m.register(AsyncTask)
    m.enqueue(SyncTask, id=1)
    m.enqueue(AsyncTask, id=2)
    await m.start()
    while len(results) < 2 or m.running_count > 0:
        print(f"Progress: Results={len(results)}, Running={m.running_count}")
        await asyncio.sleep(0.1)
    await m.stop()
    print(f"Results: {results}")

if __name__ == "__main__":
    asyncio.run(main())
