import asyncio
from zentask.core.state_machine import AsyncStateMachine

async def main():
    sm = AsyncStateMachine()
    tid = await sm.create_task(timeout=0.2)
    print('Task created:', tid)
    
    # 启动订阅者
    async def subscriber():
        async for data in sm.subscribe(tid):
            print('Received:', data)
            if data.get("type") == "finished":
                break
    
    sub_task = asyncio.ensure_future(subscriber())
    
    await asyncio.sleep(1.0)
    print('States after timeout:', list(sm._states.keys()))
    await sub_task

if __name__ == "__main__":
    asyncio.run(main())
