import asyncio
from agent import run_agent

async def test():
    print("Testing run_agent...")
    result = await run_agent("what campaigns are active right now?")
    print("Agent Result:", result)

asyncio.run(test())
