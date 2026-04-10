import asyncio
from agent import run_agent

async def main():
    try:
        async for chunk in run_agent("Show me the top 3 users"):
            print(chunk)
    except Exception as e:
        print(f"FAILED: {e}")

if __name__ == "__main__":
    asyncio.run(main())
