import asyncio
from agent import run_agent

async def main():
    try:
        async for chunk in run_agent("Who are you?"):
            print(chunk)
    except Exception as e:
        print(f"Test failed with error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
