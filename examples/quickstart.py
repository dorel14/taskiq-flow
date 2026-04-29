"""Quickstart example for taskiq-pipelines."""

from taskiq import TaskiqTask
from taskiq_redis import RedisBroker

from taskiq_pipelines import Pipeline


# Define some tasks
@taskiq.task
async def add_one(x: int) -> int:
    return x + 1

@taskiq.task
async def multiply_by_two(x: int) -> int:
    return x * 2

@taskiq.task
async def print_result(x: int) -> None:
    print(f"Final result: {x}")


async def main():
    # Create broker
    broker = RedisBroker("redis://localhost:6379")

    # Create pipeline
    pipeline = (
        Pipeline(broker)
        .call_next(add_one)
        .call_next(multiply_by_two)
        .call_next(print_result)
    )

    # Execute pipeline
    result = await pipeline.kiq(5)
    print(f"Pipeline task ID: {result.task_id}")

    # Wait for completion
    await result.wait_result()
    print("Pipeline completed!")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())