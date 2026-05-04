"""Quickstart example for taskiq-flow."""

from taskiq import InMemoryBroker

from taskiq_flow import Pipeline
from taskiq_flow.middleware import PipelineMiddleware

# Create broker (using InMemoryBroker for simplicity in quickstart)
# For production, use RedisStreamBroker with a running Redis instance
broker = InMemoryBroker(await_inplace=True).with_middlewares(PipelineMiddleware())


# Define some tasks
@broker.task
def add_one(x: int) -> int:
    """Add one to the input."""
    return x + 1


@broker.task
def multiply_by_two(x: int) -> int:
    """Multiply the input by two."""
    return x * 2


@broker.task
def print_result(x: int) -> None:
    """Print the final result."""
    print(f"Result: {x}")  # noqa: T201


async def main() -> None:
    """Run the quickstart example."""
    # Create pipeline
    pipeline = (
        Pipeline(broker)
        .call_next(add_one)
        .call_next(multiply_by_two)
        .call_next(print_result)
    )

    # Execute pipeline
    result = await pipeline.kiq(5)
    task_id = result.task_id

    # Wait for completion
    await result.wait_result()
    _ = task_id


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
