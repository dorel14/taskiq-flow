"""Quickstart example for taskiq-flow."""

from taskiq import AsyncBroker, InMemoryBroker
from taskiq_redis import RedisStreamBroker

from taskiq_flow import Pipeline

# Create broker
try:
    broker: AsyncBroker = RedisStreamBroker("redis://localhost:6379")
except Exception as e:
    print(f"Error creating RedisStreamBroker: {e}")  # noqa: T201
    broker = InMemoryBroker()


# Define some tasks
@broker.task
async def add_one(x: int) -> int:
    """Add one to the input."""
    return x + 1


@broker.task
async def multiply_by_two(x: int) -> int:
    """Multiply the input by two."""
    return x * 2


@broker.task
async def print_result(x: int) -> None:
    """Print the final result."""


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
