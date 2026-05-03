from collections.abc import Callable
from typing import Any

import pytest
from taskiq import InMemoryBroker

from taskiq_flow import PipelineMiddleware, pipeline_task


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    """
    Anyio backend.

    Backend for anyio pytest plugin.
    :return: backend name.
    """
    return "asyncio"


@pytest.fixture
def broker() -> InMemoryBroker:
    """
    Create a test broker with PipelineMiddleware.

    Returns:
        Configured InMemoryBroker instance.
    """
    broker = InMemoryBroker()
    broker.add_middlewares(PipelineMiddleware())
    return broker


@pytest.fixture
def simple_task(broker: InMemoryBroker) -> Callable[[int], Any]:
    """
    Create a simple test task.

    Returns:
        Decorated task function.
    """

    @broker.task
    async def add_one(value: int) -> int:
        """Simple task that adds 1 to input."""
        return value + 1

    return add_one


@pytest.fixture
def dataflow_task(broker: InMemoryBroker) -> Callable[[dict[str, Any]], Any]:
    """
    Create a dataflow-enabled test task.

    Returns:
        Decorated task function with @pipeline_task.
    """

    @broker.task
    @pipeline_task(output="result")
    async def process_data(data: dict[str, Any]) -> dict[str, Any]:
        """Process data and return result."""
        return {"processed": data.get("input", 0) + 1}

    return process_data
