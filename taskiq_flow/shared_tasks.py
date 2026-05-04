"""Shared utility tasks for pipeline operations."""

from typing import Any

from taskiq import async_shared_broker


@async_shared_broker.task(task_name="taskiq_flow.shared.identity")
async def identity_task(value: Any) -> Any:
    """Identity task that returns the input value unchanged."""
    return value
