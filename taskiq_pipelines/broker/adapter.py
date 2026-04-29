"""Broker adapter for result backend operations."""

from typing import Any

from taskiq import AsyncBroker


class BrokerAdapter:
    """Adapter for broker result backend operations."""

    def __init__(self, broker: AsyncBroker) -> None:
        self.broker = broker

    async def is_result_ready(self, task_id: str) -> bool:
        """Check if result is ready."""
        return await self.broker.result_backend.is_result_ready(task_id)

    async def get_result(self, task_id: str) -> Any:
        """Get result for task."""
        return await self.broker.result_backend.get_result(task_id)

    async def set_result(self, task_id: str, result: Any) -> None:
        """Set result for task."""
        await self.broker.result_backend.set_result(task_id, result)

    def get_task_id(self, task_id: str) -> str:
        """Get task ID (pass through)."""
        return task_id
