"""Pipeline context utilities."""

from typing import Any

from taskiq.brokers import AsyncBroker


class PipelineContext:
    """Context for accessing results from previous pipeline steps."""

    def __init__(self, broker: AsyncBroker):
        self.broker = broker

    async def get_result(self, task_id: str) -> Any:
        """Get result of a previous task by task_id."""
        result = await self.broker.result_backend.get_result(task_id)
        if result.is_err:
            raise RuntimeError(f"Task {task_id} failed: {result.error}")
        return result.return_value