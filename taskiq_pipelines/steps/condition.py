"""Condition step for conditional execution."""

from typing import Any, Callable

import pydantic
from taskiq import AsyncBroker, TaskiqResult

from taskiq_pipelines.abc import AbstractStep
from taskiq_pipelines.constants import CURRENT_STEP, PIPELINE_DATA


class ConditionStep(pydantic.BaseModel, AbstractStep, step_name="condition"):
    """Step that executes conditionally based on previous result."""

    condition: str | Callable[[Any], bool]
    task: Any  # SequentialStep
    else_task: Any | None = None  # SequentialStep | None

    async def act(
        self,
        broker: AsyncBroker,
        step_number: int,
        parent_task_id: str,
        task_id: str,
        pipe_data: bytes,
        result: TaskiqResult[Any],
    ) -> None:
        """Execute conditionally."""
        # Evaluate condition
        if isinstance(self.condition, str):
            # Simple expression evaluation (basic support)
            condition_met = self._eval_condition(self.condition, result.return_value)
        else:
            condition_met = await self.condition(result.return_value)

        if condition_met:
            await self.task.act(broker, step_number, parent_task_id, task_id, pipe_data, result)
        elif self.else_task:
            await self.else_task.act(broker, step_number, parent_task_id, task_id, pipe_data, result)
        # If no else and condition not met, skip this step

    def _eval_condition(self, expression: str, value: Any) -> bool:
        """Simple expression evaluation. Use with caution."""
        # Basic support for simple expressions
        try:
            # Allow 'value' in expression
            return eval(expression, {"__builtins__": {}}, {"value": value})
        except Exception:
            return False