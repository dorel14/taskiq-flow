"""Condition step for conditional execution."""

from collections.abc import Callable
from typing import Any

import pydantic
from taskiq import AsyncBroker, TaskiqResult

from taskiq_pipelines.abc import AbstractStep


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
        pipe_data: str,
        result: TaskiqResult[Any],
    ) -> None:
        """Execute conditionally."""
        # Evaluate condition
        if isinstance(self.condition, str):
            # Simple expression evaluation (basic support)
            condition_met = self._eval_condition(self.condition, result.return_value)
        elif callable(self.condition):
            # Check if it's a coroutine function
            import asyncio
            if asyncio.iscoroutinefunction(self.condition):
                condition_met = await self.condition(result.return_value)
            else:
                condition_met = self.condition(result.return_value)
        else:
            condition_met = bool(self.condition)

        if condition_met:
            await self.task.act(broker, step_number, parent_task_id, task_id, pipe_data, result)
        elif self.else_task:
            await self.else_task.act(broker, step_number, parent_task_id, task_id, pipe_data, result)
        # If no else and condition not met, skip this step

    def _eval_condition(self, expression: str, value: Any) -> bool:
        """Simple expression evaluation. Use with caution."""
        # Basic support for simple expressions
        try:
            # Allow 'value' in expression and some basic functions
            safe_builtins = {
                "len": len,
                "str": str,
                "int": int,
                "float": float,
                "bool": bool,
            }
            return eval(expression, {"__builtins__": safe_builtins}, {"value": value})
        except Exception:
            return False
