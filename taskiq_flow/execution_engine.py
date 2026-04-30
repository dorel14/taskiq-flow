"""Execution engine for dataflow-based pipelines."""

import asyncio
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any

from taskiq import AsyncBroker

from taskiq_flow.dataflow.dag import DAG, DAGNode
from taskiq_flow.dataflow.registry import DataflowRegistry
from taskiq_flow.exceptions import AbortPipeline

logger = logging.getLogger(__name__)


class TaskState(Enum):
    """State of a task in the execution engine."""

    PENDING = "pending"
    READY = "ready"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class TaskExecution:
    """Tracks execution state of a task."""

    node: DAGNode
    state: TaskState = TaskState.PENDING
    result: Any | None = None
    error: Exception | None = None
    task_id: str | None = None
    attempts: int = 0


class ExecutionEngine:
    """
    Executes a dataflow DAG with automatic parallelism.

    Manages task execution order based on data dependencies,
    handles parallel execution, retries, and error handling.
    """

    def __init__(
        self,
        broker: AsyncBroker,
        dag: DAG,
        registry: DataflowRegistry | None = None,
        fail_fast: bool = True,
        continue_on_error: bool = False,
        max_parallel: int | None = None,
    ) -> None:
        """
        Initialize the execution engine.

        Args:
            broker: TaskIQ broker for task execution
            dag: DAG to execute
            fail_fast: If True, stop on first error
            continue_on_error: If True, skip failed tasks and continue
            max_parallel: Maximum number of parallel tasks (None = unlimited)
        """
        self.broker = broker
        self.dag = dag
        self.fail_fast = fail_fast
        self.continue_on_error = continue_on_error
        self.max_parallel = max_parallel

        # Execution state - use task as key since DAGNode is not hashable
        self.task_states: dict[Any, TaskExecution] = {
            node.task: TaskExecution(node=node) for node in dag.nodes
        }
        self.data_cache: dict[str, Any] = {}
        self.completed_tasks: set[Any] = set()
        self.failed_tasks: set[Any] = set()
        self.running_tasks: set[Any] = set()

        # Pipeline context
        self.pipeline_id: str | None = None
        self.step_counter: int = 0

    async def execute(
        self,
        inputs: dict[str, Any],
        pipeline_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Execute the pipeline with given inputs.

        Args:
            inputs: External inputs to the pipeline
            pipeline_id: Optional pipeline ID for tracking

        Returns:
            Dictionary of all outputs produced by the pipeline

        Raises:
            PipelineError: If execution fails
            AbortPipeline: If pipeline is aborted
        """
        self.pipeline_id = pipeline_id

        # Store external inputs
        self.data_cache.update(inputs)

        logger.info(
            "[PIPELINE] Start pipeline_id=%s, tasks=%d",
            pipeline_id or "unknown",
            len(self.dag.nodes),
        )

        try:
            # Main execution loop
            while not self._is_complete():
                # Get ready tasks
                ready_tasks = self._get_ready_tasks()

                if not ready_tasks:
                    # No ready tasks but not complete
                    if self._has_pending_tasks():
                        # Check for deadlock
                        if self.running_tasks:
                            # Wait for running tasks to complete
                            await self._wait_for_any_task()
                        else:
                            # Deadlock - circular dependency or missing input
                            raise self._create_deadlock_error()
                    else:
                        # All tasks completed
                        break

                # Execute ready tasks
                await self._execute_tasks(ready_tasks)

            # Check for failures
            if self.failed_tasks and not self.continue_on_error:
                raise self._create_execution_error()

            # Collect all outputs
            outputs = self._collect_outputs()

            logger.info(
                "[PIPELINE] Complete pipeline_id=%s duration=N/A",
                pipeline_id or "unknown",
            )

            return outputs

        except AbortPipeline:
            logger.info(
                "[PIPELINE] Aborted pipeline_id=%s",
                pipeline_id or "unknown",
            )
            raise
        except Exception as e:
            logger.error(
                "[PIPELINE] Error pipeline_id=%s error=%s",
                pipeline_id or "unknown",
                str(e),
            )
            raise

    def _is_complete(self) -> bool:
        """Check if all tasks are completed or failed."""
        total = len(self.dag.nodes)
        completed = len(self.completed_tasks)
        failed = len(self.failed_tasks)
        return completed + failed >= total

    def _has_pending_tasks(self) -> bool:
        """Check if there are pending tasks."""
        return any(
            state.state == TaskState.PENDING for state in self.task_states.values()
        )

    def _get_ready_tasks(self) -> list[DAGNode]:
        """
        Get tasks that are ready to execute.

        A task is ready if:
        1. It's in PENDING state
        2. All its dependencies are completed
        3. Max parallel limit not reached
        """
        ready = []

        for node in self.dag.nodes:
            state = self.task_states[node.task]
            if state.state != TaskState.PENDING:
                continue

            # Check if all dependencies are completed
            deps_completed = all(
                dep.task in self.completed_tasks for dep in node.dependencies
            )

            if not deps_completed:
                continue

            # Check max parallel limit
            if self.max_parallel is not None:
                current_parallel = len(self.running_tasks)
                if current_parallel >= self.max_parallel:
                    break

            ready.append(node)

        return ready

    async def _execute_tasks(self, tasks: list[DAGNode]) -> None:
        """
        Execute multiple tasks in parallel.

        Args:
            tasks: List of tasks to execute
        """
        # Mark tasks as running
        for task_node in tasks:
            self.task_states[task_node.task].state = TaskState.RUNNING
            self.running_tasks.add(task_node.task)

        # Execute in parallel
        results = await asyncio.gather(
            *[self._execute_task(task_node) for task_node in tasks],
            return_exceptions=True,
        )

        # Process results
        for task_node, result in zip(tasks, results, strict=False):
            await self._handle_task_result(task_node, result)

    async def _execute_task(self, task_node: DAGNode) -> Any:
        """
        Execute a single task.

        Args:
            task_node: Task to execute

        Returns:
            Task result
        """
        execution = self.task_states[task_node.task]
        task = task_node.task

        # Get task metadata
        metadata = self._get_task_metadata(task)
        output_name = metadata.get("output", task.task_name)
        retries = metadata.get("retries", 0)

        # Prepare inputs
        inputs = self._prepare_inputs(task_node)

        logger.info(
            "[STEP] %s started inputs=%s",
            task.task_name,
            list(inputs.keys()),
        )

        # Execute with retries
        last_error = None
        for attempt in range(retries + 1):
            try:
                execution.attempts = attempt + 1

                # Create kicker and execute
                result = await self._execute_task_step(
                    task,
                    inputs,
                    task_node,
                )

                logger.info(
                    "[STEP] %s completed output=%s duration=N/A",
                    task.task_name,
                    output_name,
                )

                # Cache result
                self.data_cache[output_name] = result

                return result

            except AbortPipeline:
                raise
            except Exception as e:
                last_error = e
                logger.warning(
                    "[STEP] %s attempt %d failed error=%s",
                    task.task_name,
                    attempt + 1,
                    str(e),
                )

                if attempt < retries:
                    # Exponential backoff
                    wait_time = min(2**attempt, 60)
                    await asyncio.sleep(wait_time)

        # All retries exhausted
        raise last_error or Exception(f"Task {task.task_name} failed")

    async def _wait_for_any_task(self) -> None:
        """Wait for any running task to complete."""
        # Simple implementation - wait a short time
        # In a real implementation, this would use asyncio.Event or similar
        await asyncio.sleep(0.1)

    def _get_task_metadata(self, task: Any) -> dict[str, Any]:
        """Get metadata for a task."""
        # Check if task has metadata attribute
        if hasattr(task, "metadata"):
            return task.metadata  # type: ignore
        return {}

    def _prepare_inputs(self, task_node: DAGNode) -> dict[str, Any]:
        """Prepare inputs for a task from cached data."""
        inputs = {}
        for dep in task_node.dependencies:
            # Get output name from producer task
            producer = dep.task
            metadata = self._get_task_metadata(producer)
            output_name = metadata.get("output", producer.task_name)
            if output_name in self.data_cache:
                inputs[output_name] = self.data_cache[output_name]
        return inputs

    async def _execute_task_step(
        self,
        task: Any,
        inputs: dict[str, Any],
        task_node: DAGNode,
    ) -> Any:
        """Execute a single task step."""
        # Create kicker and execute
        kicker = task.kicker()
        return await kicker.kiq(**inputs)

    async def _handle_task_result(
        self,
        task_node: DAGNode,
        result: Any,
    ) -> None:
        """
        Handle the result of a task execution.

        Args:
            task_node: Task that completed
            result: Result or exception
        """
        execution = self.task_states[task_node.task]

        self.running_tasks.discard(task_node.task)

        if isinstance(result, Exception):
            execution.state = TaskState.FAILED
            execution.error = result
            self.failed_tasks.add(task_node.task)

            if self.fail_fast:
                # Cancel all pending tasks
                for node in self.dag.nodes:
                    if node.task not in self.completed_tasks:
                        self.task_states[node.task].state = TaskState.SKIPPED
        else:
            execution.state = TaskState.COMPLETED
            execution.result = result
            self.completed_tasks.add(task_node.task)

    def _create_deadlock_error(self) -> Exception:
        """Create a deadlock error."""
        pending = [
            node.task.task_name
            for node, state in self.task_states.items()
            if state.state == TaskState.PENDING
        ]
        return ValueError(
            f"Deadlock detected. Pending tasks: {pending}. "
            "Missing data dependencies or circular dependency.",
        )

    def _create_execution_error(self) -> Exception:
        """Create an execution error."""
        errors = [
            f"{node.task.task_name}: {state.error}"
            for node, state in self.task_states.items()
            if state.state == TaskState.FAILED
        ]
        return Exception(f"Pipeline execution failed. Errors: {'; '.join(errors)}")

    def _collect_outputs(self) -> dict[str, Any]:
        """
        Collect all outputs from the pipeline.

        Returns:
            Dictionary mapping output names to values
        """
        outputs = {}

        for node, state in self.task_states.items():
            if state.state == TaskState.COMPLETED:
                metadata = self._get_task_metadata(node.task)
                output_name = metadata.get("output", node.task.task_name)
                outputs[output_name] = state.result

        return outputs

    def get_execution_report(self) -> dict[str, Any]:
        """
        Get a report of the execution.

        Returns:
            Dictionary with execution statistics
        """
        total = len(self.dag.nodes)
        completed = len(self.completed_tasks)
        failed = len(self.failed_tasks)
        running = len(self.running_tasks)
        pending = total - completed - failed - running

        return {
            "total_tasks": total,
            "completed": completed,
            "failed": failed,
            "running": running,
            "pending": pending,
            "success_rate": completed / total if total > 0 else 0,
            "data_cache_size": len(self.data_cache),
        }
