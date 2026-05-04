"""Moteur d'exécution pour pipelines basés sur un DAG.

Exécute les tâches en respectant les dépendances, en parallèle
lorsque possible. Gère les retentatives, les modes d'erreur
(fail_fast, continue_on_error, skip_failed) et le cache de données.

Auteur: SoniqueBay Team
Version: 0.3.2
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any

from taskiq import AsyncBroker

from taskiq_flow.dataflow.cache import DataCache
from taskiq_flow.dataflow.dag import DAG, DAGNode
from taskiq_flow.dataflow.registry import DataflowRegistry
from taskiq_flow.decorators import get_pipeline_metadata
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
    Moteur d'exécution pour pipelines basés sur un DAG.

    Gère l'exécution ordonnée et parallèle des tâches en respectant
    les dépendances de données. Le moteur:
    - Identifie les tâches prêtes (dépendances satisfaites)
    - Exécute les tâches prêtes en parallèle (limite configurable)
    - Gère les retentements selon les métadonnées de chaque tâche
    - Propage les résultats dans le cache de données
    - Gère les modes d'erreur (fail_fast, continue_on_error, skip_failed)

    Exemple d'utilisation:
        engine = ExecutionEngine(
            broker=broker,
            dag=dag,
            fail_fast=True,
            max_parallel=10
        )
        outputs = await engine.execute(inputs={"data": [...]})

    Attributes:
        broker: Broker TaskIQ pour soumettre les tâches
        dag: DAG des tâches à exécuter
        fail_fast: Arrête au premier échec
        continue_on_error: Continue malgré les erreurs
        skip_failed: Ignore les tâches échouées
        max_parallel: Limite de tâches simultanées
    """

    def __init__(
        self,
        broker: AsyncBroker,
        dag: DAG,
        registry: DataflowRegistry | None = None,
        fail_fast: bool = True,
        continue_on_error: bool = False,
        skip_failed: bool = False,
        max_parallel: int | None = None,
    ) -> None:
        """
        Initialize the execution engine.

        Args:
            broker: TaskIQ broker for task execution
            dag: DAG to execute
            fail_fast: If True, stop on first error
            continue_on_error: If True, skip failed tasks and continue
            skip_failed: If True, skip failed tasks without retrying
            max_parallel: Maximum number of parallel tasks (None = unlimited)
        """
        self.broker = broker
        self.dag = dag
        self.fail_fast = fail_fast
        self.continue_on_error = continue_on_error
        self.skip_failed = skip_failed
        self.max_parallel = max_parallel

        # Execution state - use task as key since DAGNode is not hashable
        self.task_states: dict[Any, TaskExecution] = {
            node.task: TaskExecution(node=node) for node in dag.nodes
        }
        self.data_cache = DataCache()
        self.completed_tasks: set[Any] = set()
        self.failed_tasks: set[Any] = set()
        self.running_tasks: set[Any] = set()

        # Pipeline context
        self.pipeline_id: str | None = None
        self.step_counter: int = 0

    def _log(
        self,
        level: int,
        message: str,
        task_node: DAGNode | None = None,
        **kwargs: Any,
    ) -> None:
        """Log a message with pipeline and task context.

        Args:
            level: Logging level
            message: Log message
            task_node: Task node for context
            **kwargs: Additional context to include in log
        """
        extra = {
            "pipeline_id": self.pipeline_id,
            **kwargs,
        }
        if task_node:
            extra["task_name"] = task_node.task.task_name
            extra["step_index"] = self._get_step_index(task_node)
        logger.log(level, message, extra=extra)

    def _get_step_index(self, task_node: DAGNode) -> int:
        """Get step index for a task node.

        Args:
            task_node: Task node

        Returns:
            Step index
        """
        # Find index in topological order
        try:
            sorted_nodes = self.dag.topological_sort()
            return sorted_nodes.index(task_node)
        except (ValueError, RuntimeError):
            return -1

    async def execute(
        self,
        inputs: dict[str, Any],
        pipeline_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Exécute le DAG avec les données d'entrée fournies.

        Args:
            inputs: Dictionnaire des données externes fournies au pipeline.
                    Les clés doivent correspondre aux inputs des tâches
                    sans producteur interne.
            pipeline_id: Optionnel, identifiant pour tracking et logging

        Returns:
            Dictionnaire de toutes les sorties produites, indexées
            par leur nom de flux.

        Raises:
            PipelineError: Si l'exécution échoue (selon mode d'erreur)
            AbortPipeline: Si le pipeline est abandonné via raise AbortPipeline

        Algorithm:
            1. Stocke les inputs externes dans le cache de données
            2. Tant que toutes les tâches ne sont pas terminées:
               - Identifie les tâches prêtes (dépendances complétées)
               - Exécute les tâches prêtes en parallèle (limité par max_parallel)
               - Gère les résultats (succès, échec, retry)
            3. Collecte toutes les sorties des tâches complétées
            4. Retourne le dictionnaire de sorties
        """
        self.pipeline_id = pipeline_id

        # Store external inputs
        self.data_cache.update(inputs)

        self._log(
            logging.INFO,
            "Start pipeline execution",
            task_count=len(self.dag.nodes),
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
                if self.skip_failed:
                    # In skip_failed mode, we don't raise an error
                    self._log(
                        logging.WARNING,
                        "Pipeline completed with skipped tasks",
                        failed_count=len(self.failed_tasks),
                    )
                else:
                    raise self._create_execution_error()

            # Collect all outputs
            outputs = self._collect_outputs()

            self._log(logging.INFO, "Pipeline execution completed successfully")

            return outputs

        except AbortPipeline:
            self._log(logging.INFO, "Pipeline execution aborted")
            raise
        except Exception as e:
            self._log(
                logging.ERROR,
                "Pipeline execution error",
                error=str(e),
            )
            raise

    def _is_complete(self) -> bool:
        """Check if all tasks are completed or failed."""
        total = len(self.dag.nodes)
        completed = len(self.completed_tasks)
        failed = len(self.failed_tasks)

        # In skip_failed mode, failed tasks are marked as skipped
        if self.skip_failed:
            skipped = sum(
                1
                for state in self.task_states.values()
                if state.state == TaskState.SKIPPED
            )
            return completed + skipped >= total

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
            state = self.task_states.get(node.task)
            if state is None or state.state != TaskState.PENDING:
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

        self._log(
            logging.INFO,
            "Task execution started",
            task_node=task_node,
            inputs=list(inputs.keys()),
            retries=retries,
        )

        # Execute with retries
        last_error = None
        start_time = time.time()

        for attempt in range(retries + 1):
            try:
                execution.attempts = attempt + 1

                # Create kicker and execute
                result = await self._execute_task_step(
                    task,
                    inputs,
                    task_node,
                )

                duration = time.time() - start_time

                self._log(
                    logging.INFO,
                    "Task execution completed successfully",
                    task_node=task_node,
                    output_name=output_name,
                    duration=duration,
                    attempt=attempt + 1,
                )

                # Cache result
                self.data_cache.set(output_name, result)

                return result

            except AbortPipeline:
                raise
            except Exception as e:
                last_error = e
                duration = time.time() - start_time

                self._log(
                    logging.WARNING,
                    "Task execution attempt failed",
                    task_node=task_node,
                    attempt=attempt + 1,
                    max_attempts=retries + 1,
                    error=str(e),
                    duration=duration,
                )

                if attempt < retries:
                    # Exponential backoff
                    wait_time = min(2**attempt, 60)

                    self._log(
                        logging.INFO,
                        "Task will be retried",
                        task_node=task_node,
                        wait_time=wait_time,
                        next_attempt=attempt + 2,
                    )

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
        # Check if task is an AsyncTaskiqDecoratedTask (has original_function attribute)
        if hasattr(task, "original_function"):
            original = getattr(task, "original_function", None)
            if original is not None:
                # Get metadata from the original function
                metadata = get_pipeline_metadata(original)
                if metadata:
                    return metadata

        # Check if task has attached metadata (legacy support)
        if hasattr(task, "_pipeline_metadata"):
            meta = task._pipeline_metadata
            return {
                "output": meta.output,
                "inputs": meta.inputs,
                "retries": meta.retries,
            }

        return {}

    def _prepare_inputs(self, task_node: DAGNode) -> dict[str, Any]:
        """Prepare inputs for a task from cached data."""
        inputs = {}
        for dep in task_node.dependencies:
            # Get output name from producer task
            producer = dep.task
            metadata = self._get_task_metadata(producer)
            output_name = metadata.get("output", producer.task_name)
            if self.data_cache.has(output_name):
                inputs[output_name] = self.data_cache.get(output_name)

        # Also include external inputs for tasks with no dependencies
        # This allows the first task in a pipeline to receive external inputs
        if not task_node.dependencies:
            # Get all inputs that were passed to execute()
            for key in self.data_cache.keys:
                if key not in inputs:
                    inputs[key] = self.data_cache.get(key)

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
        result = await kicker.kiq(**inputs)
        # Wait for the result
        task_result = await result.wait_result()
        return task_result.return_value

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

            self._log(
                logging.ERROR,
                "Task execution failed",
                task_node=task_node,
                error=str(result),
            )

            if self.fail_fast:
                # Cancel all pending tasks
                for node in self.dag.nodes:
                    if node.task not in self.completed_tasks:
                        self.task_states[node.task].state = TaskState.SKIPPED
                        self._log(
                            logging.WARNING,
                            "Task skipped due to fail_fast",
                            task_node=node,
                        )
            elif self.skip_failed:
                # Skip failed task and mark as skipped
                execution.state = TaskState.SKIPPED
                self.failed_tasks.discard(task_node.task)
                self._log(
                    logging.WARNING,
                    "Task skipped due to skip_failed mode",
                    task_node=task_node,
                )
            elif self.continue_on_error:
                # Continue execution, mark task as failed but don't stop
                self._log(
                    logging.WARNING,
                    "Task failed but continuing execution",
                    task_node=task_node,
                )
        else:
            execution.state = TaskState.COMPLETED
            execution.result = result
            self.completed_tasks.add(task_node.task)

            self._log(
                logging.INFO,
                "Task result processed successfully",
                task_node=task_node,
            )

    def _create_deadlock_error(self) -> Exception:
        """Create a deadlock error."""
        pending = [
            task.task_name
            for task, state in self.task_states.items()
            if state.state == TaskState.PENDING
        ]
        error_msg = (
            f"Deadlock detected. Pending tasks: {pending}. "
            "Missing data dependencies or circular dependency."
        )

        self._log(
            logging.ERROR,
            "Deadlock detected in pipeline execution",
            pending_tasks=pending,
        )

        return ValueError(error_msg)

    def _create_execution_error(self) -> Exception:
        """Create an execution error."""
        errors = [
            f"{task.task_name}: {state.error}"
            for task, state in self.task_states.items()
            if state.state == TaskState.FAILED
        ]

        if self.skip_failed:
            error_msg = (
                f"Pipeline execution completed with skipped tasks. "
                f"Errors: {'; '.join(errors) if errors else 'None'}"
            )
        else:
            error_msg = f"Pipeline execution failed. Errors: {'; '.join(errors)}"

        self._log(
            logging.ERROR,
            "Pipeline execution failed",
            errors=errors,
        )

        return Exception(error_msg)

    def _collect_outputs(self) -> dict[str, Any]:
        """
        Collect all outputs from the pipeline.

        Returns:
            Dictionary mapping output names to values
        """
        outputs = {}

        for task, state in self.task_states.items():
            if state.state == TaskState.COMPLETED:
                # task is the AsyncTaskiqDecoratedTask
                metadata = self._get_task_metadata(task)
                output_name = metadata.get("output", task.task_name)
                outputs[output_name] = state.result

        self._log(
            logging.INFO,
            "Pipeline outputs collected",
            output_count=len(outputs),
            outputs=list(outputs.keys()),
        )

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
