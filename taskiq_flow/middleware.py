"""Middleware Pipeline pour l'orchestration de workflows.

Ce module contient le PipelineMiddleware qui intercepte l'exécution
des tâches TaskIQ pour gérer le flux d'exécution des pipelines.
Il décide quelle étape exécuter ensuite, gère le suivi (tracking)
et les hooks d'événements.

Auteur: SoniqueBay Team
Version: 0.3.2
"""

from logging import getLogger
from typing import Any, cast

import pydantic
from taskiq import TaskiqMessage, TaskiqMiddleware, TaskiqResult

from taskiq_flow.constants import CURRENT_STEP, PIPELINE_DATA, PIPELINE_ID
from taskiq_flow.exceptions import AbortPipeline
from taskiq_flow.hooks.events import (
    PipelineCompleteEvent,
    PipelineErrorEvent,
    StepCompleteEvent,
    StepErrorEvent,
    StepStartEvent,
)
from taskiq_flow.pipeliner import DumpedStep
from taskiq_flow.steps import parse_step

logger = getLogger(__name__)


class PipelineMiddleware(TaskiqMiddleware):
    """
    Middleware TaskIQ pour l'orchestration de pipelines.

    C'est le composant cœur de taskiq-flow. Intercepte chaque
    tâche exécutée via le broker et:
    - Détecte si la tâche fait partie d'un pipeline (labels)
    - Détermine l'étape suivante à exécuter
    - Gère le tracking et les hooks
    - Lance la tâche suivante ou termine le pipeline

    Architecture:
        post_save() est appelé après chaque tâche. Il lit
        l'étape courante depuis les labels, exécute l'étape
        suivante via step.act(), ou termine le pipeline.

    Usage:
        broker = InMemoryBroker().with_middlewares(PipelineMiddleware())

    Attributes:
        tracking_manager: Gestionnaire de suivi (optionnel)
        hook_manager: Gestionnaire d'événements (optionnel)
    """

    def __init__(
        self,
        tracking_manager: Any = None,  # PipelineTrackingManager | None
        hook_manager: Any = None,  # HookManager | None
    ) -> None:
        self.tracking_manager = tracking_manager
        self.hook_manager = hook_manager
        super().__init__()

    async def post_save(
        self,
        message: "TaskiqMessage",
        result: "TaskiqResult[Any]",
    ) -> None:
        """
        Handler principal appelé après exécution d'une tâche.

        Détermine si la tâche fait partie d'un pipeline en vérifiant
        la présence du label CURRENT_STEP. Si oui:
        1. Notifie le démarrage de l'étape suivante (hook + tracking)
        2. Si c'était la dernière étape: termine le pipeline
        3. Sinon: exécute l'étape suivante via _execute_next_step()

        Args:
            message: Message de la tâche qui vient de se terminer
            result: Résultat de cette tâche

        Note:
            Si result.is_err, l'étape est considérée échouée mais
            le traitement d'erreur est délégué à on_error()
        """
        if result.is_err:
            return

        pipeline_info = self._extract_pipeline_info(message)
        if pipeline_info[0] is None:
            return

        current_step_num, pipeline_id, steps_data = pipeline_info

        await self._handle_step_start(current_step_num, pipeline_id, steps_data)

        if current_step_num + 1 >= len(steps_data):
            await self._handle_pipeline_completion(pipeline_id, result)
            return

        await self._execute_next_step(
            current_step_num,
            pipeline_id,
            steps_data,
            message,
            result,
        )

    def _extract_pipeline_info(
        self,
        message: "TaskiqMessage",
    ) -> tuple[int, str | None, list[DumpedStep]] | tuple[None, None, None]:
        """Extract pipeline information from message."""
        if CURRENT_STEP not in message.labels:
            return None, None, None

        current_step_num = int(message.labels[CURRENT_STEP])
        pipeline_id = message.labels.get(PIPELINE_ID)

        if PIPELINE_DATA not in message.labels:
            logger.warning("Pipeline data not found. Execution flow is broken.")
            return None, None, None

        pipeline_data = message.labels[PIPELINE_DATA]
        parsed_data = self.broker.serializer.loadb(pipeline_data)
        try:
            steps_data = pydantic.TypeAdapter(list[DumpedStep]).validate_python(
                parsed_data,
            )
        except ValueError as err:
            logger.warning("Cannot parse pipeline_data: %s", err, exc_info=True)
            return None, None, None

        return current_step_num, pipeline_id, steps_data

    async def _handle_step_start(
        self,
        current_step_num: int,
        pipeline_id: str | None,
        steps_data: list[DumpedStep],
    ) -> None:
        """Handle the start of a step."""
        if self.hook_manager and pipeline_id:
            await self._dispatch_step_start_hook(
                current_step_num,
                pipeline_id,
                steps_data,
            )

        if self.tracking_manager and pipeline_id:
            await self._mark_step_started_tracking(
                current_step_num,
                pipeline_id,
                steps_data,
            )

        self._log_step_start(current_step_num, pipeline_id, steps_data)

    async def _dispatch_step_start_hook(
        self,
        current_step_num: int,
        pipeline_id: str,
        steps_data: list[DumpedStep],
    ) -> None:
        """Dispatch step start hook."""
        current_step_data = steps_data[current_step_num]
        parsed_step = parse_step(
            current_step_data.step_type,
            current_step_data.step_data,
        )
        await self.hook_manager.dispatch(
            StepStartEvent(
                pipeline_id=pipeline_id,
                step_index=current_step_num,
                task_name=cast(str, getattr(parsed_step, "task_name", "unknown")),
                task_id=current_step_data.task_id,
            ),
        )

    async def _mark_step_started_tracking(
        self,
        current_step_num: int,
        pipeline_id: str,
        steps_data: list[DumpedStep],
    ) -> None:
        """Mark step as started in tracking."""
        current_step_data = steps_data[current_step_num]
        parsed_step = parse_step(
            current_step_data.step_type,
            current_step_data.step_data,
        )
        await self.tracking_manager.mark_step_started(
            pipeline_id,
            current_step_num,
            current_step_data.task_id,
            cast(str, getattr(parsed_step, "task_name", "unknown")),
        )

    def _log_step_start(
        self,
        current_step_num: int,
        pipeline_id: str | None,
        steps_data: list[DumpedStep],
    ) -> None:
        """Log step start."""
        current_step_data = steps_data[current_step_num]
        parsed_step = parse_step(
            current_step_data.step_type,
            current_step_data.step_data,
        )
        task_name = cast(str, getattr(parsed_step, "task_name", "unknown"))
        logger.info(
            f"[{pipeline_id or 'unknown'}][STEP {current_step_num}] START {task_name}",
            extra={
                "pipeline_id": pipeline_id,
                "step": current_step_num,
                "task": task_name,
                "event": "step_start",
            },
        )

    async def _handle_pipeline_completion(
        self,
        pipeline_id: str | None,
        result: "TaskiqResult[Any]",
    ) -> None:
        """Handle pipeline completion."""
        logger.debug("Pipeline is completed.")

        if self.tracking_manager and pipeline_id:
            await self.tracking_manager.mark_pipeline_completed(
                pipeline_id,
                result.return_value,
            )

        if self.hook_manager and pipeline_id:
            await self.hook_manager.dispatch(
                PipelineCompleteEvent(
                    pipeline_id=pipeline_id,
                    result=result.return_value,
                ),
            )

    async def _execute_next_step(
        self,
        current_step_num: int,
        pipeline_id: str | None,
        steps_data: list[DumpedStep],
        message: "TaskiqMessage",
        result: "TaskiqResult[Any]",
    ) -> None:
        """Execute the next step in the pipeline."""
        next_step_data = steps_data[current_step_num + 1]

        try:
            next_step = parse_step(
                step_type=next_step_data.step_type,
                step_data=next_step_data.step_data,
            )
        except ValueError as exc:
            logger.warning("Cannot parse step data.")
            logger.debug("%s", exc, exc_info=True)
            return

        try:
            await next_step.act(
                broker=self.broker,
                step_number=current_step_num + 1,
                parent_task_id=message.task_id,
                task_id=next_step_data.task_id,
                pipe_data=message.labels[PIPELINE_DATA],
                result=result,
            )

            await self._handle_step_completion(
                current_step_num,
                pipeline_id,
                steps_data,
                result,
            )

        except AbortPipeline as abort_exc:
            await self._handle_step_error(
                current_step_num,
                pipeline_id,
                steps_data,
                abort_exc,
                len(steps_data),
            )
            if current_step_num == len(steps_data) - 1:
                return
            await self.fail_pipeline(steps_data[-1].task_id)

    async def _handle_step_completion(
        self,
        current_step_num: int,
        pipeline_id: str | None,
        steps_data: list[DumpedStep],
        result: "TaskiqResult[Any]",
    ) -> None:
        """Handle successful step completion."""
        if self.hook_manager and pipeline_id:
            await self._dispatch_step_complete_hook(
                current_step_num,
                pipeline_id,
                steps_data,
                result,
            )

        if self.tracking_manager and pipeline_id:
            await self.tracking_manager.mark_step_completed(
                pipeline_id,
                current_step_num,
            )

        self._log_step_completion(current_step_num, pipeline_id, steps_data)

    async def _dispatch_step_complete_hook(
        self,
        current_step_num: int,
        pipeline_id: str,
        steps_data: list[DumpedStep],
        result: "TaskiqResult[Any]",
    ) -> None:
        """Dispatch step complete hook."""
        current_step_data = steps_data[current_step_num]
        parsed_step = parse_step(
            current_step_data.step_type,
            current_step_data.step_data,
        )
        await self.hook_manager.dispatch(
            StepCompleteEvent(
                pipeline_id=pipeline_id,
                step_index=current_step_num,
                task_name=cast(str, getattr(parsed_step, "task_name", "unknown")),
                task_id=current_step_data.task_id,
                result=result.return_value,
            ),
        )

    def _log_step_completion(
        self,
        current_step_num: int,
        pipeline_id: str | None,
        steps_data: list[DumpedStep],
    ) -> None:
        """Log step completion."""
        current_step_data = steps_data[current_step_num]
        parsed_step = parse_step(
            current_step_data.step_type,
            current_step_data.step_data,
        )
        task_name = cast(str, getattr(parsed_step, "task_name", "unknown"))
        logger.info(
            f"[{pipeline_id or 'unknown'}][STEP {current_step_num}] DONE",
            extra={
                "pipeline_id": pipeline_id,
                "step": current_step_num,
                "task": task_name,
                "event": "step_complete",
            },
        )

    async def _handle_step_error(
        self,
        current_step_num: int,
        pipeline_id: str | None,
        steps_data: list[DumpedStep],
        error: BaseException,
        total_steps: int,
    ) -> None:
        """Handle step error."""
        logger.warning("Pipeline is aborted. Reason: %s", error, exc_info=True)

        if self.tracking_manager and pipeline_id:
            await self.tracking_manager.mark_step_failed(
                pipeline_id,
                current_step_num,
                str(error),
            )

        if self.hook_manager and pipeline_id:
            await self._dispatch_step_error_hook(
                current_step_num,
                pipeline_id,
                steps_data,
                error,
            )

    async def _dispatch_step_error_hook(
        self,
        current_step_num: int,
        pipeline_id: str,
        steps_data: list[DumpedStep],
        error: BaseException,
    ) -> None:
        """Dispatch step error hook."""
        current_step_data = steps_data[current_step_num]
        parsed_step = parse_step(
            current_step_data.step_type,
            current_step_data.step_data,
        )
        await self.hook_manager.dispatch(
            StepErrorEvent(
                pipeline_id=pipeline_id,
                step_index=current_step_num,
                task_name=cast(str, getattr(parsed_step, "task_name", "unknown")),
                task_id=current_step_data.task_id,
                error=str(error),
            ),
        )

    async def on_error(
        self,
        message: "TaskiqMessage",
        result: "TaskiqResult[Any]",
        exception: BaseException,
    ) -> None:
        """
        Handles on_error event.

        :param message: current message.
        :param result: execution result.
        :param exception: found exception.
        """
        pipeline_info = self._extract_pipeline_info(message)
        if pipeline_info[0] is None:
            return

        current_step_num, pipeline_id, steps_data = pipeline_info

        await self._handle_step_error(
            current_step_num,
            pipeline_id,
            steps_data,
            exception,
            len(steps_data),
        )

        if current_step_num == len(steps_data) - 1:
            # Pipeline failed
            if self.tracking_manager and pipeline_id:
                await self.tracking_manager.mark_pipeline_failed(
                    pipeline_id,
                    str(exception),
                )
            if self.hook_manager and pipeline_id:
                await self.hook_manager.dispatch(
                    PipelineErrorEvent(pipeline_id=pipeline_id, error=str(exception)),
                )
            return
        await self.fail_pipeline(steps_data[-1].task_id, result.error)

    async def fail_pipeline(
        self,
        last_task_id: str,
        abort: BaseException | None = None,
    ) -> None:
        """
        This function aborts pipeline.

        This is done by setting error result for
        the last task in the pipeline.

        :param last_task_id: id of the last task.
        :param abort: caught earlier exception or default
        """
        await self.broker.result_backend.set_result(
            last_task_id,
            TaskiqResult(
                is_err=True,
                return_value=None,  # type: ignore
                error=abort or AbortPipeline(reason="Execution aborted."),
                execution_time=0,
                log="Error found while executing pipeline.",
            ),
        )
