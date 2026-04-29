from logging import getLogger
from typing import Any

import pydantic
from taskiq import TaskiqMessage, TaskiqMiddleware, TaskiqResult

from taskiq_pipelines.constants import CURRENT_STEP, PIPELINE_DATA, PIPELINE_ID
from taskiq_pipelines.exceptions import AbortPipeline
from taskiq_pipelines.hooks.events import (
    PipelineCompleteEvent,
    PipelineErrorEvent,
    StepCompleteEvent,
    StepErrorEvent,
    StepStartEvent,
)
from taskiq_pipelines.pipeliner import DumpedStep
from taskiq_pipelines.steps import parse_step

logger = getLogger(__name__)


class PipelineMiddleware(TaskiqMiddleware):
    """Pipeline middleware."""

    def __init__(
        self,
        tracking_manager: Any = None,  # PipelineTrackingManager | None
        hook_manager: Any = None,  # HookManager | None
    ) -> None:
        self.tracking_manager = tracking_manager
        self.hook_manager = hook_manager
        super().__init__()

    async def post_save(  # noqa: PLR0911
        self,
        message: "TaskiqMessage",
        result: "TaskiqResult[Any]",
    ) -> None:
        """
        Handle post-execute event.

        This is the heart of pipelines.
        Here we decide what to do next.

        If the message have pipeline
        labels we can calculate our next step.

        :param message: current message.
        :param result: result of the execution.
        """
        if result.is_err:
            return
        if CURRENT_STEP not in message.labels:
            return
        current_step_num = int(message.labels[CURRENT_STEP])
        pipeline_id = message.labels.get(PIPELINE_ID)
        if PIPELINE_DATA not in message.labels:
            logger.warning("Pipeline data not found. Execution flow is broken.")
            return
        pipeline_data = message.labels[PIPELINE_DATA]
        parsed_data = self.broker.serializer.loadb(pipeline_data)
        try:
            steps_data = pydantic.TypeAdapter(list[DumpedStep]).validate_python(
                parsed_data,
            )
        except ValueError as err:
            logger.warning("Cannot parse pipeline_data: %s", err, exc_info=True)
            return


        if self.hook_manager and pipeline_id:

            # Parse the step to get task_name
            current_step_data = steps_data[current_step_num]
            parsed_step = parse_step(current_step_data.step_type,
                                    current_step_data.step_data)
            await self.hook_manager.dispatch(StepStartEvent(
                pipeline_id=pipeline_id,
                step_index=current_step_num,
                task_name=parsed_step.task_name,
                task_id=current_step_data.task_id,
            ))

            # Tracking: mark step start
        if self.tracking_manager and pipeline_id:
            current_step_data = steps_data[current_step_num]
            parsed_step = parse_step(current_step_data.step_type,
                                     current_step_data.step_data)
            await self.tracking_manager.mark_step_started(
                pipeline_id, current_step_num,
                current_step_data.task_id,
                parsed_step.task_name,
            )

        # Logging
        current_step_data = steps_data[current_step_num]
        parsed_step = parse_step(current_step_data.step_type,
                                current_step_data.step_data)
        logger.info(
            f"[{pipeline_id or 'unknown'}][STEP {current_step_num}] START {parsed_step.task_name}",
            extra={"pipeline_id": pipeline_id, "step": current_step_num, 
                    "task": parsed_step.task_name,
                    "event": "step_start"},
        )

        if current_step_num + 1 >= len(steps_data):
            logger.debug("Pipeline is completed.")
            # Mark pipeline completed
            if self.tracking_manager and pipeline_id:
                await self.tracking_manager.mark_pipeline_completed(pipeline_id,
                    result.return_value)
            if self.hook_manager and pipeline_id:

                await self.hook_manager.dispatch(PipelineCompleteEvent(
                    pipeline_id=pipeline_id,
                    result=result.return_value))
            return

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
                pipe_data=pipeline_data,
                result=result,
            )

            # Hook: step_complete
            if self.hook_manager and pipeline_id:

                current_step_data = steps_data[current_step_num]
                parsed_step = parse_step(current_step_data.step_type,
                                        current_step_data.step_data)
                await self.hook_manager.dispatch(StepCompleteEvent(
                    pipeline_id=pipeline_id,
                    step_index=current_step_num,
                    task_name=parsed_step.task_name,
                    task_id=current_step_data.task_id,
                    result=result.return_value,
                ))

            # Tracking: complete
            if self.tracking_manager and pipeline_id:
                await self.tracking_manager.mark_step_completed(pipeline_id,
                                                                current_step_num)

            logger.info(f"[{pipeline_id or 'unknown'}][STEP {current_step_num}] DONE", 
                            extra={"pipeline_id": pipeline_id,
                                    "step": current_step_num,
                                    "task": parsed_step.task_name,
                                    "event": "step_complete"})

        except AbortPipeline as abort_exc:
            logger.warning(
                "Pipeline is aborted. Reason: %s",
                abort_exc,
                exc_info=True,
            )
            # Tracking: fail
            if self.tracking_manager and pipeline_id:
                await self.tracking_manager.mark_step_failed(pipeline_id,
                                                                current_step_num,
                                                                str(abort_exc))
            # Hook: error
            if self.hook_manager and pipeline_id:

                current_step_data = steps_data[current_step_num]
                parsed_step = parse_step(current_step_data.step_type,
                    current_step_data.step_data)
                await self.hook_manager.dispatch(StepErrorEvent(
                    pipeline_id=pipeline_id,
                    step_index=current_step_num,
                    task_name=parsed_step.task_name,
                    task_id=current_step_data.task_id,
                    error=str(abort_exc),
                ))
            if current_step_num == len(steps_data) - 1:
                return
            await self.fail_pipeline(steps_data[-1].task_id)

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
        if CURRENT_STEP not in message.labels:
            return
        current_step_num = int(message.labels[CURRENT_STEP])
        pipeline_id = message.labels.get(PIPELINE_ID)
        if PIPELINE_DATA not in message.labels:
            logger.warning("Pipeline data not found. Execution flow is broken.")
            return
        pipe_data = message.labels[PIPELINE_DATA]
        try:
            steps = pydantic.TypeAdapter(list[DumpedStep]).validate_json(pipe_data)
        except ValueError:
            return

        # Tracking: fail step
        if self.tracking_manager and pipeline_id:
            await self.tracking_manager.mark_step_failed(pipeline_id, current_step_num,
                                                        str(exception))

        # Hook: step error
        if self.hook_manager and pipeline_id:
            from taskiq_pipelines.hooks.events import StepErrorEvent
            current_step_data = steps[current_step_num]
            parsed_step = parse_step(current_step_data.step_type,
                                    current_step_data.step_data)
            await self.hook_manager.dispatch(StepErrorEvent(
                pipeline_id=pipeline_id,
                step_index=current_step_num,
                task_name=parsed_step.task_name,
                task_id=current_step_data.task_id,
                error=str(exception),
            ))

        if current_step_num == len(steps) - 1:
            # Pipeline failed
            if self.tracking_manager and pipeline_id:
                await self.tracking_manager.mark_pipeline_failed(pipeline_id,
                                                                str(exception))
            if self.hook_manager and pipeline_id:

                await self.hook_manager.dispatch(PipelineErrorEvent(
                    pipeline_id=pipeline_id,
                    error=str(exception)
                    ))
            return
        await self.fail_pipeline(steps[-1].task_id, result.error)

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
