"""
Middleware Pipeline pour l'orchestration de workflows.

Ce module contient le PipelineMiddleware qui intercepte l'exécution
des tâches TaskIQ pour gérer le flux d'exécution des pipelines.
Il décide quelle étape exécuter ensuite, gère le suivi (tracking)
et les hooks d'événements.

Auteur: SoniqueBay Team
Version: 1.0.2
"""

import logging
from typing import Any, cast

import pydantic
from taskiq import TaskiqMessage, TaskiqMiddleware, TaskiqResult

from taskiq_flow.constants import CURRENT_STEP, PIPELINE_DATA, PIPELINE_ID
from taskiq_flow.exceptions import AbortPipeline
from taskiq_flow.hooks.events import (
    PipelineCompleteEvent,
    PipelineErrorEvent,
    PipelineEvent,
    StepCompleteEvent,
    StepErrorEvent,
    StepStartEvent,
)
from taskiq_flow.integration.websocket.fastapi_ws import (
    get_fastapi_ws_manager,
)
from taskiq_flow.integration.websocket.server import get_websocket_server
from taskiq_flow.metrics.collector import MetricsCollector
from taskiq_flow.pipeliner import DumpedStep
from taskiq_flow.steps import parse_step

try:
    from taskiq_flow.hooks.manager import WEBSOCKET_AVAILABLE
except ImportError:
    WEBSOCKET_AVAILABLE = False

logger = logging.getLogger(__name__)


class PipelineMiddleware(TaskiqMiddleware):
    """
    Middleware TaskIQ pour l'orchestration de pipelines.

    Detecte si la tâche fait partie d'un pipeline (labels),
    determine l'etape suivante, gere le tracking et les hooks.

    Attributes:
        tracking_manager: Gestionnaire de suivi (optionnel)
        hook_manager: Gestionnaire d'evenements (optionnel)
        metrics_collector: Collecteur de metriques (optionnel)

    """

    def __init__(
        self,
        tracking_manager: Any = None,
        hook_manager: Any = None,
        metrics_collector: MetricsCollector | None = None,
    ) -> None:
        self.tracking_manager = tracking_manager
        self.hook_manager = hook_manager
        self.metrics_collector = metrics_collector
        super().__init__()

    async def post_save(
        self,
        message: "TaskiqMessage",
        result: "TaskiqResult[Any]",
    ) -> None:
        """Handler principal appele apres execution d'une tache."""
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

        if self.metrics_collector and pipeline_id:
            step_data = steps_data[current_step_num]
            parsed_step = parse_step(step_data.step_type, step_data.step_data)
            self.metrics_collector.step_started(
                pipeline_id,
                cast(str, getattr(parsed_step, "task_name", "unknown")),
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

        if self.metrics_collector and pipeline_id:
            self.metrics_collector.pipeline_complete(
                pipeline_id,
                success=True,
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

        if self.metrics_collector and pipeline_id:
            step_data = steps_data[current_step_num]
            parsed_step = parse_step(step_data.step_type, step_data.step_data)
            task_name = cast(str, getattr(parsed_step, "task_name", "unknown"))
            self.metrics_collector.task_executed(
                task_name,
                "success",
                0.0,
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

        if self.metrics_collector and pipeline_id:
            step_data = steps_data[current_step_num]
            parsed_step = parse_step(step_data.step_type, step_data.step_data)
            task_name = cast(str, getattr(parsed_step, "task_name", "unknown"))
            self.metrics_collector.task_executed(
                task_name,
                "failure",
                0.0,
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
                attempt=1,
                max_attempts=1,
            ),
        )

    async def on_error(
        self,
        message: "TaskiqMessage",
        result: "TaskiqResult[Any]",
        exception: BaseException,
    ) -> None:
        """Handle on_error event."""
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
        """Abort pipeline by setting error result for the last task."""
        await self.broker.result_backend.set_result(
            last_task_id,
            TaskiqResult(
                is_err=True,
                return_value=cast(Any, None),
                error=abort or AbortPipeline(reason="Execution aborted."),
                execution_time=0,
                log="Error found while executing pipeline.",
            ),
        )


class TransportMiddleware:
    """
    Pluggable transport middleware for event broadcasting.

    Supports multiple transport types:
    - websocket: WebSocket broadcast
    - http_stream: HTTP SSE streaming
    - redis_pubsub: Redis pub/sub
    """

    def __init__(self, transport_type: str = "websocket", **kwargs: Any) -> None:
        """
        Initialize transport middleware.

        Args:
            transport_type: Type of transport (websocket, http_stream, redis_pubsub)
            **kwargs: Transport-specific configuration

        """
        self.transport_type = transport_type
        self.config = kwargs
        self._fastapi_manager: Any = None
        self._transport = self._create_transport()

    def _create_transport(self) -> Any:
        """Create transport instance based on type."""
        if self.transport_type == "websocket":
            return self._create_websocket_transport()
        if self.transport_type == "http_stream":
            return self._create_http_stream_transport()
        if self.transport_type == "redis_pubsub":
            return self._create_redis_pubsub_transport()
        raise ValueError(f"Unsupported transport type: {self.transport_type}")

    def _create_websocket_transport(self) -> Any:
        """Create WebSocket transport."""
        if self._try_fastapi_websocket():
            return self._fastapi_manager

        if not WEBSOCKET_AVAILABLE:
            logger.warning("picows not available, WebSocket transport disabled")
            return None
        return get_websocket_server(
            host=self.config.get("host", "127.0.0.1"),
            port=self.config.get("port", 8765),
        )

    def _try_fastapi_websocket(self) -> bool:
        """Try to create FastAPI WebSocket transport."""
        try:
            manager = get_fastapi_ws_manager()
            self._fastapi_manager = manager
            return True
        except ImportError:
            return False

    def _create_http_stream_transport(self) -> Any:
        """Create HTTP stream (SSE) transport."""
        from taskiq_flow.transport.http_stream import get_http_stream_transport  # noqa: I001, PLC0415

        transport = get_http_stream_transport()
        logger.info("HTTP stream transport configured")
        return transport

    def _create_redis_pubsub_transport(self) -> Any:
        """Create Redis pub/sub transport."""
        try:
            from taskiq_flow.transport.redis_pubsub import RedisPubSubTransport  # noqa: I001, PLC0415

            transport = RedisPubSubTransport(
                redis_client=self.config.get("redis_client"),
                channel_prefix=self.config.get("channel_prefix", "pipeline_events"),
            )
            logger.info("Redis pub/sub transport configured")
            return transport
        except ImportError:
            logger.warning("Redis client not available, Redis pub/sub disabled")
            return None

    async def broadcast(self, event: PipelineEvent) -> None:
        """
        Broadcast event through transport.

        Args:
            event: Pipeline event to broadcast.

        """
        if self._transport is None:
            logger.debug("No transport configured, skipping broadcast")
            return

        try:
            if self.transport_type == "websocket" and hasattr(
                self._transport, "broadcast_event"
            ):
                await self._transport.broadcast_event(
                    event.pipeline_id,
                    event.model_dump(),
                )
            elif self.transport_type in {"http_stream", "redis_pubsub"}:
                await self._transport.broadcast(event)
        except Exception as e:
            logger.error(f"Failed to broadcast event via {self.transport_type}: {e}")


__all__ = [
    "PipelineMiddleware",
    "TransportMiddleware",
]
