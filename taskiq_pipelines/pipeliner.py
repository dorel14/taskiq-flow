from __future__ import annotations

import uuid
from collections.abc import Coroutine
from types import CoroutineType
from typing import Any, Generic, Literal, ParamSpec, TypeVar, overload

import pydantic
from taskiq import AsyncBroker, AsyncTaskiqTask
from taskiq.decor import AsyncTaskiqDecoratedTask
from taskiq.kicker import AsyncKicker

from taskiq_pipelines.constants import (
    CURRENT_STEP,
    EMPTY_PARAM_NAME,
    PIPELINE_DATA,
    PIPELINE_ID,
    STEP_RETRIES,
    STEP_TIMEOUT,
)
from taskiq_pipelines.steps import FilterStep, MapperStep, SequentialStep, parse_step

_ReturnType = TypeVar("_ReturnType")
_FuncParams = ParamSpec("_FuncParams")
_T = TypeVar("_T")
_T2 = TypeVar("_T2")


class DumpedStep(pydantic.BaseModel):
    """Dumped state model."""

    step_type: str
    step_data: dict[str, Any]
    task_id: str


DumpedSteps = pydantic.RootModel[list[DumpedStep]]


class PipelineOptions:
    """Options for pipeline execution."""

    def __init__(
        self,
        retries: int | None = None,
        timeout: int | None = None,
        fail_fast: bool = True,
        continue_on_error: bool = False,
    ) -> None:
        self.default_retries = retries
        self.default_timeout = timeout
        self.fail_fast = fail_fast
        self.continue_on_error = continue_on_error

    def update(
        self,
        retries: int | None = None,
        timeout: int | None = None,
        fail_fast: bool | None = None,
        continue_on_error: bool | None = None,
    ) -> None:
        """Update options."""
        if retries is not None:
            self.default_retries = retries
        if timeout is not None:
            self.default_timeout = timeout
        if fail_fast is not None:
            self.fail_fast = fail_fast
        if continue_on_error is not None:
            self.continue_on_error = continue_on_error


class Pipeline(Generic[_FuncParams, _ReturnType]):
    """
    Pipeline constructor.

    This class helps you to build pipelines.
    It creates all needed data and manages
    task ids. Also it has helper methods,
    to easily add new pipeline steps.

    Of course it can be done manually,
    but it's nice to have.
    """

    @overload
    def __init__(
        self,
        broker: AsyncBroker,
        task: (
            AsyncKicker[_FuncParams, Coroutine[Any, Any, _ReturnType]]
            | AsyncKicker[_FuncParams, CoroutineType[Any, Any, _ReturnType]]
            | AsyncTaskiqDecoratedTask[_FuncParams, Coroutine[Any, Any, _ReturnType]]
            | AsyncTaskiqDecoratedTask[
                _FuncParams,
                CoroutineType[Any, Any, _ReturnType],
            ]
            | None
        ) = None,
    ) -> None: ...

    @overload
    def __init__(
        self,
        broker: AsyncBroker,
        task: (
            AsyncKicker[_FuncParams, _ReturnType]
            | AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType]
            | None
        ) = None,
    ) -> None: ...

    def __init__(
        self,
        broker: AsyncBroker,
        task: AsyncKicker[Any, Any] | AsyncTaskiqDecoratedTask[Any, Any] | None = None,
    ) -> None:
        self.broker = broker
        self.steps: list[DumpedStep] = []
        self.pipeline_id: str | None = None
        self.tracking_enabled: bool = False
        self.tracking_manager: Any = None  # PipelineTrackingManager | None
        self.hook_manager: Any = None  # HookManager | None
        self.options: PipelineOptions = PipelineOptions()
        if task:
            self.call_next(task)

    def with_tracking(
        self,
        enabled: bool = True,
        manager: Any = None,  # PipelineTrackingManager
    ) -> Pipeline[_FuncParams, _ReturnType]:
        """Enable pipeline tracking."""
        self.tracking_enabled = enabled
        self.tracking_manager = manager
        return self

    def with_hooks(self, manager: Any) -> Pipeline[_FuncParams,
                                                _ReturnType]:  # HookManager
        """Set hook manager for events."""
        self.hook_manager = manager
        return self

    def with_options(
        self,
        retries: int | None = None,
        timeout: int | None = None,
        fail_fast: bool | None = None,
        continue_on_error: bool | None = None,
    ) -> Pipeline[_FuncParams, _ReturnType]:
        """Set pipeline execution options."""
        self.options.update(retries, timeout, fail_fast, continue_on_error)
        return self

    @overload
    def call_next(
        self: Pipeline[_FuncParams, _ReturnType],
        task: (
            AsyncKicker[[_ReturnType], Coroutine[Any, Any, _T]]
            | AsyncKicker[[_ReturnType], CoroutineType[Any, Any, _T]]
            | AsyncTaskiqDecoratedTask[[_ReturnType], Coroutine[Any, Any, _T]]
            | AsyncTaskiqDecoratedTask[[_ReturnType], CoroutineType[Any, Any, _T]]
        ),
        param_name: str | Literal[-1] | None = None,
        **additional_kwargs: Any,
    ) -> Pipeline[_FuncParams, _T]: ...

    @overload
    def call_next(
        self: Pipeline[_FuncParams, _ReturnType],
        task: (
            AsyncKicker[[_ReturnType], _T] | AsyncTaskiqDecoratedTask[[_ReturnType], _T]
        ),
        param_name: str | Literal[-1] | None = None,
        **additional_kwargs: Any,
    ) -> Pipeline[_FuncParams, _T]: ...

    def call_next(
        self,
        task: AsyncKicker[Any, Any] | AsyncTaskiqDecoratedTask[Any, Any],
        param_name: str | None | Literal[-1] = None,
        **additional_kwargs: Any,
    ) -> Any:
        """
        Adds sequential step.

        This task will be executed right after
        the previous and result of the previous task
        will be passed as the first argument,
        or it will be passed as key word argument,
        if param_name is specified.

        :param task: task to execute.
        :param param_name: kwarg param name, defaults to None.
            If set to -1 (EMPTY_PARAM_NAME), result is not passed.
        :param additional_kwargs: additional kwargs to task.
        :return: updated pipeline.
        """
        self.steps.append(
            DumpedStep(
                step_type=SequentialStep._step_name,
                step_data=SequentialStep.from_task(
                    task=task,
                    param_name=param_name,
                    **additional_kwargs,
                ).model_dump(),
                task_id="",
            ),
        )
        return self

    @overload
    def call_after(
        self: Pipeline[_FuncParams, _ReturnType],
        task: (
            AsyncKicker[[], Coroutine[Any, Any, _T]]
            | AsyncKicker[[], CoroutineType[Any, Any, _T]]
            | AsyncTaskiqDecoratedTask[[], Coroutine[Any, Any, _T]]
            | AsyncTaskiqDecoratedTask[[], CoroutineType[Any, Any, _T]]
        ),
        **additional_kwargs: Any,
    ) -> Pipeline[_FuncParams, _T]: ...

    @overload
    def call_after(
        self: Pipeline[_FuncParams, _ReturnType],
        task: AsyncKicker[[], _T] | AsyncTaskiqDecoratedTask[[], _T],
        **additional_kwargs: Any,
    ) -> Pipeline[_FuncParams, _T]: ...

    def call_after(
        self,
        task: AsyncKicker[Any, Any] | AsyncTaskiqDecoratedTask[Any, Any],
        **additional_kwargs: Any,
    ) -> Any:
        """
        Adds sequential step.

        This task will be executed right after
        the previous and result of the previous task
        is not passed to the next task.

        This is equivalent to call_next(task, param_name=-1).

        :param task: task to execute.
        :param additional_kwargs: additional kwargs to task.
        :return: updated pipeline.
        """
        self.steps.append(
            DumpedStep(
                step_type=SequentialStep._step_name,
                step_data=SequentialStep.from_task(
                    task=task,
                    param_name=EMPTY_PARAM_NAME,
                    **additional_kwargs,
                ).model_dump(),
                task_id="",
            ),
        )
        return self

    @overload
    def map(
        self: Pipeline[_FuncParams, list[_T]],
        task: (
            AsyncKicker[Any, Coroutine[Any, Any, _T2]]
            | AsyncKicker[Any, CoroutineType[Any, Any, _T2]]
            | AsyncTaskiqDecoratedTask[Any, Coroutine[Any, Any, _T2]]
            | AsyncTaskiqDecoratedTask[Any, CoroutineType[Any, Any, _T2]]
        ),
        param_name: str | None = None,
        skip_errors: bool = False,
        check_interval: float = 0.5,
        **additional_kwargs: Any,
    ) -> Pipeline[_FuncParams, list[_T2]]: ...

    @overload
    def map(
        self: Pipeline[_FuncParams, list[_T]],
        task: AsyncKicker[Any, _T2] | AsyncTaskiqDecoratedTask[Any, _T2],
        param_name: str | None = None,
        skip_errors: bool = False,
        check_interval: float = 0.5,
        **additional_kwargs: Any,
    ) -> Pipeline[_FuncParams, list[_T2]]: ...

    def map(
        self: Pipeline[_FuncParams, list[Any]],
        task: AsyncKicker[Any, Any] | AsyncTaskiqDecoratedTask[Any, Any],
        param_name: str | None = None,
        skip_errors: bool = False,
        check_interval: float = 0.5,
        **additional_kwargs: Any,
    ) -> Any:
        """
        Create new map task.

        This task is used to map values of an
        iterable.

        It creates many subtasks and then collects
        all results.

        :param task: task to execute on each value of an iterable.
        :param param_name: param name to use to inject the result of
            the previous task. If none, result injected as the first argument.
        :param skip_errors: skip error results, defaults to False.
        :param check_interval: how often task completion is checked.
        :param additional_kwargs: additional function's kwargs.
        :return: pipeline.
        """
        self.steps.append(
            DumpedStep(
                step_type=MapperStep._step_name,
                step_data=MapperStep.from_task(
                    task=task,
                    param_name=param_name,
                    skip_errors=skip_errors,
                    check_interval=check_interval,
                    **additional_kwargs,
                ).model_dump(),
                task_id="",
            ),
        )
        return self

    @overload
    def filter(
        self: Pipeline[_FuncParams, list[_T]],
        task: (
            AsyncKicker[[_T], Coroutine[Any, Any, bool]]
            | AsyncKicker[[_T], CoroutineType[Any, Any, bool]]
            | AsyncTaskiqDecoratedTask[[_T], Coroutine[Any, Any, bool]]
            | AsyncTaskiqDecoratedTask[[_T], CoroutineType[Any, Any, bool]]
        ),
        param_name: str | None = None,
        skip_errors: bool = False,
        check_interval: float = 0.5,
        **additional_kwargs: Any,
    ) -> Pipeline[_FuncParams, list[_T]]: ...

    @overload
    def filter(
        self: Pipeline[_FuncParams, list[_T]],
        task: AsyncKicker[[_T], bool] | AsyncTaskiqDecoratedTask[[_T], bool],
        param_name: str | None = None,
        skip_errors: bool = False,
        check_interval: float = 0.5,
        **additional_kwargs: Any,
    ) -> Pipeline[_FuncParams, list[_T]]: ...

    def filter(
        self,
        task: AsyncKicker[Any, Any] | AsyncTaskiqDecoratedTask[Any, Any],
        param_name: str | None = None,
        skip_errors: bool = False,
        check_interval: float = 0.5,
        **additional_kwargs: Any,
    ) -> Any:
        """
        Add filter step.

        This step is executed on a list of items,
        like map.

        It runs many small subtasks for each item
        in sequence and if task returns true,
        the result is added to the final list.

        :param task: task to execute on every item.
        :param param_name: parameter name to pass item into, defaults to None
        :param skip_errors: skip errors if any, defaults to False
        :param check_interval: how often the result of all subtasks is checked,
             defaults to 0.5
        :param additional_kwargs: additional function's kwargs.
        :return: pipeline with filtering step.
        """
        self.steps.append(
            DumpedStep(
                step_type=FilterStep._step_name,
                step_data=FilterStep.from_task(
                    task=task,
                    param_name=param_name,
                    skip_errors=skip_errors,
                    check_interval=check_interval,
                    **additional_kwargs,
                ).model_dump(),
                task_id="",
            ),
        )
        return self

    def dumpb(self) -> bytes:
        """
        Dumps current pipeline as string.

        :returns: serialized pipeline.
        """
        return self.broker.serializer.dumpb(
            DumpedSteps.model_validate(self.steps).model_dump(),
        )

    @classmethod
    def loadb(cls, broker: AsyncBroker, pipe_data: bytes) -> Pipeline[Any, Any]:
        """
        Parses serialized pipeline.

        This method requires broker,
        to make pipeline kickable.

        :param broker: broker to use when call kiq.
        :param pipe_data: serialized pipeline data.
        :return: new
        """
        pipe: Pipeline[Any, Any] = Pipeline(broker)
        data = broker.serializer.loadb(pipe_data)
        pipe.steps = DumpedSteps.model_validate(data)  # type: ignore[assignment]
        return pipe

    async def kiq(
        self,
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> AsyncTaskiqTask[_ReturnType]:
        """
        Kiq pipeline.

        This function is used as kiq in functions,
        but it saves current pipeline as
        custom label, so worker can understand,
        what to do next.

        :param args: first function's args.
        :param kwargs: first function's kwargs.

        :raises ValueError: if pipe is empty, or
            first step isn't sequential.

        :return: TaskqTask for the final function.
        """
        if not self.steps:
            raise ValueError("Pipeline is empty.")

        # Generate pipeline ID and init tracking
        if self.tracking_enabled:
            self.pipeline_id = str(uuid.uuid4())
            if self.tracking_manager:
                await self.tracking_manager.initiate(self.pipeline_id, len(self.steps))

        # Dispatch pipeline start event
        if self.hook_manager:
            from taskiq_pipelines.hooks.events import PipelineStartEvent
            await self.hook_manager.dispatch(PipelineStartEvent(
                    pipeline_id=self.pipeline_id or ""))

        self._update_task_ids()
        step = self.steps[0]
        parsed_step = parse_step(step.step_type, step.step_data)
        if not isinstance(parsed_step, SequentialStep):
            raise ValueError("First step must be sequential.")

        # Prepare labels
        labels = {CURRENT_STEP: 0, PIPELINE_DATA: self.dumpb()}
        if self.pipeline_id:
            labels[PIPELINE_ID] = self.pipeline_id
        if self.options.default_retries is not None:
            labels[STEP_RETRIES] = str(self.options.default_retries)
        if self.options.default_timeout is not None:
            labels[STEP_TIMEOUT] = str(self.options.default_timeout)

        kicker = (
            AsyncKicker(
                parsed_step.task_name,
                broker=self.broker,
                labels=parsed_step.labels,
            )
            .with_task_id(step.task_id)
            .with_labels(**labels)  # type: ignore
        )
        taskiq_task = await kicker.kiq(*args, **kwargs)
        taskiq_task.task_id = self.steps[-1].task_id
        return taskiq_task

    def _update_task_ids(self) -> None:
        """Calculates task ids for each step in the pipeline."""
        for step in self.steps:
            step.task_id = self.broker.id_generator()
