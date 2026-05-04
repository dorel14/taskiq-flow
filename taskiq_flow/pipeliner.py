"""Pipeline de base pour l'orchestration de tâches TaskIQ.

Ce module fournit la classe Pipeline de base qui permet de construire
des séquences de tâches avec passage de résultats. C'est le cœur de
l'orchestration de taskiq-flow, gérant l'enchaînement des étapes et
leur exécution via le middleware.

Auteur: SoniqueBay Team
Version: 0.3.2
"""

from __future__ import annotations

import uuid
from collections.abc import Coroutine
from types import CoroutineType
from typing import Any, ClassVar, Generic, Literal, ParamSpec, TypeVar, overload

import pydantic
from taskiq import AsyncBroker, AsyncTaskiqTask
from taskiq.decor import AsyncTaskiqDecoratedTask
from taskiq.kicker import AsyncKicker

from taskiq_flow.constants import (
    CURRENT_STEP,
    EMPTY_PARAM_NAME,
    PIPELINE_DATA,
    PIPELINE_ID,
    STEP_RETRIES,
    STEP_TIMEOUT,
)
from taskiq_flow.hooks.events import PipelineStartEvent
from taskiq_flow.steps import (
    FilterStep,
    GroupStep,
    MapperStep,
    SequentialStep,
    parse_step,
)

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
        skip_failed: bool = False,
    ) -> None:
        self.default_retries = retries
        self.default_timeout = timeout
        self.fail_fast = fail_fast
        self.continue_on_error = continue_on_error
        self.skip_failed = skip_failed

    def update(
        self,
        retries: int | None = None,
        timeout: int | None = None,
        fail_fast: bool | None = None,
        continue_on_error: bool | None = None,
        skip_failed: bool | None = None,
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
        if skip_failed is not None:
            self.skip_failed = skip_failed


class Pipeline(Generic[_FuncParams, _ReturnType]):
    """
    Constructeur de pipelines séquentiels.

    Cette classe permet de construire un pipeline de tâches en chaîne,
    où chaque tâche reçoit le résultat de la précédente. C'est
    l'abstraction de base de taskiq-flow pour l'orchestration de workflows.

    Un pipeline est construit par méthode fluent (chaînage) puis
    exécuté via la méthode kiq() qui renvoie une AsyncTaskiqTask.

    Caractéristiques:
    - Exécution séquentielle par défaut
    - Support du passage de résultats entre tâches
    - Intégration avec le middleware pour orchestration avancée
    - Possibilité d'ajouter du tracking et des hooks
    - Persistance dans un registre global pour exécution distribuée

    Exemple:
        pipeline = (
            Pipeline(broker)
            .call_next(task1)
            .call_next(task2)
            .call_next(task3)
        )
        result = await pipeline.kiq(arg1, arg2)

    Note:
        Les types génériques _FuncParams et _ReturnType représentent
        respectivement les paramètres de la première tâche et le
        type de retour de la dernière tâche.
    """

    # Global registry for pipelines (needed for distributed task execution)
    _pipeline_registry: ClassVar[dict[str, Pipeline[Any, Any]]] = {}

    @classmethod
    def register_pipeline(cls, pipeline: Pipeline[Any, Any]) -> str:
        """Register a pipeline in the global registry.

        Args:
            pipeline: Pipeline to register

        Returns:
            The pipeline ID
        """
        if pipeline.pipeline_id is None:
            pipeline.pipeline_id = str(uuid.uuid4())
        cls._pipeline_registry[pipeline.pipeline_id] = pipeline
        return pipeline.pipeline_id

    @classmethod
    def get_pipeline(cls, pipeline_id: str) -> Pipeline[Any, Any] | None:
        """Get a pipeline from the registry.

        Args:
            pipeline_id: The pipeline ID

        Returns:
            The pipeline or None if not found
        """
        return cls._pipeline_registry.get(pipeline_id)

    @classmethod
    def unregister_pipeline(cls, pipeline_id: str) -> bool:
        """Remove a pipeline from the registry.

        Args:
            pipeline_id: The pipeline ID

        Returns:
            True if removed, False if not found
        """
        if pipeline_id in cls._pipeline_registry:
            del cls._pipeline_registry[pipeline_id]
            return True
        return False

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
        """
        Initialise un nouveau pipeline.

        Args:
            broker: Broker TaskIQ pour l'exécution des tâches
            task: Tâche initiale optionnelle (première étape du pipeline)

        Attributes:
            steps: Liste des étapes sérialisées du pipeline
            pipeline_id: Identifiant unique du pipeline (généré au kiq)
            tracking_enabled: Indique si le tracking est activé
            tracking_manager: Gestionnaire de tracking
            hook_manager: Gestionnaire de hooks
            options: Options d'exécution (retry, timeout, etc.)
        """
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
        manager: Any = None,  # PipelineTrackingManager | None
    ) -> Pipeline[_FuncParams, _ReturnType]:
        """
        Active le suivi d'exécution du pipeline.

        Args:
            enabled: Si True, active le tracking. Si False, désactive.
            manager: Instance de PipelineTrackingManager (optionnel).
                    Si non fourni, un gestionnaire par défaut sera créé
                    lors de l'appel à kiq() si le tracking est activé.

        Returns:
            Self pour chaînage fluent
        """
        self.tracking_enabled = enabled
        self.tracking_manager = manager
        return self

    def with_hooks(
        self,
        manager: Any,
    ) -> Pipeline[_FuncParams, _ReturnType]:  # HookManager
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
        Ajoute une étape séquentielle au pipeline.

        La tâche spécifiée sera exécutée après la précédente et
        recevra son résultat comme premier argument, ou comme
        argument keyword si param_name est spécifié.

        Args:
            task: Tâche décorée TaskIQ à exécuter
            param_name: Nom du paramètre dans la signature de la tâche
                       où passer le résultat précédent.
                       - None: passe comme premier argument positionnel
                       - "nom": passe comme argument keyword "nom"
                       - -1 (EMPTY_PARAM_NAME): ne passe pas le résultat
            **additional_kwargs: Arguments additionnels fixes à passer
                               à la tâche

        Returns:
            Self pour chaînage fluent

        Example:
            pipeline = Pipeline(broker).call_next(task1).call_next(task2)

            # Passer le résultat comme premier argument (défaut)
            pipeline.call_next(process_result)

            # Passer le résultat comme argument nommé
            pipeline.call_next(process_data, param_name="input_data")

            # Ne pas passer le résultat (séquentiel sans dépendance)
            pipeline.call_next(finalize, param_name=-1)
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
        Ajoute une étape de map (application en parallèle).

        Cette étape prend la liste résultant de l'étape précédente et
        exécute la tâche spécifiée sur chaque élément en parallèle.
        Les résultats sont collectés dans une liste dans le même ordre.

        Args:
            task: Tâche à appliquer à chaque élément
            param_name: Nom du paramètre recevant chaque élément.
                       Si None, l'élément est passé comme premier argument.
            skip_errors: Si True, les erreurs sont ignorées et l'élément
                        correspondant est omis du résultat. Si False,
                        la première erreur propagée annule le pipeline.
            check_interval: Intervalle en secondes entre chaque vérification
                           de complétion des sous-tâches (défaut: 0.5s)
            **additional_kwargs: Arguments additionnels à passer à chaque
                                invocation de la tâche

        Returns:
            Self pour chaînage fluent

        Example:
            # Appliquer process_item à chaque élément d'une liste
            pipeline.map(process_item, param_name="item", max_parallel=10)
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
        Ajoute une étape de filtre.

        Similaire à map mais ne conserve que les éléments pour
        lesquels la tâche renvoie une valeur truthy. Exécute la
        tâche sur chaque élément en parallèle puis filtre les résultats.

        Args:
            task: Tâche prédicat (doit renvoyer booléen)
            param_name: Nom du paramètre recevant chaque élément
            skip_errors: Si True, ignore les erreurs de tâche
            check_interval: Intervalle de vérification des complétions
            **additional_kwargs: Arguments additionnels pour la tâche

        Returns:
            Self pour chaînage fluent

        Example:
            # Ne garder que les éléments pairs
            pipeline.filter(is_even, param_name="n")
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
        Lance l'exécution du pipeline de manière asynchrone.

        Cette méthode sérialise le pipeline, génère un identifiant unique,
        et délègue la première tâche au broker TaskIQ avec les labels
        nécessaires à l'orchestration par le middleware.

        Args:
            *args: Arguments positionnels pour la première tâche du pipeline
            **kwargs: Arguments keyword pour la première tâche

        Returns:
            AsyncTaskiqTask représentant la dernière tâche du pipeline.
            Peut être utilisé pour attendre le résultat via wait_result()
            ou pour annuler l'exécution.

        Raises:
            ValueError: Si le pipeline est vide ou si la première étape
                      n'est pas une étape séquentielle

        Example:
            result_task = await pipeline.kiq(input_data)
            final_result = await result_task.wait_result()

        Note:
            Le pipeline_id est généré automatiquement si le tracking
            est activé. Les labels_taskiq contiennent les données
            sérialisées du pipeline pour reconstruction par le middleware.
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
            await self.hook_manager.dispatch(
                PipelineStartEvent(pipeline_id=self.pipeline_id or ""),
            )

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

    def group(
        self,
        tasks: list[AsyncKicker[Any, Any] | AsyncTaskiqDecoratedTask[Any, Any]],
        param_names: list[str | None] | None = None,
        **additional_kwargs: Any,
    ) -> Any:
        """
        Ajoute une étape de groupe (exécution parallèle de tâches indépendantes).

        Exécute toutes les tâches spécifiées en parallèle et collecte
        leurs résultats dans une liste. Les tâches du groupe n'ont pas
        de dépendance entre elles.

        Args:
            tasks: Liste des tâches à exécuter en parallèle
            param_names: Liste optionnelle des noms de paramètres pour
                        chaque tâche. Si fourni, doit avoir la même
                        longueur que tasks. Détermine comment le
                        résultat précédent est injecté dans chaque tâche.
            **additional_kwargs: Arguments additionnels à passer à chaque
                                tâche (peut inclure des values par task_name)

        Returns:
            Self pour chaînage fluent

        Example:
            # Exécuter trois tâches indépendantes en parallèle
            pipeline.group(
                [task_a, task_b, task_c],
                param_names=[None, "data", None]
            )
        """
        self.steps.append(
            DumpedStep(
                step_type=GroupStep._step_name,
                step_data=GroupStep.from_tasks(
                    tasks=tasks,
                    param_names=param_names,
                    **additional_kwargs,
                ).model_dump(),
                task_id="",
            ),
        )
        return self

