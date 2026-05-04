"""Step mapper pour l'exécution parallèle sur une collection.

Ce module définit MapperStep qui applique une tâche à chaque élément
d'une collection en parallèle, ainsi que la tâche partagée wait_tasks
pour la collecte des résultats.

Auteur: SoniqueBay Team
Version: 0.3.2
"""

import asyncio
from collections.abc import Iterable
from typing import Any

import pydantic
from taskiq import (
    AsyncBroker,
    AsyncTaskiqDecoratedTask,
    Context,
    TaskiqDepends,
    TaskiqResult,
    async_shared_broker,
)
from taskiq.kicker import AsyncKicker

from taskiq_flow.abc import AbstractStep
from taskiq_flow.constants import (
    CURRENT_STEP,
    PIPELINE_DATA,
    STEP_RETRIES,
    STEP_RETRY_DELAY,
    STEP_TIMEOUT,
)
from taskiq_flow.exceptions import AbortPipeline, MappingError


@async_shared_broker.task(task_name="taskiq_flow.shared.wait_tasks")
async def wait_tasks(
    task_ids: list[str],
    check_interval: float,
    skip_errors: bool = True,
    context: Context = TaskiqDepends(),
) -> list[Any]:
    """
    Waits for subtasks to complete.

    This function is used by mapper
    step.

    It awaits for all tasks from task_ids
    to complete and then collects results
    in single list.

    :param task_ids: list of task ids.
    :param check_interval: how often task completions are checked.
    :param context: current execution context, defaults to default_context
    :param skip_errors: doesn't fail pipeline if error is found.
    :raises TaskiqError: if error is found and skip_errors is false.
    :return: list of results.
    """
    ordered_ids = task_ids[:]
    tasks_set = set(task_ids)
    while tasks_set:
        for task_id in task_ids:
            if await context.broker.result_backend.is_result_ready(task_id):
                try:
                    tasks_set.remove(task_id)
                except LookupError:
                    continue
        if tasks_set:
            await asyncio.sleep(check_interval)

    results = []
    for task_id in ordered_ids:
        result = await context.broker.result_backend.get_result(task_id)
        if result.is_err:
            if skip_errors:
                continue
            err_cause = None
            if isinstance(result.error, BaseException):
                err_cause = result.error
            raise MappingError(task_id=task_id, error=result.error) from err_cause

        results.append(result.return_value)
    return results


class MapperStep(pydantic.BaseModel, AbstractStep, step_name="mapper"):
    """
    Step de mapping parallèle sur une collection.

    Prend le résultat de l'étape précédente (qui doit être itérable)
    et exécute la tâche spécifiée sur chaque élément en parallèle.
    Les résultats sont collectés dans une liste ordonnée.

    Attributs:
        task_name: Nom de la tâche à exécuter pour chaque élément
        labels: Labels TaskIQ
        param_name: Nom du paramètre injectant chaque élément
        additional_kwargs: Arguments additionnels communs
        skip_errors: Si True, les erreurs sont ignorées
        check_interval: Fréquence de vérification de complétion (secondes)
        retries: Nombre de tentatives (optionnel)
        timeout: Timeout par tâche (optionnel)
        retry_delay: Délai entre tentatives (optionnel)
    """

    task_name: str
    labels: dict[str, str]
    param_name: str | None
    additional_kwargs: dict[str, Any]
    skip_errors: bool
    check_interval: float
    retries: int | None = None
    timeout: int | None = None
    retry_delay: int | None = None

    async def act(
        self,
        broker: AsyncBroker,
        step_number: int,
        parent_task_id: str,
        task_id: str,
        pipe_data: str,
        result: "TaskiqResult[Any]",
    ) -> None:
        """
        Exécute l'étape de map.

        Itère sur le résultat de l'étape précédente et crée une
        sous-tâche pour chaque élément. Une tâche collectrice
        (wait_tasks) attend la complétion de toutes les sous-tâches
        et assemble le résultat final.

        Args:
            broker: Broker TaskIQ
            step_number: Numéro de l'étape actuelle
            parent_task_id: ID de la tâche parente
            task_id: ID à utiliser pour la tâche collectrice
            pipe_data: Pipeline sérialisé
            result: Résultat de l'étape précédente (doit être itérable)

        Raises:
            AbortPipeline: Si le résultat précédent n'est pas itérable
        """
        sub_task_ids: list[str] = []
        return_value = result.return_value
        if not isinstance(return_value, Iterable):
            raise AbortPipeline(reason="Result of the previous task is not iterable.")

        for item in return_value:
            kicker: AsyncKicker[Any, Any] = AsyncKicker(
                task_name=self.task_name,
                broker=broker,
                labels=self.labels,
            )
            if self.param_name:
                self.additional_kwargs[self.param_name] = item
                task = await kicker.kiq(**self.additional_kwargs)
            else:
                task = await kicker.kiq(item, **self.additional_kwargs)
            sub_task_ids.append(task.task_id)

        await (
            wait_tasks.kicker()
            .with_task_id(task_id)
            .with_broker(
                broker,
            )
            .with_labels(
                **{CURRENT_STEP: step_number, PIPELINE_DATA: pipe_data},  # type: ignore
            )
            .kiq(
                sub_task_ids,
                check_interval=self.check_interval,
                skip_errors=self.skip_errors,
            )
        )

    @classmethod
    def from_task(
        cls,
        task: AsyncKicker[Any, Any] | AsyncTaskiqDecoratedTask[Any, Any],
        param_name: str | None,
        skip_errors: bool,
        check_interval: float,
        retries: int | None = None,
        timeout: int | None = None,
        retry_delay: int | None = None,
        **additional_kwargs: Any,
    ) -> "MapperStep":
        """
        Create new mapper step from task.

        :param task: task to execute.
        :param param_name: parameter name.
        :param skip_errors: don't fail collector
            task on errors.
        :param check_interval: how often tasks are checked.
        :param retries: retry count.
        :param timeout: timeout in seconds.
        :param retry_delay: delay between retries.
        :param additional_kwargs: additional function's kwargs.
        :return: new mapper step.
        """
        kicker = task.kicker() if isinstance(task, AsyncTaskiqDecoratedTask) else task
        message = kicker._prepare_message()
        labels = dict(message.labels)

        # Add retry/timeout labels
        if retries is not None:
            labels[STEP_RETRIES] = str(retries)
        if timeout is not None:
            labels[STEP_TIMEOUT] = str(timeout)
        if retry_delay is not None:
            labels[STEP_RETRY_DELAY] = str(retry_delay)

        return MapperStep(
            task_name=message.task_name,
            labels=labels,
            param_name=param_name,
            additional_kwargs=additional_kwargs,
            skip_errors=skip_errors,
            check_interval=check_interval,
            retries=retries,
            timeout=timeout,
            retry_delay=retry_delay,
        )

