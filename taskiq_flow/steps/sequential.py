"""Step séquentiel pour l'exécution linéaire de tâches.

Ce module définit SequentialStep, le step le plus fondamental qui
exécute une tâche après une autre en passant le résultat de la
précédente comme argument à la suivante.

Auteur: SoniqueBay Team
Version: 1.0.2
"""

from typing import Any

import pydantic
from taskiq import AsyncBroker, AsyncTaskiqDecoratedTask, TaskiqResult
from taskiq.kicker import AsyncKicker

from taskiq_flow.abc import AbstractStep
from taskiq_flow.constants import (
    CURRENT_STEP,
    EMPTY_PARAM_NAME,
    PIPELINE_DATA,
    STEP_RETRIES,
    STEP_RETRY_DELAY,
    STEP_TIMEOUT,
)


class SequentialStep(pydantic.BaseModel, AbstractStep, step_name="sequential"):
    """
    Step séquentiel exécutant une tâche après une autre.

    C'est le step le plus fondamental. Il exécute une tâche et peut
    optionnellement passer le résultat de la tâche précédente comme
    argument à la tâche courante.

    Attributs:
        task_name: Nom de la tâche à exécuter
        labels: Labels TaskIQ à attacher à la tâche
        param_name: Comment injecter le résultat précédent:
                   - None: comme premier argument positionnel
                   - str: comme argument keyword avec ce nom
                   - -1: ne pas passer le résultat
        additional_kwargs: Arguments fixes additionnels
        retries: Nombre de tentatives en cas d'échec
        timeout: Timeout en secondes
        retry_delay: Délai entre les tentatives
    """

    task_name: str
    labels: dict[str, str]
    # order is important here, otherwise pydantic will always choose str.
    # we use int instead of Literal[-1] because pydantic thinks that -1 is always str.
    param_name: int | str | None
    additional_kwargs: dict[str, Any]
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
        Exécute l'étape séquentielle.

        Crée une nouvelle tâche TaskIQ et lui passe le résultat
        de la tâche précédente selon le mode configuré.

        Args:
            broker: Broker TaskIQ pour soumettre la tâche
            step_number: Numéro de l'étape dans le pipeline (0-indexé)
            parent_task_id: ID de la tâche parente (étape précédente)
            task_id: ID à attribuer à cette tâche
            pipe_data: Données sérialisées du pipeline (à passer dans les labels)
            result: Résultat de l'étape précédente

        Note:
            Le résultat précédent peut être passé de trois façons:
            - Positionnel (param_name=None): comme premier argument
            - Keyword (param_name="nom"): comme argument nommé
            - Omis (param_name=-1): pas d'injection du résultat
        """
        kicker: AsyncKicker[Any, Any] = (
            AsyncKicker(
                task_name=self.task_name,
                broker=broker,
                labels=self.labels,
            )
            .with_task_id(task_id)
            .with_labels(
                **{PIPELINE_DATA: pipe_data, CURRENT_STEP: step_number},  # type: ignore
            )
        )
        if isinstance(self.param_name, str):
            self.additional_kwargs[self.param_name] = result.return_value
            await kicker.kiq(**self.additional_kwargs)
        elif self.param_name == EMPTY_PARAM_NAME:
            await kicker.kiq(**self.additional_kwargs)
        else:
            await kicker.kiq(result.return_value, **self.additional_kwargs)

    @classmethod
    def from_task(
        cls,
        task: AsyncKicker[Any, Any] | AsyncTaskiqDecoratedTask[Any, Any],
        param_name: str | int | None,
        retries: int | None = None,
        timeout: int | None = None,
        retry_delay: int | None = None,
        **additional_kwargs: Any,
    ) -> "SequentialStep":
        """
        Fabrique un step séquentiel à partir d'une tâche TaskIQ.

        Args:
            task: Tâche décorée ou kicker TaskIQ
            param_name: Nom du paramètre ou constantes (EMPTY_PARAM_NAME)
            retries: Nombre de tentatives (override)
            timeout: Timeout en secondes
            retry_delay: Délai entre tentatives (secondes)
            **additional_kwargs: Arguments additionnels pour la tâche

        Returns:
            Instance de SequentialStep prête pour l'exécution

        Example:
            step = SequentialStep.from_task(
                my_task,
                param_name="input_data",
                retries=3,
                timeout=60
            )
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

        return SequentialStep(
            task_name=message.task_name,
            labels=labels,
            param_name=param_name,
            additional_kwargs=additional_kwargs,
            retries=retries,
            timeout=timeout,
            retry_delay=retry_delay,
        )
