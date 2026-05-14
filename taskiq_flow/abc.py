"""
Classes abstraites de base pour les composants taskiq-flow.

Ce module définit les classes abstraites qui servent de contrat
pour les implémentations concrètes, notamment AbstractStep qui
doit être étendue par tous les steps de pipeline.

Auteur: SoniqueBay Team
Version: 1.0.2
"""

from abc import ABC, abstractmethod
from typing import Any, ClassVar

from taskiq import AsyncBroker, TaskiqResult


class AbstractStep(ABC):
    """
    Classe abstraite de base pour tous les steps de pipeline.

    Tout step personnalisé doit hériter de cette classe et
    implémenter la méthode act(). Le paramètre de classe step_name
    est automatiquement défini comme identifiant du step.

    Usage:
        class MonStep(AbstractStep, step_name="mon_step"):
            async def act(self, broker, step_number, parent_task_id,
                         task_id, pipe_data, result):
                # Logique d'exécution
                pass

    Attributes:
        _step_name: Identifiant unique du type de step (str)
        _known_steps: Registre global de tous les steps (ClassVar)

    """

    _step_name: str
    _known_steps: ClassVar[dict[str, type["AbstractStep"]]] = {}

    def __init_subclass__(cls, step_name: str, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Sets step name to the step.
        cls._step_name = step_name
        # Registers new subclass in the dict of known steps.
        cls._known_steps[step_name] = cls

    @abstractmethod
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
        Exécute l'action du step.

        Méthode principale à implémenter par les sous-classes.
        Reçoit le résultat de l'étape précédente et doit déclencher
        la (les) tâche(s) suivante(s) via le broker.

        Args:
            broker: Broker TaskIQ pour soumettre les tâches
            step_number: Numéro de l'étape (0-based dans le pipeline)
            parent_task_id: ID de la tâche parent (étape précédente)
            task_id: ID à attribuer à la (première) tâche créée
            pipe_data: Pipeline sérialisé (à passer dans les labels)
            result: Résultat de l'étape précédente

        Important:
            La tâche créée doit hériter des labels pipe_data et
            current_step pour que le middleware puisse la chaîner.

        """
