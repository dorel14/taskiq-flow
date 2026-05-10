"""Interface de stockage pour le suivi des pipelines.

Définit le contrat abstrait pour les backends de stockage
de suivi d'exécution de pipelines. Les implementations concrètes
incluent InMemoryPipelineStorage et RedisPipelineStorage.

Auteur: SoniqueBay Team
Version: 1.0.2
"""

from abc import ABC, abstractmethod
from typing import Any

from taskiq_flow.tracking.models import PipelineStatusInfo


class PipelineStorage(ABC):
    """
    Interface abstraite pour le stockage de suivi de pipelines.

    Définit le contrat que tout backend de stockage doit implémenter
    pour persister l'état d'exécution des pipelines. Permet de
    changer de backend (mémoire, Redis, etc.) sans modifier
    la logique métier.

    Méthodes à implémenter:
        - create_pipeline
        - start_pipeline / complete_pipeline / fail_pipeline
        - start_step / complete_step / fail_step
        - get_pipeline_status
        - list_pipelines
        - cleanup_old

    Les méthodes doivent être asynchrones et thread-safe.
    """

    @abstractmethod
    async def create_pipeline(self, pipeline_id: str, total_steps: int) -> None:
        """
        Crée une nouvelle entrée de pipeline avec statut initial.

        Args:
            pipeline_id: Identifiant unique du pipeline
            total_steps: Nombre total d'étapes attendues
        """
        ...

    @abstractmethod
    async def start_pipeline(self, pipeline_id: str) -> None:
        """
        Marque le pipeline comme démarré.

        Args:
            pipeline_id: Identifiant du pipeline
        """
        ...

    @abstractmethod
    async def complete_pipeline(self, pipeline_id: str, result: Any) -> None:
        """
        Marque le pipeline comme terminé avec succès.

        Args:
            pipeline_id: Identifiant du pipeline
            result: Résultat final de l'exécution
        """
        ...

    @abstractmethod
    async def fail_pipeline(self, pipeline_id: str, error: str) -> None:
        """
        Marque le pipeline comme échoué.

        Args:
            pipeline_id: Identifiant du pipeline
            error: Message d'erreur décrivant l'échec
        """
        ...

    @abstractmethod
    async def start_step(
        self,
        pipeline_id: str,
        step_index: int,
        task_id: str,
        task_name: str,
    ) -> None:
        """
        Marque une étape comme démarrée.

        Args:
            pipeline_id: Identifiant du pipeline
            step_index: Index de l'étape (0-based)
            task_id: ID de la tâche TaskIQ
            task_name: Nom de la tâche
        """
        ...

    @abstractmethod
    async def complete_step(self, pipeline_id: str, step_index: int) -> None:
        """
        Marque une étape comme terminée.

        Args:
            pipeline_id: Identifiant du pipeline
            step_index: Index de l'étape
        """
        ...

    @abstractmethod
    async def fail_step(self, pipeline_id: str, step_index: int, error: str) -> None:
        """
        Marque une étape comme échouée.

        Args:
            pipeline_id: Identifiant du pipeline
            step_index: Index de l'étape
            error: Message d'erreur
        """
        ...

    @abstractmethod
    async def get_pipeline_status(self, pipeline_id: str) -> PipelineStatusInfo | None:
        """
        Récupère le statut complet d'un pipeline.

        Args:
            pipeline_id: Identifiant du pipeline

        Returns:
            Dictionnaire du statut ou None si pipeline inexistant
        """
        ...

    @abstractmethod
    async def list_pipelines(self, limit: int = 10) -> list[PipelineStatusInfo]:
        """
        Liste les pipelines les plus récents.

        Args:
            limit: Nombre maximum de pipelines à retourner

        Returns:
            Liste ordonnée par date de création décroissante
        """
        ...

    @abstractmethod
    async def cleanup_old(self, ttl_seconds: int = 3600) -> int:
        """
        Nettoie les données anciennes dépassées.

        Args:
            ttl_seconds: Durée de vie en secondes

        Returns:
            Nombre d'éléments supprimés
        """
        ...
