"""Modèles de données pour le suivi des pipelines.

Définit les modèles Pydantic pour représenter l'état d'exécution
des pipelines et de leurs étapes: PipelineStatusInfo, StepStatusInfo,
ainsi que les enums PipelineStatus et StepStatus.

Auteur: SoniqueBay Team
Version: 0.3.2
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class PipelineStatus(str, Enum):
    """
    Enumération des états d'exécution d'un pipeline.

    Utilisé pour catégoriser l'état global du pipeline à tout moment.
    """

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class StepStatus(str, Enum):
    """
    Enumération des états d'une étape individuelle.

    Chaque étape du pipeline traverse ces états séquentiellement
    ou peut être SKIPPED en cas d'option skip_failed/fail_fast.
    """

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class StepStatusInfo(BaseModel):
    """
    Informations de statut pour une étape de pipeline.

    Représente l'état d'une étape individuelle, incluant
    ses timestamps et d'éventuelles erreurs.

    Attributes:
        step_index: Index de l'étape dans le pipeline (0-based)
        task_name: Nom de la tâche exécutée
        task_id: ID TaskIQ de l'exécution
        status: État actuel (StepStatus)
        started_at: Horodatage de début (ou None)
        finished_at: Horodatage de fin (ou None)
        retries: Nombre de tentatives effectuées
        error: Message d'erreur si échec
    """

    step_index: int
    task_name: str
    task_id: str
    status: StepStatus
    started_at: datetime | None = None
    finished_at: datetime | None = None
    retries: int = 0
    error: str | None = None


class PipelineStatusInfo(BaseModel):
    """
    Information complète de statut pour un pipeline.

    Agrège l'état global et les états de chaque étape.
    Utilisé comme objet de retour pour get_status().

    Attributes:
        pipeline_id: Identifiant unique du pipeline
        status: État global (PipelineStatus)
        total_steps: Nombre total d'étapes attendues
        current_step: Index de l'étape en cours (0 si none en cours)
        created_at: Horodatage de création
        started_at: Horodatage de premier démarrage (ou None)
        finished_at: Horodatage de fin (ou None)
        result: Résultat final si complété
        error: Message d'erreur si échoué
        steps: Liste des statuts d'étape
    """

    pipeline_id: str
    status: PipelineStatus
    total_steps: int
    current_step: int = 0
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: datetime | None = None
    finished_at: datetime | None = None
    result: Any = None
    error: str | None = None
    steps: list[StepStatusInfo] = Field(default_factory=list)
