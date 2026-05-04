"""Tâches utilitaires partagées pour les opérations de pipeline.

Contient des tâches communes comme identity_task utilisées
par divers steps.

Auteur: SoniqueBay Team
Version: 0.3.2
"""

from typing import Any

from taskiq import async_shared_broker


@async_shared_broker.task(task_name="taskiq_flow.shared.identity")
async def identity_task(value: Any) -> Any:
    """Identity task that returns the input value unchanged."""
    return value
