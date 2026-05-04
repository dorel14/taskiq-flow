"""Adaptateur pour les opérations de résultat du broker.

Fournit une interface unifiée pour les opérations sur le backend
de résultats (is_result_ready, get_result, set_result) independent
de l'implémentation concrète du broker TaskIQ.

Auteur: SoniqueBay Team
Version: 0.3.2
"""

import logging
from typing import Any

from taskiq import AsyncBroker

logger = logging.getLogger(__name__)


class BrokerAdapter:
    """Adapter for broker result backend operations."""

    def __init__(self, broker: AsyncBroker) -> None:
        self.broker = broker

    async def is_result_ready(self, task_id: str) -> bool:
        """Check if result is ready."""
        try:
            return await self.broker.result_backend.is_result_ready(task_id)
        except Exception as e:
            logger.error(f"Failed to check if result is ready for task {task_id}: {e}")
            raise

    async def get_result(self, task_id: str) -> Any:
        """Get result for task."""
        try:
            return await self.broker.result_backend.get_result(task_id)
        except Exception as e:
            logger.error(f"Failed to get result for task {task_id}: {e}")
            raise

    async def set_result(self, task_id: str, result: Any) -> None:
        """Set result for task."""
        try:
            await self.broker.result_backend.set_result(task_id, result)
        except Exception as e:
            logger.error(f"Failed to set result for task {task_id}: {e}")
            raise

    def get_task_id(self, task_id: str) -> str:
        """Get task ID (pass through)."""
        return task_id

