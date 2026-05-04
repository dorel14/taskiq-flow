"""Représentation d'un nœud de données dans le graphe dataflow.

Ce module définit DataNode qui représente un artifact de données
produit ou consommé par les tâches du pipeline. Un DataNode suit
les producteurs et consumers pour construire les dépendances.

Auteur: SoniqueBay Team
Version: 0.3.1
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class DataNode:
    """
    Représente un artifact de données dans le graphe dataflow.

    Un DataNode suit la production et la consommation d'un flux
    de données nommé. Il permet de construire les dépendances
    entre tâches sans que celles-ci ne se connaissent directement.

    Attributes:
        name: Nom unique du flux de données (ex: "audio_features")
        producer_task: Tâche productrice (None si entrée externe)
        consumers: Liste des tâches consommant ce flux
        is_external: True si ce flux est une entrée externe du pipeline
    """

    name: str
    producer_task: Any = None
    consumers: list[Any] = field(default_factory=list)
    is_external: bool = False

    def add_consumer(self, task: Any) -> None:
        """Add a consumer task for this data."""
        if task not in self.consumers:
            self.consumers.append(task)

    def set_producer(self, task: Any) -> None:
        """Set the producer task for this data."""
        self.producer_task = task
