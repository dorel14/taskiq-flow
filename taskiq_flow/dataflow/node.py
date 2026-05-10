"""Représentation d'un nœud de données dans le graphe dataflow.

Ce module définit DataNode qui représente un artifact de données
produit ou consommé par les tâches du pipeline. Un DataNode suit
les producteurs et consumers pour construire les dépendances.

Auteur: SoniqueBay Team
Version: 1.0.2
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

    C'est la brique de base pour le suivi des flux de données à travers
    le pipeline. Chaque donnée (intermédiaire ou finale) est représentée
    par un DataNode.

    Attributes:
        name: Nom unique du flux de données (ex: "audio_features")
        producer_task: Tâche productrice (None si entrée externe)
        consumers: Liste des tâches consommant ce flux
        is_external: True si ce flux est une entrée externe du pipeline

    Example:
        Création d'un nœud pour une donnée produite:

        >>> from taskiq_flow.dataflow import DataNode
        >>> node = DataNode(name="audio_features")
        >>> node.set_producer(task_extract)
        >>> assert node.producer_task == task_extract
        >>> assert node.is_external is False

    Example:
        Création d'un nœud pour une entrée externe:

        >>> node = DataNode(name="user_config", is_external=True)
        >>> node.add_consumer(task_process)
        >>> assert task_process in node.consumers
        >>> assert node.producer_task is None
    """

    name: str
    producer_task: Any = None
    consumers: list[Any] = field(default_factory=list)
    is_external: bool = False

    def add_consumer(self, task: Any) -> None:
        """
        Ajoute une tâche consommatrice pour cette donnée.

        La tâche sera ajoutée à la liste des consommateurs si elle
        n'y est pas déjà. Cela permet de suivre qui dépend de cette
        sortie de données.

        Args:
            task: La tâche consommatrice à enregistrer

        Example:
            >>> node = DataNode(name="features")
            >>> node.add_consumer(task_train)
            >>> node.add_consumer(task_evaluate)
            >>> len(node.consumers)
            2
        """
        if task not in self.consumers:
            self.consumers.append(task)

    def set_producer(self, task: Any) -> None:
        """
        Définit la tâche productrice de cette donnée.

        Cette méthode est appelée lorsqu'une tâche déclare produire
        cette donnée en sortie.

        Args:
            task: La tâche productrice

        Example:
            >>> node = DataNode(name="result")
            >>> node.set_producer(task_compute)
            >>> assert node.producer_task == task_compute
        """
        self.producer_task = task
