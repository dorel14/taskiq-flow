"""Module de compatibilité et détection des brokers.

Ce module centralise les outils pour détecter et adapter
différents types de brokers TaskIQ (Redis, RabbitMQ, Kafka, InMemory).
Utilisé notamment par le TrackingStorageFactory pour choisir
le stockage approprié.

Auteur: SoniqueBay Team
Version: 1.0.2
"""

from taskiq_flow.broker.adapter import BrokerAdapter
from taskiq_flow.broker.detector import BrokerDetector, BrokerType

__all__ = [
    "BrokerAdapter",
    "BrokerDetector",
    "BrokerType",
]
