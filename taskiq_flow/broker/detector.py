"""
Détection du type de broker TaskIQ.

Ce module permet de détecter automatiquement le type de broker
(Redis, RabbitMQ, Kafka, InMemory) à partir d'une instance AsyncBroker.
Utile pour l'auto-configuration des composants dépendants du broker.

Auteur: SoniqueBay Team
Version: 1.0.2
"""

import sys
from enum import Enum

if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    # Fallback for Python 3.10 and earlier
    class StrEnum(str, Enum):
        """String Enum for Python < 3.11."""

        def __str__(self) -> str:
            return str(self.value)


from taskiq import AsyncBroker
from taskiq.brokers.inmemory_broker import InMemoryBroker
from taskiq.brokers.shared_broker import AsyncSharedBroker

try:
    from taskiq_redis import RedisStreamBroker as RedisBroker

    HAS_REDIS = True
except ImportError:
    HAS_REDIS = False

try:
    from taskiq_aio_pika import AioPikaBroker as RabbitBroker

    HAS_RABBITMQ = True
except ImportError:
    HAS_RABBITMQ = False

try:
    from taskiq_aio_kafka import AioKafkaBroker as KafkaBroker

    HAS_KAFKA = True
except ImportError:
    HAS_KAFKA = False


class BrokerType(StrEnum):
    """Supported broker types."""

    REDIS = "redis"
    RABBITMQ = "rabbitmq"
    KAFKA = "kafka"
    INMEMORY = "inmemory"
    UNKNOWN = "unknown"


class BrokerDetector:
    """Detect broker type from AsyncBroker instance."""

    @staticmethod
    def detect(broker: AsyncBroker) -> BrokerType:
        """Detect the type of the given broker."""
        # Handle SharedBroker (unwrap to actual broker)
        if isinstance(broker, AsyncSharedBroker):
            broker = getattr(broker, "broker", broker)
        # Try Redis
        if HAS_REDIS and isinstance(broker, RedisBroker):
            return BrokerType.REDIS

        # Try RabbitMQ
        if HAS_RABBITMQ and isinstance(broker, RabbitBroker):
            return BrokerType.RABBITMQ

        # Try Kafka
        if HAS_KAFKA and isinstance(broker, KafkaBroker):
            return BrokerType.KAFKA

        # InMemory
        if isinstance(broker, InMemoryBroker):
            return BrokerType.INMEMORY

        # Unknown
        return BrokerType.UNKNOWN
