"""Broker type detection."""

from enum import StrEnum

from taskiq import AsyncBroker
from taskiq.brokers.inmemory_broker import InMemoryBroker
from taskiq.brokers.shared_broker import AsyncSharedBroker


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
            broker = broker.broker

        # Try Redis
        try:
            from taskiq_redis.broker import RedisBroker
            if isinstance(broker, RedisBroker):
                return BrokerType.REDIS
        except ImportError:
            pass

        # Try RabbitMQ
        try:
            from taskiq_rabbit.broker import RabbitBroker
            if isinstance(broker, RabbitBroker):
                return BrokerType.RABBITMQ
        except ImportError:
            pass

        # Try Kafka
        try:
            from taskiq_kafka.broker import KafkaBroker
            if isinstance(broker, KafkaBroker):
                return BrokerType.KAFKA
        except ImportError:
            pass

        # InMemory
        if isinstance(broker, InMemoryBroker):
            return BrokerType.INMEMORY

        # Unknown
        return BrokerType.UNKNOWN
