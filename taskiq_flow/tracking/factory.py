# ruff: noqa: PLC0415
"""Factory for creating pipeline storage with auto-detection."""

from taskiq import AsyncBroker

from ..broker.detector import BrokerDetector, BrokerType
from .memory_storage import InMemoryPipelineStorage
from .redis_storage import RedisPipelineStorage
from .storage import PipelineStorage


class TrackingStorageFactory:
    """Factory for creating appropriate pipeline storage based on broker."""

    @staticmethod
    def create(
        broker: AsyncBroker,
        redis_url: str | None = None,
        ttl_seconds: int = 3600,
    ) -> PipelineStorage:
        """Create storage based on broker type auto-detection."""
        broker_type = BrokerDetector.detect(broker)

        if broker_type == BrokerType.REDIS:
            # Try to extract Redis URL from broker if not provided
            if redis_url is None:
                redis_url = TrackingStorageFactory._extract_redis_url(broker)
            if redis_url:
                return RedisPipelineStorage(redis_url, ttl_seconds)
            # Fallback to memory if Redis URL not available
            return InMemoryPipelineStorage()

        if broker_type in (BrokerType.RABBITMQ, BrokerType.KAFKA):
            # For brokers without shared storage, use Redis if URL provided
            if redis_url:
                return RedisPipelineStorage(redis_url, ttl_seconds)
            return InMemoryPipelineStorage()

        # InMemory or unknown
        return InMemoryPipelineStorage()

    @staticmethod
    def _extract_redis_url(broker: AsyncBroker) -> str | None:
        """Extract Redis URL from broker if possible."""
        # First, try to get URL from broker's url attribute (for any broker type)
        broker_url = getattr(broker, "url", None)
        if broker_url and str(broker_url).startswith("redis://"):
            return str(broker_url)

        # Then, try to import taskiq_redis and check if it's a RedisBroker
        try:
            from taskiq_redis.broker import RedisBroker  # type: ignore

            if isinstance(broker, RedisBroker):
                # Assuming broker has a url attribute or similar
                # This might need adjustment based on actual taskiq-redis implementation
                return getattr(broker, "url", None) or "redis://localhost:6379"
        except ImportError:
            pass
        return None
