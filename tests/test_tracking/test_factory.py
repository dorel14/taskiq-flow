# mypy: disable-error-code=no-untyped-def
"""Tests for tracking storage factory."""

import sys
from unittest.mock import Mock, patch

from taskiq import InMemoryBroker

from taskiq_flow.broker.detector import BrokerType
from taskiq_flow.tracking.factory import TrackingStorageFactory
from taskiq_flow.tracking.memory_storage import InMemoryPipelineStorage
from taskiq_flow.tracking.redis_storage import RedisPipelineStorage


def test_create_with_inmemory_broker():
    """Test creating storage with InMemoryBroker."""
    broker = InMemoryBroker()
    storage = TrackingStorageFactory.create(broker)

    assert isinstance(storage, InMemoryPipelineStorage)


def test_create_with_redis_broker_mock() -> None:
    """Test creating storage with RedisBroker (mocked)."""

    # Mock a RedisBroker
    class MockRedisBroker:
        def __init__(self, url="redis://localhost:6379") -> None:
            self.url = url

    broker = MockRedisBroker()

    with patch(
        "taskiq_flow.tracking.factory.BrokerDetector.detect",
        return_value=BrokerType.REDIS,
    ):
        storage = TrackingStorageFactory.create(broker, redis_url="redis://test:6379")  # type: ignore[arg-type]

    assert isinstance(storage, RedisPipelineStorage)


def test_create_with_redis_broker_auto_extract():
    """Test creating storage with RedisBroker auto-extract URL."""

    # Mock RedisBroker with url attribute
    class MockRedisBroker:
        def __init__(self, url="redis://localhost:6379") -> None:
            self.url = url

    broker = MockRedisBroker("redis://auto:6379")

    with patch(
        "taskiq_flow.tracking.factory.BrokerDetector.detect",
        return_value=BrokerType.REDIS,
    ):
        storage = TrackingStorageFactory.create(broker)  # type: ignore[arg-type]

    assert isinstance(storage, RedisPipelineStorage)


def test_create_with_rabbitmq_broker():
    """Test creating storage with RabbitMQBroker."""

    class MockRabbitMQBroker:
        pass

    broker = MockRabbitMQBroker()

    with patch(
        "taskiq_flow.tracking.factory.BrokerDetector.detect",
        return_value=BrokerType.RABBITMQ,
    ):
        storage = TrackingStorageFactory.create(broker)  # type: ignore[arg-type]

    assert isinstance(storage, InMemoryPipelineStorage)


def test_create_with_rabbitmq_broker_with_redis():
    """Test creating storage with RabbitMQ broker with Redis URL."""

    class MockRabbitMQBroker:
        pass

    broker = MockRabbitMQBroker()

    with patch(
        "taskiq_flow.tracking.factory.BrokerDetector.detect",
        return_value=BrokerType.RABBITMQ,
    ):
        storage = TrackingStorageFactory.create(broker, redis_url="redis://shared:6379")  # type: ignore[arg-type]

    assert isinstance(storage, RedisPipelineStorage)


def test_extract_redis_url_with_redis_broker():
    """Test extracting Redis URL from RedisBroker."""
    # Mock taskiq_redis import
    mock_redis_broker = Mock()
    mock_redis_broker.url = "redis://mock:6379"

    # Mock the import
    mock_module = Mock()
    mock_module.RedisBroker = type(mock_redis_broker)
    sys.modules["taskiq_redis"] = mock_module
    sys.modules["taskiq_redis.broker"] = mock_module

    try:
        url = TrackingStorageFactory._extract_redis_url(mock_redis_broker)
        assert url == "redis://mock:6379"
    finally:
        # Clean up
        if "taskiq_redis" in sys.modules:
            del sys.modules["taskiq_redis"]
        if "taskiq_redis.broker" in sys.modules:
            del sys.modules["taskiq_redis.broker"]


def test_extract_redis_url_no_taskiq_redis():
    """Test extracting Redis URL when taskiq_redis not available."""
    broker = InMemoryBroker()
    url = TrackingStorageFactory._extract_redis_url(broker)
    assert url is None


def test_extract_redis_url_redis_broker_no_url():
    """Test extracting Redis URL from RedisBroker without url attribute."""
    # Mock taskiq_redis import
    mock_redis_broker = Mock()
    # No url attribute
    del mock_redis_broker.url

    mock_module = Mock()
    mock_module.RedisBroker = type(mock_redis_broker)
    sys.modules["taskiq_redis"] = mock_module
    sys.modules["taskiq_redis.broker"] = mock_module

    try:
        url = TrackingStorageFactory._extract_redis_url(mock_redis_broker)
        assert url == "redis://localhost:6379"  # default fallback
    finally:
        # Clean up
        if "taskiq_redis" in sys.modules:
            del sys.modules["taskiq_redis"]
        if "taskiq_redis.broker" in sys.modules:
            del sys.modules["taskiq_redis.broker"]


def test_create_with_ttl():
    """Test creating storage with custom TTL."""
    broker = InMemoryBroker()
    storage = TrackingStorageFactory.create(broker, ttl_seconds=7200)

    # InMemory doesn't use TTL, but Redis does
    assert isinstance(storage, InMemoryPipelineStorage)

    # Test with Redis
    class MockRedisBroker:
        pass

    broker = MockRedisBroker()  # type: ignore[assignment]

    with patch(
        "taskiq_flow.tracking.factory.BrokerDetector.detect",
        return_value=BrokerType.REDIS,
    ):
        storage = TrackingStorageFactory.create(
            broker,
            redis_url="redis://test:6379",
            ttl_seconds=7200,
        )

    assert isinstance(storage, RedisPipelineStorage)


def test_create_with_unknown_broker():
    """Test creating storage with unknown broker type."""

    class UnknownBroker:
        pass

    broker = UnknownBroker()
    storage = TrackingStorageFactory.create(broker)  # type: ignore[arg-type]

    assert isinstance(storage, InMemoryPipelineStorage)


def test_create_with_rabbitmq_broker_no_redis():
    """Test creating storage with RabbitMQ broker without Redis URL."""

    # Mock RabbitMQ broker
    class MockRabbitMQBroker:
        pass

    broker = MockRabbitMQBroker()
    storage = TrackingStorageFactory.create(broker)  # type: ignore[arg-type]

    assert isinstance(storage, InMemoryPipelineStorage)
