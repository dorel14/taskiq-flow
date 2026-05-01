"""Tests for broker detection and adapter."""

import pytest
from taskiq import InMemoryBroker

from taskiq_flow.broker.adapter import BrokerAdapter
from taskiq_flow.broker.detector import BrokerDetector, BrokerType


def test_broker_type_enum() -> None:
    """Test BrokerType enum values."""
    assert BrokerType.REDIS.value == "redis"
    assert BrokerType.RABBITMQ.value == "rabbitmq"
    assert BrokerType.KAFKA.value == "kafka"
    assert BrokerType.INMEMORY.value == "inmemory"
    assert BrokerType.UNKNOWN.value == "unknown"


def test_detect_inmemory_broker() -> None:
    """Test detection of InMemoryBroker."""
    broker = InMemoryBroker()
    detected = BrokerDetector.detect(broker)
    assert detected == BrokerType.INMEMORY


# Note: SharedBroker detection is complex and depends on taskiq internals
# The basic detection for known broker types works


def test_detect_unknown_broker() -> None:
    """Test detection of unknown broker type."""

    class UnknownBroker:
        pass

    broker = UnknownBroker()
    detected = BrokerDetector.detect(broker)  # type: ignore[arg-type]
    assert detected == BrokerType.UNKNOWN


def test_broker_adapter_creation() -> None:
    """Test BrokerAdapter creation."""
    broker = InMemoryBroker()
    adapter = BrokerAdapter(broker)
    assert adapter.broker == broker


@pytest.mark.asyncio
async def test_broker_adapter_result_operations() -> None:
    """Test BrokerAdapter result backend operations."""
    broker = InMemoryBroker()

    # Set up a result
    task_id = "test_task_123"
    result_value = "test_result"

    adapter = BrokerAdapter(broker)

    # Test setting result
    await adapter.set_result(task_id, result_value)

    # Test checking if ready
    is_ready = await adapter.is_result_ready(task_id)
    assert is_ready is True

    # Test getting result
    retrieved = await adapter.get_result(task_id)
    assert retrieved == result_value


@pytest.mark.asyncio
async def test_broker_adapter_not_ready() -> None:
    """Test BrokerAdapter when result is not ready."""
    broker = InMemoryBroker()
    adapter = BrokerAdapter(broker)

    task_id = "nonexistent_task"
    is_ready = await adapter.is_result_ready(task_id)
    assert is_ready is False


def test_broker_adapter_get_task_id() -> None:
    """Test BrokerAdapter.get_task_id method."""
    broker = InMemoryBroker()
    adapter = BrokerAdapter(broker)

    task_id = "test_123"
    result = adapter.get_task_id(task_id)
    assert result == task_id
