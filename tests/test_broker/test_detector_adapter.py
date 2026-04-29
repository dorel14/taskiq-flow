"""Tests for broker detection and adapter."""

import pytest
from taskiq import InMemoryBroker

from taskiq_pipelines.broker.adapter import BrokerAdapter
from taskiq_pipelines.broker.detector import BrokerDetector, BrokerType


def test_broker_type_enum():
    """Test BrokerType enum values."""
    assert BrokerType.REDIS == "redis"
    assert BrokerType.RABBITMQ == "rabbitmq"
    assert BrokerType.KAFKA == "kafka"
    assert BrokerType.INMEMORY == "inmemory"
    assert BrokerType.UNKNOWN == "unknown"


def test_detect_inmemory_broker():
    """Test detection of InMemoryBroker."""
    broker = InMemoryBroker()
    detected = BrokerDetector.detect(broker)
    assert detected == BrokerType.INMEMORY


# Note: SharedBroker detection is complex and depends on taskiq internals
# The basic detection for known broker types works


def test_detect_unknown_broker():
    """Test detection of unknown broker type."""
    class UnknownBroker:
        pass

    broker = UnknownBroker()
    detected = BrokerDetector.detect(broker)
    assert detected == BrokerType.UNKNOWN


def test_broker_adapter_creation():
    """Test BrokerAdapter creation."""
    broker = InMemoryBroker()
    adapter = BrokerAdapter(broker)
    assert adapter.broker == broker


@pytest.mark.asyncio
async def test_broker_adapter_result_operations():
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
async def test_broker_adapter_not_ready():
    """Test BrokerAdapter when result is not ready."""
    broker = InMemoryBroker()
    adapter = BrokerAdapter(broker)

    task_id = "nonexistent_task"
    is_ready = await adapter.is_result_ready(task_id)
    assert is_ready is False


def test_broker_adapter_get_task_id():
    """Test BrokerAdapter.get_task_id method."""
    broker = InMemoryBroker()
    adapter = BrokerAdapter(broker)

    task_id = "test_123"
    result = adapter.get_task_id(task_id)
    assert result == task_id
