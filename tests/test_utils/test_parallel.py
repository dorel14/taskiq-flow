"""Tests for parallel utilities."""

from typing import Any

import pytest
from taskiq import InMemoryBroker
from taskiq.kicker import AsyncKicker

from taskiq_flow.pipeliner import Pipeline
from taskiq_flow.utils.parallel import chunked_map, parallel_map


@pytest.fixture
def broker() -> InMemoryBroker:
    """Create test broker."""
    return InMemoryBroker()


@pytest.fixture
def mock_task(broker: InMemoryBroker) -> AsyncKicker[[int], int]:
    """Create mock task."""

    @broker.task
    def test_task(x: int) -> int:
        return x * 2

    return test_task  # type: ignore[return-value]


def test_parallel_map_creation(
    broker: InMemoryBroker, mock_task: AsyncKicker[[int], int],
) -> None:
    """Test parallel_map creates a pipeline."""
    pipeline: Pipeline[Any, list[int]] = parallel_map(mock_task, [1, 2, 3])  # type: ignore[arg-type]
    assert pipeline is not None
    assert len(pipeline.steps) == 3  # One step per item


def test_parallel_map_with_kwargs(
    broker: InMemoryBroker, mock_task: AsyncKicker[[int], int],
) -> None:
    """Test parallel_map with additional kwargs."""
    pipeline: Pipeline[Any, list[int]] = parallel_map(mock_task, [1, 2], multiplier=3)  # type: ignore[arg-type]
    assert len(pipeline.steps) == 2


def test_chunked_map_creation(
    broker: InMemoryBroker, mock_task: AsyncKicker[[list[int]], int],
) -> None:
    """Test chunked_map creates a pipeline."""
    items = list(range(10))
    pipeline: Pipeline[Any, list[int]] = chunked_map(mock_task, items, chunk_size=3)  # type: ignore[arg-type]
    assert pipeline is not None
    # Should create steps for chunks: [0,1,2], [3,4,5], [6,7,8], [9]
    assert len(pipeline.steps) == 4


def test_chunked_map_chunk_size(
    broker: InMemoryBroker, mock_task: AsyncKicker[[list[int]], int],
) -> None:
    """Test chunked_map with different chunk sizes."""
    items = [1, 2, 3, 4, 5]

    # Chunk size 2
    pipeline: Pipeline[Any, list[int]] = chunked_map(mock_task, items, chunk_size=2)  # type: ignore[arg-type]
    assert len(pipeline.steps) == 3  # [1,2], [3,4], [5]

    # Chunk size 1
    pipeline = chunked_map(mock_task, items, chunk_size=1)  # type: ignore[arg-type]
    assert len(pipeline.steps) == 5  # One per item


def test_chunked_map_auto_concurrency(
    broker: InMemoryBroker, mock_task: AsyncKicker[[list[int]], int],
) -> None:
    """Test chunked_map with auto concurrency."""
    items = list(range(20))
    pipeline: Pipeline[Any, list[int]] = chunked_map(mock_task,
                                                    items, auto_concurrency=True)  # type: ignore[arg-type]
    # Should create multiple chunks
    assert len(pipeline.steps) > 1


def test_chunked_map_max_concurrency(
    broker: InMemoryBroker, mock_task: AsyncKicker[[list[int]], int],
) -> None:
    """
    Test chunked_map with max concurrency
    (currently not implemented in pipeline creation).
    """
    items = list(range(20))
    pipeline: Pipeline[Any, list[int]] = chunked_map(mock_task, items, max_concurrency=5) # type: ignore[arg-type]
    # Currently, max_concurrency doesn't affect pipeline creation
    # This is a placeholder for future implementation
    assert isinstance(pipeline, Pipeline)
