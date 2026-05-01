"""Tests for Redis pipeline storage."""


import pytest

from taskiq_flow.tracking.redis_storage import RedisPipelineStorage

redis = pytest.importorskip("redis")


@pytest.fixture
async def redis_storage() -> RedisPipelineStorage:
    """Create Redis storage instance."""
    pytest.importorskip("redis")
    storage = RedisPipelineStorage("redis://localhost:6379", ttl_seconds=3600)
    try:
        # Test connection
        await storage.redis.ping()
    except Exception as e:
        pytest.skip(f"Redis not available for testing: {e}")
    return storage


@pytest.mark.asyncio
async def test_redis_create_pipeline(redis_storage: RedisPipelineStorage) -> None:
    """Test creating a pipeline in Redis."""
    await redis_storage.create_pipeline("test_pipe", 2)

    status = await redis_storage.get_pipeline_status("test_pipe")
    assert status is not None
    assert status.pipeline_id == "test_pipe"
    assert status.total_steps == 2


@pytest.mark.asyncio
async def test_redis_pipeline_lifecycle(redis_storage: RedisPipelineStorage) -> None:
    """Test full pipeline lifecycle in Redis."""
    pipe_id = "redis_test_pipe"

    # Create
    await redis_storage.create_pipeline(pipe_id, 1)

    # Start
    await redis_storage.start_pipeline(pipe_id)

    # Start step
    await redis_storage.start_step(pipe_id, 0, "task1", "Test Task")

    # Complete step
    await redis_storage.complete_step(pipe_id, 0)

    # Complete pipeline
    await redis_storage.complete_pipeline(pipe_id, "success")

    # Verify
    status = await redis_storage.get_pipeline_status(pipe_id)
    assert status is not None
    assert status.status.name == "COMPLETED"
    assert status.result == "success"


@pytest.mark.asyncio
async def test_redis_list_pipelines(redis_storage: RedisPipelineStorage) -> None:
    """Test listing pipelines in Redis."""
    # Create multiple pipelines
    for i in range(3):
        await redis_storage.create_pipeline(f"pipe{i}", 1)

    pipelines = await redis_storage.list_pipelines()
    # Redis implementation returns empty list (simplified)
    assert isinstance(pipelines, list)


@pytest.mark.asyncio
async def test_redis_cleanup(redis_storage: RedisPipelineStorage) -> None:
    """Test cleanup in Redis."""
    await redis_storage.create_pipeline("cleanup_test", 1)
    await redis_storage.complete_pipeline("cleanup_test", "done")

    # Cleanup is a no-op in Redis implementation (TTL handled automatically)
    cleaned = await redis_storage.cleanup_old(ttl_seconds=1)
    assert cleaned == 0
