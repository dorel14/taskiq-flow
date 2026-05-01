"""Integration tests for map-reduce pipeline operations."""

import pytest
from taskiq import InMemoryBroker

from taskiq_flow import DataflowPipeline, pipeline_task
from taskiq_flow.map_reduce import ChunkConfig


@pytest.fixture
def broker() -> InMemoryBroker:
    """Create a test broker."""
    return InMemoryBroker()


class TestMapReducePipeline:
    """Integration tests for map-reduce with pipeline."""

    @pytest.mark.asyncio
    async def test_pipeline_map_reduce_basic(self, broker: InMemoryBroker) -> None:
        """Test basic map-reduce with pipeline."""

        @pipeline_task(output="doubled")
        @broker.task
        async def double(x: int) -> int:
            return x * 2

        @pipeline_task(output="total")
        @broker.task
        async def sum_all(items: list[int], initial: int = 0) -> int:
            return sum(items) + initial

        # Use kiq_map_reduce_advanced directly (no DAG needed for map-reduce)
        pipeline = DataflowPipeline(broker)

        items = list(range(1, 6))  # 1, 2, 3, 4, 5

        result = await pipeline.kiq_map_reduce_advanced(
            double,
            sum_all,
            items,
            max_parallel=3,
            initial=0,
        )

        # Sum of doubled 1-5 = 2+4+6+8+10 = 30
        assert result == 30

    @pytest.mark.asyncio
    async def test_pipeline_map_reduce_with_chunking(
        self, broker: InMemoryBroker
    ) -> None:
        """Test map-reduce with chunked reduction."""

        @pipeline_task(output="square")
        @broker.task
        async def square(x: int) -> int:
            return x * x

        @pipeline_task(output="sum")
        @broker.task
        async def sum_reduce(items: list[int], initial: int = 0) -> int:
            return sum(items) + initial

        # Use kiq_map_reduce_advanced directly
        pipeline = DataflowPipeline(broker)

        items = list(range(1, 21))  # 1 to 20

        result = await pipeline.kiq_map_reduce_advanced(
            square,
            sum_reduce,
            items,
            max_parallel=5,
            reduce_chunk_size=10,
            initial=0,
        )

        # Sum of squares 1-20 = 2870
        assert result == 2870

    @pytest.mark.asyncio
    async def test_pipeline_map_sweep(self, broker: InMemoryBroker) -> None:
        """Test parameter sweep with pipeline."""

        @pipeline_task(output="result")
        @broker.task
        async def compute(a: int, b: int, c: int = 1) -> int:
            return a + b * c

        pipeline = DataflowPipeline(broker)

        param_values = {
            "a": [1, 2],
            "b": [10, 20],
        }

        result = await pipeline.kiq_map_sweep(
            compute,
            param_values,
            output="result",
            c=2,
            max_parallel=5,
        )

        assert "result" in result
        assert "metadata" in result
        assert result["metadata"]["items_processed"] == 4  # 2*2 combinations
        assert result["metadata"]["success_rate"] == 1.0

        # Results: (1+10*2)=21, (1+20*2)=41, (2+10*2)=22, (2+20*2)=42
        expected = [21, 41, 22, 42]
        assert sorted(result["result"]) == sorted(expected)

    @pytest.mark.asyncio
    async def test_pipeline_map_with_chunk_config(self, broker: InMemoryBroker) -> None:
        """Test pipeline map with chunk configuration."""

        @pipeline_task(output="value")
        @broker.task
        async def process(x: int) -> int:
            return x * 10

        # Add map operation to pipeline
        pipeline = DataflowPipeline(broker)
        chunk_config = ChunkConfig(chunk_size=25, adaptive=False)
        pipeline.map(process, list(range(100)), "value", chunk_config=chunk_config)

        # Verify the configuration is stored correctly
        assert hasattr(pipeline, "_map_operations")
        assert len(pipeline._map_operations) == 1
        assert "chunk_config" in pipeline._map_operations[0]["kwargs"]
        assert pipeline._map_operations[0]["kwargs"]["chunk_config"].chunk_size == 25

    @pytest.mark.asyncio
    async def test_pipeline_map_reduce_metadata(self, broker: InMemoryBroker) -> None:
        """Test that map-reduce returns metadata."""

        @pipeline_task(output="processed")
        @broker.task
        async def process(x: int) -> int:
            return x + 1

        @pipeline_task(output="total")
        @broker.task
        async def sum_all(items: list[int], initial: int = 0) -> int:
            return sum(items) + initial

        pipeline = DataflowPipeline(broker)

        # Add map operation with chunk config
        chunk_config = ChunkConfig(chunk_size=5, adaptive=False)
        pipeline.map(process, list(range(10)), "processed", chunk_config=chunk_config)
        pipeline.reduce(sum_all, "processed", "total")

        result = await pipeline.kiq_map_reduce(initial=0)

        assert "processed" in result
        assert "total" in result
        assert "processed_metadata" in result
        assert result["processed_metadata"]["items_processed"] == 10
        assert result["processed_metadata"]["success_rate"] == 1.0
        assert result["total"] == sum(range(1, 11))  # 1+2+...+10 = 55


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
