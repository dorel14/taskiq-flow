"""Advanced map-reduce tests."""

import pytest
from taskiq import InMemoryBroker

from taskiq_flow import DataflowPipeline, pipeline_task
from taskiq_flow.map_reduce import ChunkConfig, MapReduce, MapResult


@pytest.fixture
def broker() -> InMemoryBroker:
    """Create a test broker."""
    return InMemoryBroker()


class TestMapReduceAdvanced:
    """Tests for advanced map-reduce operations."""

    @pytest.mark.asyncio
    async def test_map_with_chunking(self, broker: InMemoryBroker) -> None:
        """Test map operation with intelligent chunking."""

        @pipeline_task(output="doubled")
        @broker.task
        async def double(x: int) -> int:
            return x * 2

        items = list(range(100))
        chunk_config = ChunkConfig(chunk_size=25, adaptive=False)

        result = await MapReduce.map(
            broker,
            double,
            items,
            output="doubled",
            chunk_config=chunk_config,
            max_parallel=10,
        )

        assert isinstance(result, MapResult)
        assert len(result.results) == 100
        assert result.output_name == "doubled"
        assert result.items_processed == 100
        assert result.duration > 0
        assert result.success_rate == 1.0
        assert result.results[:5] == [0, 2, 4, 6, 8]

    @pytest.mark.asyncio
    async def test_map_with_adaptive_chunking(self, broker: InMemoryBroker) -> None:
        """Test map with adaptive chunk sizing."""

        @pipeline_task(output="square")
        @broker.task
        async def square(x: int) -> int:
            return x * x

        # Small list - should use smaller chunks
        items_small = list(range(50))
        chunk_config = ChunkConfig(adaptive=True, min_chunk_size=10, max_chunk_size=100)

        result = await MapReduce.map(
            broker,
            square,
            items_small,
            output="square",
            chunk_config=chunk_config,
            max_parallel=5,
        )

        assert len(result.results) == 50
        assert result.success_rate == 1.0

        # Large list - should use larger chunks
        items_large = list(range(5000))
        result_large = await MapReduce.map(
            broker,
            square,
            items_large,
            output="square",
            chunk_config=chunk_config,
            max_parallel=10,
        )

        assert len(result_large.results) == 5000
        assert result_large.success_rate == 1.0

    @pytest.mark.asyncio
    async def test_map_with_progress_callback(self, broker: InMemoryBroker) -> None:
        """Test map with progress callback."""

        @pipeline_task(output="value")
        @broker.task
        async def identity(x: int) -> int:
            return x

        items = list(range(20))
        progress_updates = []

        def progress_callback(done: int, total: int) -> None:
            progress_updates.append((done, total))

        result = await MapReduce.map(
            broker,
            identity,
            items,
            output="value",
            progress_callback=progress_callback,
            max_parallel=5,
        )

        assert len(result.results) == 20
        assert len(progress_updates) > 0
        # Last update should be (20, 20)
        assert progress_updates[-1] == (20, 20)

    @pytest.mark.asyncio
    async def test_map_sweep(self, broker: InMemoryBroker) -> None:
        """Test multi-dimensional map sweep."""

        @pipeline_task(output="sum")
        @broker.task
        async def add(x: int, y: int) -> int:
            return x + y

        param_values = {
            "x": [1, 2, 3],
            "y": [10, 20],
        }

        result = await MapReduce.map_sweep(
            broker,
            add,
            param_values,
            output="sum",
            max_parallel=5,
        )

        assert isinstance(result, MapResult)
        # 3 x values * 2 y values = 6 combinations
        assert result.items_processed == 6
        assert result.success_rate == 1.0
        # Results should be: 1+10, 1+20, 2+10, 2+20, 3+10, 3+20
        expected = [11, 21, 12, 22, 13, 23]
        assert sorted(result.results) == sorted(expected)

    @pytest.mark.asyncio
    async def test_map_sweep_with_kwargs(self, broker: InMemoryBroker) -> None:
        """Test map sweep with additional kwargs."""

        @pipeline_task(output="calc")
        @broker.task
        async def calculate(x: int, y: int, multiplier: int = 1) -> int:
            return (x + y) * multiplier

        param_values = {
            "x": [1, 2],
            "y": [10],
        }

        result = await MapReduce.map_sweep(
            broker,
            calculate,
            param_values,
            output="calc",
            multiplier=2,
            max_parallel=5,
        )

        assert result.items_processed == 2
        assert sorted(result.results) == [22, 24]  # (1+10)*2, (2+10)*2

    @pytest.mark.asyncio
    async def test_reduce_with_chunking(self, broker: InMemoryBroker) -> None:
        """Test reduce with chunked reduction."""

        @pipeline_task(output="sum")
        @broker.task
        async def sum_reduce(items: list[int], initial: int = 0) -> int:
            return sum(items) + initial

        items = list(range(1, 101))  # 1 to 100

        result = await MapReduce.reduce(
            broker,
            sum_reduce,
            items,
            output="sum",
            initial=0,
            chunk_size=30,
        )

        # Sum of 1 to 100 = 5050
        assert result == 5050

    @pytest.mark.asyncio
    async def test_pipeline_map_reduce_integration(
        self,
        broker: InMemoryBroker,
    ) -> None:
        """Test map-reduce integration with DataflowPipeline."""

        @pipeline_task(output="doubled")
        @broker.task
        async def double(x: int) -> int:
            return x * 2

        @pipeline_task(output="total")
        @broker.task
        async def sum_all(items: list[int], initial: int = 0) -> int:
            return sum(items) + initial

        items = list(range(1, 11))  # 1 to 10

        # Use kiq_map_reduce_advanced instead of from_tasks
        # since the tasks don't have dependencies for a DAG
        pipeline = DataflowPipeline(broker)
        result = await pipeline.kiq_map_reduce_advanced(
            double,
            sum_all,
            items,
            map_output="doubled",
            reduce_output="total",
            max_parallel=5,
        )

        # Sum of doubled 1-10 = 2+4+6+8+10+12+14+16+18+20 = 110
        assert result == 110

    @pytest.mark.asyncio
    async def test_map_empty_list(self, broker: InMemoryBroker) -> None:
        """Test map with empty list."""

        @pipeline_task(output="value")
        @broker.task
        async def process(x: int) -> int:
            return x

        result = await MapReduce.map(
            broker,
            process,
            [],
            output="value",
        )

        assert isinstance(result, MapResult)
        assert len(result.results) == 0
        assert result.items_processed == 0

    @pytest.mark.asyncio
    async def test_map_reduce_empty_list(self, broker: InMemoryBroker) -> None:
        """Test map-reduce with empty list."""

        @pipeline_task(output="value")
        @broker.task
        async def process(x: int) -> int:
            return x

        @pipeline_task(output="sum")
        @broker.task
        async def sum_reduce(items: list[int], initial: int = 0) -> int:
            return sum(items) + initial

        result = await MapReduce.map_reduce(
            broker,
            process,
            sum_reduce,
            [],
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_map_result_metadata(self, broker: InMemoryBroker) -> None:
        """Test MapResult metadata."""

        @pipeline_task(output="value")
        @broker.task
        async def process(x: int) -> int:
            return x * 2

        items = [1, 2, 3]

        result = await MapReduce.map(
            broker,
            process,
            items,
            output="test_output",
        )

        assert result.output_name == "test_output"
        assert result.items_processed == 3
        assert result.duration >= 0
        assert result.success_rate == 1.0
        assert result.errors == []
        assert result.results == [2, 4, 6]

    @pytest.mark.asyncio
    async def test_chunk_config_calculate_chunks(self) -> None:
        """Test ChunkConfig chunk calculation."""
        config = ChunkConfig(chunk_size=10)
        items = list(range(25))
        chunks = config.calculate_chunks(items)

        assert len(chunks) == 3  # 10, 10, 5
        assert len(chunks[0]) == 10
        assert len(chunks[1]) == 10
        assert len(chunks[2]) == 5

    @pytest.mark.asyncio
    async def test_chunk_config_max_chunks(self) -> None:
        """Test ChunkConfig with max_chunks limit."""
        config = ChunkConfig(chunk_size=5, max_chunks=3)
        items = list(range(100))
        chunks = config.calculate_chunks(items)

        assert len(chunks) == 3
        assert sum(len(c) for c in chunks) == 15  # Only first 15 items

    @pytest.mark.asyncio
    async def test_pipeline_map_reduce_advanced_integration(
        self,
        broker: InMemoryBroker,
    ) -> None:
        """Test map-reduce integration with DataflowPipeline."""

        @pipeline_task(output="doubled")
        @broker.task
        async def double(x: int) -> int:
            return x * 2

        @pipeline_task(output="total")
        @broker.task
        async def sum_all(items: list[int], initial: int = 0) -> int:
            return sum(items) + initial

        items = list(range(5))

        # Use kiq_map_reduce_advanced instead of from_tasks
        # since the tasks don't have dependencies for a DAG
        pipeline = DataflowPipeline(broker)
        result = await pipeline.kiq_map_reduce_advanced(
            double,
            sum_all,
            items,
            map_output="doubled",
            reduce_output="total",
        )

        # Sum of doubled 0-4 = 0+2+4+6+8 = 20
        assert result == 20


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
