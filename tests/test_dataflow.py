"""Tests for dataflow-based pipeline features."""

from typing import Any

import pytest
from taskiq import InMemoryBroker

from taskiq_flow import (
    DAGBuilder,
    DataflowPipeline,
    DataflowRegistry,
    pipeline_task,
)


@pytest.fixture
def broker() -> InMemoryBroker:
    """Create a test broker."""
    return InMemoryBroker()


@pytest.fixture
def registry() -> DataflowRegistry:
    """Create a test registry."""
    return DataflowRegistry()


# ============================================================================
# Test DataflowRegistry
# ============================================================================


class TestDataflowRegistry:
    """Tests for DataflowRegistry."""

    def test_registry_creation(self, registry: DataflowRegistry) -> None:
        """Test registry creation."""
        assert len(registry.tasks) == 0
        assert len(registry.data_nodes) == 0

    def test_register_task(
        self,
        registry: DataflowRegistry,
        broker: InMemoryBroker,
    ) -> None:
        """Test registering a task."""

        @pipeline_task(output="output1")
        @broker.task
        async def task1(input1: str) -> str:
            return input1

        registry.register_task(
            task1,
            output="output1",
            inputs=["input1"],
        )

        assert len(registry.tasks) == 1
        assert "output1" in registry.data_nodes
        assert registry.data_producers["output1"] == task1

    def test_register_task_with_dependencies(
        self,
        registry: DataflowRegistry,
        broker: InMemoryBroker,
    ) -> None:
        """Test registering tasks with dependencies."""

        @pipeline_task(output="output1")
        @broker.task
        async def task1(input1: str) -> str:
            return input1

        @pipeline_task(output="output2")
        @broker.task
        async def task2(output1: str) -> str:
            return output1

        registry.register_task(task1, output="output1", inputs=["input1"])
        registry.register_task(task2, output="output2", inputs=["output1"])

        assert len(registry.tasks) == 2
        assert "output1" in registry.data_nodes
        assert "output2" in registry.data_nodes

        # Check that output1 has a consumer (task2)
        consumers = registry.get_consumers("output1")
        assert task2 in consumers

    def test_get_producer(
        self,
        registry: DataflowRegistry,
        broker: InMemoryBroker,
    ) -> None:
        """Test getting producer for data."""

        @broker.task
        @pipeline_task(output="output1")
        async def task1(input1: str) -> str:
            return input1

        registry.register_task(task1, output="output1", inputs=["input1"])

        producer = registry.get_producer("output1")
        assert producer == task1

        # Non-existent data
        assert registry.get_producer("nonexistent") is None

    def test_build_dag(
        self,
        registry: DataflowRegistry,
        broker: InMemoryBroker,
    ) -> None:
        """Test building DAG from registered tasks."""

        @pipeline_task(output="output1")
        @broker.task
        async def task1(input1: str) -> str:
            return input1

        @pipeline_task(output="output2")
        @broker.task
        async def task2(output1: str) -> str:
            return output1

        registry.register_task(task1, output="output1", inputs=["input1"])
        registry.register_task(task2, output="output2", inputs=["output1"])

        dag = registry.build_dag()

        assert len(dag.nodes) == 2
        assert len(dag.edges) == 1

        # Check edge direction
        from_node, to_node = dag.edges[0]
        assert from_node.task == task1
        assert to_node.task == task2

    def test_build_dag_parallel(
        self,
        registry: DataflowRegistry,
        broker: InMemoryBroker,
    ) -> None:
        """Test building DAG with parallel tasks."""

        @pipeline_task(output="output1")
        @broker.task
        async def task1(input1: str) -> str:
            return input1

        @pipeline_task(output="output2")
        @broker.task
        async def task2(input2: str) -> str:
            return input2

        @pipeline_task(output="output3")
        @broker.task
        async def task3(output1: str, output2: str) -> str:
            return output1 + output2

        registry.register_task(task1, output="output1", inputs=["input1"])
        registry.register_task(task2, output="output2", inputs=["input2"])
        registry.register_task(task3, output="output3", inputs=["output1", "output2"])

        dag = registry.build_dag()

        assert len(dag.nodes) == 3
        assert len(dag.edges) == 2

        # Check levels
        dag.compute_levels()
        # task1 and task2 are at level 0, task3 at level 1
        # (input1/input2 are external, not in DAG)
        assert len(dag.levels) == 2
        assert len(dag.levels[0]) == 2  # task1 and task2 can run in parallel
        assert len(dag.levels[1]) == 1  # task3 depends on both

    def test_external_inputs(
        self,
        registry: DataflowRegistry,
        broker: InMemoryBroker,
    ) -> None:
        """Test detection of external inputs."""

        @broker.task
        @pipeline_task(output="output1")
        async def task1(input1: str) -> str:
            return input1

        registry.register_task(task1, output="output1", inputs=["input1"])

        external = registry.get_external_inputs()
        assert "input1" in external


# ============================================================================
# Test DAG
# ============================================================================


class TestDAG:
    """Tests for DAG."""

    def test_topological_sort(self, broker: InMemoryBroker) -> None:
        """Test topological sorting."""

        @pipeline_task(output="output1")
        @broker.task
        async def task1(input1: str) -> str:
            return input1

        @pipeline_task(output="output2")
        @broker.task
        async def task2(output1: str) -> str:
            return output1

        registry = DataflowRegistry()
        registry.register_task(task1, output="output1", inputs=["input1"])
        registry.register_task(task2, output="output2", inputs=["output1"])

        dag = registry.build_dag()
        sorted_nodes = dag.topological_sort()

        assert len(sorted_nodes) == 2
        assert sorted_nodes[0].task == task1
        assert sorted_nodes[1].task == task2

    def test_compute_levels(self, broker: InMemoryBroker) -> None:
        """Test level computation for parallel execution."""

        @pipeline_task(output="output1")
        @broker.task
        async def task1(input1: str) -> str:
            return input1

        @pipeline_task(output="output2")
        @broker.task
        async def task2(input2: str) -> str:
            return input2

        @pipeline_task(output="output3")
        @broker.task
        async def task3(output1: str, output2: str) -> str:
            return output1 + output2

        registry = DataflowRegistry()
        registry.register_task(task1, output="output1", inputs=["input1"])
        registry.register_task(task2, output="output2", inputs=["input2"])
        registry.register_task(task3, output="output3", inputs=["output1", "output2"])

        dag = registry.build_dag()
        dag.compute_levels()

        # task1 and task2 are at level 0, task3 at level 1
        # (input1/input2 are external, not in DAG)
        assert len(dag.levels) == 2
        assert len(dag.levels[0]) == 2  # task1, task2 (parallel)
        assert len(dag.levels[1]) == 1  # task3

    def test_circular_dependency(self, broker: InMemoryBroker) -> None:
        """Test detection of circular dependencies."""

        @pipeline_task(output="output1")
        @broker.task
        async def task1(output2: str) -> str:
            return output2

        @pipeline_task(output="output2")
        @broker.task
        async def task2(output1: str) -> str:
            return output1

        registry = DataflowRegistry()
        registry.register_task(task1, output="output1", inputs=["output2"])
        registry.register_task(task2, output="output2", inputs=["output1"])

        with pytest.raises(ValueError, match="Circular dependency"):
            registry.build_dag()


# ============================================================================
# Test DAGBuilder
# ============================================================================


class TestDAGBuilder:
    """Tests for DAGBuilder."""

    def test_from_tasks(self, broker: InMemoryBroker) -> None:
        """Test building DAG from tasks."""

        @pipeline_task(output="output1")
        @broker.task
        async def task1(input1: str) -> str:
            return input1

        @pipeline_task(output="output2")
        @broker.task
        async def task2(output1: str) -> str:
            return output1

        dag = DAGBuilder.from_tasks([task1, task2])

        assert len(dag.nodes) == 2
        assert len(dag.edges) == 1

    def test_infer_inputs(self, broker: InMemoryBroker) -> None:
        """Test inferring inputs from function signature."""

        @pipeline_task(output="output1")
        @broker.task
        async def task1(input1: str, input2: str) -> str:
            return input1 + input2

        metadata = DAGBuilder._infer_inputs(task1)
        assert "input1" in metadata
        assert "input2" in metadata

    def test_validate_dag(self, broker: InMemoryBroker) -> None:
        """Test DAG validation."""

        @broker.task
        @pipeline_task(output="output1")
        async def task1(input1: int) -> int:
            return input1

        dag = DAGBuilder.from_tasks([task1])

        # Should not raise
        DAGBuilder.validate_dag(dag)


# ============================================================================
# Test DataflowPipeline
# ============================================================================


class TestDataflowPipeline:
    """Tests for DataflowPipeline."""

    def test_create_pipeline(self, broker: InMemoryBroker) -> None:
        """Test creating a dataflow pipeline."""

        @pipeline_task(output="output1")
        @broker.task
        async def task1(input1: str) -> str:
            return input1

        pipeline = DataflowPipeline.from_tasks(broker, [task1])

        assert pipeline._dag is not None
        assert len(pipeline._dag.nodes) == 1

    def test_visualize(self, broker: InMemoryBroker) -> None:
        """Test pipeline visualization."""

        @pipeline_task(output="output1")
        @broker.task
        async def task1(input1: int) -> int:
            return input1

        @pipeline_task(output="output2")
        @broker.task
        async def task2(output1: int) -> int:
            return output1

        pipeline = DataflowPipeline.from_tasks(broker, [task1, task2])

        viz = pipeline.visualize()

        assert "nodes" in viz
        assert "edges" in viz
        assert len(viz["nodes"]) == 2
        assert len(viz["edges"]) == 1

    def test_visualize_dot(self, broker: InMemoryBroker) -> None:
        """Test DOT format visualization."""

        @pipeline_task(output="output1")
        @broker.task
        async def task1(input1: int) -> int:
            return input1

        pipeline = DataflowPipeline.from_tasks(broker, [task1])

        dot = pipeline.visualize_dot()

        assert "digraph" in dot
        assert "task1" in dot

    def test_print_dag(self, broker: InMemoryBroker, capsys: Any) -> None:
        """Test printing DAG."""

        @pipeline_task(output="output1")
        @broker.task
        async def task1(input1: int) -> int:
            return input1

        pipeline = DataflowPipeline.from_tasks(broker, [task1])

        pipeline.print_dag()
        captured = capsys.readouterr()

        assert "Pipeline DAG" in captured.out
        assert "task1" in captured.out


# ============================================================================
# Test Integration
# ============================================================================


class TestIntegration:
    """Integration tests for dataflow pipeline."""

    def test_full_pipeline(self, broker: InMemoryBroker) -> None:
        """Test full pipeline with dependencies."""

        @pipeline_task(output="doubled")
        @broker.task
        async def double(x: int) -> int:
            return x * 2

        @pipeline_task(output="squared")
        @broker.task
        async def square(doubled: int) -> int:
            return doubled**2

        pipeline = DataflowPipeline.from_tasks(broker, [double, square])

        assert pipeline._dag is not None
        # Verify DAG structure
        assert len(pipeline._dag.nodes) == 2
        assert len(pipeline._dag.edges) == 1

        # Verify levels
        pipeline._dag.compute_levels()
        assert len(pipeline._dag.levels) == 2

    def test_parallel_pipeline(self, broker: InMemoryBroker) -> None:
        """Test pipeline with parallel tasks."""

        @pipeline_task(output="a")
        @broker.task
        async def task_a(x: int) -> int:
            return x + 1

        @pipeline_task(output="b")
        @broker.task
        async def task_b(x: int) -> int:
            return x + 2

        @pipeline_task(output="c")
        @broker.task
        async def task_c(a: int, b: int) -> int:
            return a + b

        pipeline = DataflowPipeline.from_tasks(broker, [task_a, task_b, task_c])

        assert pipeline._dag is not None
        # Verify DAG structure
        pipeline._dag.compute_levels()

        # task_a and task_b should be in same level (parallel)
        assert len(pipeline._dag.levels[0]) == 2
        # task_c should be in next level
        assert len(pipeline._dag.levels[1]) == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
