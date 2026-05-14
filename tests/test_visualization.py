"""
Tests for DAG visualization features.

This module tests the DAG visualization capabilities including
JSON export, DOT format, NetworkX conversion, critical path detection,
and parallel group identification.

Author: SoniqueBay Team
Version: 0.4.5
"""

import pytest
from taskiq import InMemoryBroker

from taskiq_flow import DAGBuilder, DataflowPipeline, pipeline_task
from taskiq_flow.dataflow.dag import DAG
from taskiq_flow.visualization import DAGVisualizer, MermaidGenerator


@pytest.fixture
def broker() -> InMemoryBroker:
    """Create a test broker."""
    return InMemoryBroker()


@pytest.fixture
def simple_dag(broker: InMemoryBroker) -> DAG:
    """Create a simple DAG for testing."""

    @pipeline_task(output="output1")
    @broker.task
    async def task1(input1: str) -> str:
        return f"processed_{input1}"

    @pipeline_task(output="output2")
    @broker.task
    async def task2(output1: str) -> str:
        return f"final_{output1}"

    return DAGBuilder.from_tasks([task1, task2])


@pytest.fixture
def parallel_dag(broker: InMemoryBroker) -> DAG:
    """Create a DAG with parallel tasks for testing."""

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

    return DAGBuilder.from_tasks([task_a, task_b, task_c])


@pytest.fixture
def complex_dag(broker: InMemoryBroker) -> DAG:
    """Create a complex DAG for critical path testing."""

    @pipeline_task(output="a")
    @broker.task
    async def task_a() -> int:
        return 1

    @pipeline_task(output="b")
    @broker.task
    async def task_b(a: int) -> int:
        return a + 1

    @pipeline_task(output="c")
    @broker.task
    async def task_c(a: int) -> int:
        return a + 2

    @pipeline_task(output="d")
    @broker.task
    async def task_d(b: int, c: int) -> int:
        return b + c

    @pipeline_task(output="e")
    @broker.task
    async def task_e(d: int) -> int:
        return d * 2

    return DAGBuilder.from_tasks([task_a, task_b, task_c, task_d, task_e])


class TestDAGVisualizer:
    """Tests for DAGVisualizer class."""

    def test_to_json(self, simple_dag: DAG) -> None:
        """Test JSON export."""
        result = DAGVisualizer.to_json(simple_dag)

        assert "nodes" in result
        assert "edges" in result
        assert "levels" in result
        assert len(result["nodes"]) == 2
        assert len(result["edges"]) == 1
        assert len(result["levels"]) == 2

    def test_to_json_extended(self, simple_dag: DAG) -> None:
        """Test extended JSON export."""
        result = DAGVisualizer.to_json_extended(simple_dag)

        assert "nodes" in result
        assert "edges" in result
        assert "is_cyclic" in result
        assert "node_count" in result
        assert "edge_count" in result
        assert "level_count" in result
        assert result["is_cyclic"] is False
        assert result["node_count"] == 2
        assert result["edge_count"] == 1

    def test_to_dot(self, simple_dag: DAG) -> None:
        """Test DOT format export."""
        result = DAGVisualizer.to_dot(simple_dag)

        assert "digraph" in result
        assert "task1" in result
        assert "task2" in result

    def test_to_cytoscape_json(self, simple_dag: DAG) -> None:
        """Test Cytoscape.js format export."""
        result = DAGVisualizer.to_cytoscape_json(simple_dag)

        assert "nodes" in result
        assert "edges" in result
        assert len(result["nodes"]) == 2
        assert len(result["edges"]) == 1

    def test_detect_critical_path(self, complex_dag: DAG) -> None:
        """Test critical path detection."""
        result = DAGVisualizer.detect_critical_path(complex_dag)

        assert isinstance(result, list)
        assert len(result) > 0
        # Critical path should include task names (they include module prefix)
        assert any("task_a" in task for task in result)
        assert any("task_e" in task for task in result)

    def test_find_parallelizable_groups(self, parallel_dag: DAG) -> None:
        """Test parallel group detection."""
        result = DAGVisualizer.find_parallelizable_groups(parallel_dag)

        assert isinstance(result, list)
        assert len(result) >= 2
        # First level should have task_a and task_b (parallel)
        assert len(result[0]) == 2
        # Second level should have task_c
        assert len(result[1]) == 1

    def test_to_networkx(self, simple_dag: DAG) -> None:
        """Test NetworkX conversion."""
        result = DAGVisualizer.to_networkx(simple_dag)

        assert hasattr(result, "nodes")
        assert hasattr(result, "edges")
        assert len(list(result.nodes())) == 2
        assert len(list(result.edges())) == 1

    def test_print_ascii(
        self, simple_dag: DAG, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Test ASCII printing."""
        DAGVisualizer.print_ascii(simple_dag)
        captured = capsys.readouterr()

        assert "Pipeline DAG" in captured.out
        assert "task1" in captured.out
        assert "task2" in captured.out


class TestMermaidGenerator:
    """Tests for MermaidGenerator class."""

    def test_to_mermaid(self, simple_dag: DAG) -> None:
        """Test Mermaid code generation."""
        generator = MermaidGenerator(simple_dag)
        result = generator.to_mermaid()

        assert "flowchart" in result
        assert "task1" in result
        assert "task2" in result

    def test_to_mermaid_with_styling(self, simple_dag: DAG) -> None:
        """Test styled Mermaid generation."""
        generator = MermaidGenerator(simple_dag)
        result = generator.to_mermaid_with_styling()

        assert "flowchart" in result
        assert "classDef" in result
        assert "task1" in result

    def test_to_mermaid_interactive(self, simple_dag: DAG) -> None:
        """Test interactive Mermaid generation."""
        generator = MermaidGenerator(simple_dag)
        result = generator.to_mermaid_interactive()

        assert "<script>" in result
        assert "mermaid" in result
        assert "task1" in result


class TestDAGVisualizationIntegration:
    """Integration tests for DAG visualization."""

    def test_pipeline_visualize(self, broker: InMemoryBroker) -> None:
        """Test pipeline visualization method."""

        @pipeline_task(output="result")
        @broker.task
        async def task1(input1: int) -> int:
            return input1

        pipeline = DataflowPipeline.from_tasks(broker, [task1])
        viz = pipeline.visualize()

        assert "nodes" in viz
        assert "edges" in viz
        assert len(viz["nodes"]) == 1

    def test_pipeline_visualize_dot(self, broker: InMemoryBroker) -> None:
        """Test pipeline DOT visualization."""

        @pipeline_task(output="result")
        @broker.task
        async def task1(input1: int) -> int:
            return input1

        pipeline = DataflowPipeline.from_tasks(broker, [task1])
        dot = pipeline.visualize_dot()

        assert "digraph" in dot
        assert "task1" in dot

    def test_pipeline_print_dag(
        self, broker: InMemoryBroker, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Test pipeline DAG printing."""

        @pipeline_task(output="result")
        @broker.task
        async def task1(input1: int) -> int:
            return input1

        pipeline = DataflowPipeline.from_tasks(broker, [task1])
        pipeline.print_dag()
        captured = capsys.readouterr()

        assert "Pipeline DAG" in captured.out
        assert "task1" in captured.out


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
