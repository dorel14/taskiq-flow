"""Tests for enhanced DAG builder functionality."""

from typing import Any

import pytest
from taskiq import InMemoryBroker

from taskiq_flow import pipeline_task
from taskiq_flow.dag_builder import DAGBuilder
from taskiq_flow.dataflow.dag import DAG
from taskiq_flow.dataflow.registry import DataflowRegistry


@pytest.fixture
def broker() -> InMemoryBroker:
    """Create a test broker."""
    return InMemoryBroker()


class TestDAGBuilderEnhancements:
    """Tests for enhanced DAG builder functionality."""

    def test_dag_builder_with_external_inputs(self, broker: InMemoryBroker) -> None:
        """Test DAG building with external inputs."""

        @broker.task
        @pipeline_task(output="processed")
        async def process_data(external_input: str) -> str:
            return f"processed_{external_input}"

        # Build DAG with external input
        dag = DAGBuilder.from_tasks(
            tasks=[process_data],
            external_inputs=["external_input"]
        )

        assert isinstance(dag, DAG)
        assert len(dag.nodes) == 1
        assert len(dag.edges) == 0  # No internal dependencies

    def test_dag_builder_external_input_conflict(self, broker: InMemoryBroker) -> None:
        """Test that external inputs cannot conflict with task outputs."""

        @broker.task
        @pipeline_task(output="data")
        async def produce_data() -> str:
            return "produced"

        @broker.task
        @pipeline_task(output="processed", inputs=["data"])
        async def process_data(data: str) -> str:
            return f"processed_{data}"

        # This should fail because "data" is a task output, not external
        with pytest.raises(ValueError, match="conflicts with task output"):
            DAGBuilder.from_tasks(
                tasks=[produce_data, process_data],
                external_inputs=["data"]  # Conflict!
            )

    def test_dag_validation_empty_dag(self) -> None:
        """Test validation of empty DAG."""
        dag = DAG()

        with pytest.raises(ValueError, match="DAG contains no nodes"):
            DAGBuilder.validate_dag(dag)

    def test_dag_validation_circular_dependency(self, broker: InMemoryBroker) -> None:
        """Test validation detects circular dependencies."""

        @broker.task
        @pipeline_task(output="a", inputs=["b"])
        async def task_a(b: str) -> str:
            return f"a_{b}"

        @broker.task
        @pipeline_task(output="b", inputs=["a"])
        async def task_b(a: str) -> str:
            return f"b_{a}"

        registry = DataflowRegistry()
        DAGBuilder._register_task(task_a, registry)
        DAGBuilder._register_task(task_b, registry)

        # Circular dependency should be detected during build_dag
        with pytest.raises(ValueError, match="Circular dependency detected"):
            registry.build_dag()

    def test_dag_validation_detailed_error_messages(self, broker: InMemoryBroker) -> None:
        """Test that validation provides detailed error messages."""

        # Create a valid DAG first
        @broker.task
        @pipeline_task(output="valid_output")
        async def valid_task() -> str:
            return "result"

        registry = DataflowRegistry()
        DAGBuilder._register_task(valid_task, registry)

        registry.build_dag()

        # Test validation of empty DAG
        empty_dag = DAG()
        with pytest.raises(ValueError) as exc_info:
            DAGBuilder.validate_dag(empty_dag)

        error_msg = str(exc_info.value)
        assert "no nodes" in error_msg

    def test_dag_validation_disconnected_components(self, broker: InMemoryBroker) -> None:
        """Test validation of disconnected components in DAG."""

        @broker.task
        @pipeline_task(output="a")
        async def task_a() -> str:
            return "a"

        @broker.task
        @pipeline_task(output="b")
        async def task_b() -> str:
            return "b"

        # Create a DAG with disconnected components
        registry = DataflowRegistry()
        DAGBuilder._register_task(task_a, registry)
        DAGBuilder._register_task(task_b, registry)

        dag = registry.build_dag()

        # This should now fail - disconnected components are not allowed
        with pytest.raises(ValueError, match="disconnected components"):
            DAGBuilder.validate_dag(dag)

    def test_analyze_missing_dependencies(self, broker: InMemoryBroker) -> None:
        """Test analysis of missing dependencies."""

        @broker.task
        @pipeline_task(output="a")
        async def task_a() -> str:
            return "a"

        @broker.task
        @pipeline_task(output="b", inputs=["missing1", "missing2"])
        async def task_b(missing1: str, missing2: str) -> str:
            return f"b_{missing1}_{missing2}"

        registry = DataflowRegistry()
        missing = DAGBuilder.analyze_missing_dependencies([task_a, task_b], registry)

        # Should contain task_b with missing dependencies
        assert len(missing) > 0
        task_names = list(missing.keys())
        assert any("task_b" in name for name in task_names)

        # Get the missing deps for task_b
        task_b_missing = None
        for task_name, deps in missing.items():
            if "task_b" in task_name:
                task_b_missing = deps
                break

        assert task_b_missing is not None
        assert "missing1" in task_b_missing
        assert "missing2" in task_b_missing

    def test_input_inference_from_signature(self, broker: InMemoryBroker) -> None:
        """Test input inference from function signatures."""

        @broker.task
        @pipeline_task(output="result")
        async def func_with_defaults(
            required_arg: str,
            optional_arg: str = "default",
            *args: Any,
            **kwargs: Any
        ) -> str:
            return f"{required_arg}_{optional_arg}"

        inferred = DAGBuilder._infer_inputs_from_callable(func_with_defaults)
        assert "required_arg" in inferred
        assert "optional_arg" not in inferred  # Has default
        assert "args" not in inferred  # *args
        assert "kwargs" not in inferred  # **kwargs

    def test_input_inference_skips_self_cls(self, broker: InMemoryBroker) -> None:
        """Test that input inference skips self/cls parameters."""

        class TestClass:
            @broker.task
            @pipeline_task(output="result")
            async def method(self, arg1: str, arg2: int) -> str:
                return f"{arg1}_{arg2}"

        inferred = DAGBuilder._infer_inputs_from_callable(TestClass().method)
        assert "self" not in inferred
        assert "arg1" in inferred
        assert "arg2" in inferred

    def test_validate_dag_with_valid_dag(self, broker: InMemoryBroker) -> None:
        """Test validation passes for valid DAG."""

        @broker.task
        @pipeline_task(output="a")
        async def task_a() -> str:
            return "a"

        @broker.task
        @pipeline_task(output="b", inputs=["a"])
        async def task_b(a: str) -> str:
            return f"b_{a}"

        registry = DataflowRegistry()
        DAGBuilder._register_task(task_a, registry)
        DAGBuilder._register_task(task_b, registry)

        dag = registry.build_dag()

        # Should not raise
        DAGBuilder.validate_dag(dag)

        # Check that levels were computed
        assert hasattr(dag, "levels")
        assert len(dag.levels) >= 2  # At least 2 levels

    def test_from_callable_tasks(self, broker: InMemoryBroker) -> None:
        """Test building DAG from callable (non-decorated) tasks."""

        @pipeline_task(output="a")
        async def task_a() -> str:
            return "a"

        @pipeline_task(output="b", inputs=["a"])
        async def task_b(a: str) -> str:
            return f"b_{a}"

        dag = DAGBuilder.from_callable_tasks([task_a, task_b])

        assert isinstance(dag, DAG)
        assert len(dag.nodes) == 2
        assert len(dag.edges) == 1

        # Validate the DAG
        DAGBuilder.validate_dag(dag)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
