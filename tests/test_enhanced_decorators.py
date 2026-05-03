"""Tests for enhanced pipeline task decorators."""

from typing import Any

import pytest
from taskiq import InMemoryBroker

from taskiq_flow import (
    get_all_pipeline_outputs,
    get_pipeline_metadata,
    get_task_by_output,
    get_task_outputs,
    is_pipeline_task,
    pipeline_task,
    pipeline_task_multi_output,
)
from taskiq_flow.decorators import PipelineTaskMetadata, _task_registry


@pytest.fixture
def broker() -> InMemoryBroker:
    """Create a test broker."""
    return InMemoryBroker()


@pytest.fixture(autouse=True)
def clean_registry() -> None:
    """Clean the global registry before each test."""
    _task_registry.clear()


class TestPipelineTaskDecorator:
    """Tests for the enhanced @pipeline_task decorator."""

    def test_basic_pipeline_task(self, broker: InMemoryBroker) -> None:
        """Test basic pipeline task decoration."""

        @broker.task
        @pipeline_task(output="result")
        async def add_one(value: int) -> int:
            return value + 1

        assert is_pipeline_task(add_one)
        metadata = get_pipeline_metadata(add_one)
        assert metadata["output"] == "result"
        assert metadata["retries"] == 0
        assert metadata["is_pipeline_task"] is True

    def test_pipeline_task_with_retries(self, broker: InMemoryBroker) -> None:
        """Test pipeline task with custom retry count."""

        @broker.task
        @pipeline_task(output="result", retries=3)
        async def failing_task(value: int) -> int:
            return value * 2

        metadata = get_pipeline_metadata(failing_task)
        assert metadata["retries"] == 3

    def test_pipeline_task_with_explicit_inputs(self, broker: InMemoryBroker) -> None:
        """Test pipeline task with explicit input specification."""

        @broker.task
        @pipeline_task(output="sum", inputs=["a", "b"])
        async def add_two(a: int, b: int) -> int:
            return a + b

        metadata = get_pipeline_metadata(add_two)
        assert metadata["inputs"] == ["a", "b"]

    def test_pipeline_task_input_inference(self, broker: InMemoryBroker) -> None:
        """Test automatic input inference from function signature."""

        @broker.task
        @pipeline_task(output="product")
        async def multiply(x: int, y: int, config: dict[str, Any] | None = None) -> int:
            # config has default, so shouldn't be inferred as input
            return x * y

        metadata = get_pipeline_metadata(multiply)
        # Should only infer x and y, not config (has default)
        assert "x" in metadata["inputs"]
        assert "y" in metadata["inputs"]
        assert "config" not in metadata["inputs"]

    def test_duplicate_output_validation(self, broker: InMemoryBroker) -> None:
        """Test that duplicate output names raise an error."""

        @broker.task
        @pipeline_task(output="duplicate")
        async def task1() -> int:
            return 1

        @broker.task
        @pipeline_task(output="duplicate")
        async def task2() -> int:
            return 2

        # This should raise an error when validating
        with pytest.raises(ValueError, match="Duplicate output name"):
            _task_registry.validate_outputs()

    def test_sync_function_support(self, broker: InMemoryBroker) -> None:
        """Test that sync functions are supported."""

        @broker.task
        @pipeline_task(output="sync_result")
        def sync_task(value: int) -> int:
            return value + 10

        assert is_pipeline_task(sync_task)
        metadata = get_pipeline_metadata(sync_task)
        assert metadata["output"] == "sync_result"


class TestPipelineTaskMultiOutput:
    """Tests for multi-output pipeline tasks."""

    def test_multi_output_task(self, broker: InMemoryBroker) -> None:
        """Test multi-output pipeline task."""

        @broker.task
        @pipeline_task_multi_output(outputs={"data": dict, "stats": dict}, retries=1)
        async def process_multi(value: int) -> dict[str, Any]:
            return {"data": {"value": value}, "stats": {"count": 1}}

        assert is_pipeline_task(process_multi)
        metadata = get_pipeline_metadata(process_multi)
        assert metadata["output"] == "data"  # First output is primary
        assert metadata["multiple_outputs"] is True
        assert metadata["retries"] == 1

        # Check outputs
        outputs = get_task_outputs(process_multi)
        assert "data" in outputs
        assert "stats" in outputs

    def test_multi_output_registry(self, broker: InMemoryBroker) -> None:
        """Test that multi-output tasks register all outputs."""

        @broker.task
        @pipeline_task_multi_output(outputs={"features": list, "metadata": dict})
        async def extract_features(path: str) -> dict[str, Any]:
            return {"features": [1, 2, 3], "metadata": {"duration": 180}}

        # Check that both outputs are registered
        assert get_task_by_output("features") is not None
        assert get_task_by_output("metadata") is not None

        # Check all outputs
        all_outputs = get_all_pipeline_outputs()
        assert "features" in all_outputs
        assert "metadata" in all_outputs


class TestRegistryOperations:
    """Tests for registry operations."""

    def test_get_all_pipeline_outputs(self, broker: InMemoryBroker) -> None:
        """Test getting all registered outputs."""

        @broker.task
        @pipeline_task(output="output1")
        async def task1() -> int:
            return 1

        @broker.task
        @pipeline_task(output="output2")
        async def task2() -> str:
            return "hello"

        outputs = get_all_pipeline_outputs()
        assert "output1" in outputs
        assert "output2" in outputs

    def test_get_task_by_output(self, broker: InMemoryBroker) -> None:
        """Test finding task by output name."""

        @broker.task
        @pipeline_task(output="special_output")
        async def special_task() -> int:
            return 42

        task = get_task_by_output("special_output")
        assert task is not None
        # TaskIQ modifies the function name, so we check the original name
        assert "special_task" in task.__name__

    def test_validate_outputs_no_conflicts(self, broker: InMemoryBroker) -> None:
        """Test output validation with no conflicts."""

        @broker.task
        @pipeline_task(output="unique1")
        async def task1() -> int:
            return 1

        @broker.task
        @pipeline_task(output="unique2")
        async def task2() -> int:
            return 2

        # Should not raise
        _task_registry.validate_outputs()

class TestErrorHandling:
    """Tests for error handling in decorators."""

    def test_invalid_retries_negative(self) -> None:
        """Test that negative retries raise an error."""
        with pytest.raises(ValueError, match="Retries must be non-negative"):
            PipelineTaskMetadata(output="test", retries=-1)

    def test_invalid_output_empty(self) -> None:
        """Test that empty output name raises an error."""
        with pytest.raises(ValueError, match="must specify an output name"):
            PipelineTaskMetadata(output="", retries=0)

    def test_invalid_inputs_type(self) -> None:
        """Test that invalid inputs type raises an error."""
        with pytest.raises(ValueError, match="Inputs must be a list"):
            PipelineTaskMetadata(output="test", inputs="invalid")  # type: ignore


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
