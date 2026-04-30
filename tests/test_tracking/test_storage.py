# mypy: disable-error-code=no-untyped-def
"""Tests for pipeline storage interface."""

import pytest

from taskiq_pipelines.tracking.storage import PipelineStorage


def test_pipeline_storage_is_abstract():
    """Test that PipelineStorage is abstract and cannot be instantiated."""
    with pytest.raises(TypeError):
        PipelineStorage()


def test_pipeline_storage_has_abstract_methods():
    """Test that PipelineStorage defines expected abstract methods."""
    expected_methods = [
        "create_pipeline",
        "start_pipeline",
        "complete_pipeline",
        "fail_pipeline",
        "start_step",
        "complete_step",
        "fail_step",
        "get_pipeline_status",
        "list_pipelines",
        "cleanup_old",
    ]

    for method in expected_methods:
        assert hasattr(PipelineStorage, method), f"Missing abstract method: {method}"
