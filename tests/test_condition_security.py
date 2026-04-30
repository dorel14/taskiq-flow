"""Tests for condition step security."""

from taskiq_pipelines.steps.condition import ConditionStep


def test_safe_condition_evaluation():
    """Test that safe conditions work correctly."""
    step = ConditionStep(condition="", task=None)  # Dummy values for testing

    # Test basic comparisons
    assert step._eval_condition("value > 5", 10) is True
    assert step._eval_condition("value < 5", 10) is False
    assert step._eval_condition("value == 5", 5) is True

    # Test safe function calls
    assert step._eval_condition("len(value) > 2", [1, 2, 3]) is True
    assert step._eval_condition("bool(value)", 0) is False
    assert step._eval_condition("str(value) == 'test'", "test") is True


def test_unsafe_condition_evaluation():
    """Test that unsafe expressions are rejected."""
    step = ConditionStep(condition="", task=None)  # Dummy values for testing

    # Test dangerous expressions are rejected
    assert step._eval_condition("__import__('os').system('echo test')", None) is False
    assert step._eval_condition("exec('print(1)')", None) is False
    assert step._eval_condition("eval('1+1')", None) is False

    # Test unsupported operations are rejected
    assert step._eval_condition("value.__class__", None) is False
    assert step._eval_condition("globals()", None) is False

    # Test multiple variables are rejected
    assert step._eval_condition("x + y", {"x": 1, "y": 2}) is False


def test_condition_edge_cases():
    """Test edge cases in condition evaluation."""
    step = ConditionStep(condition="", task=None)  # Dummy values for testing

    # Test malformed expressions
    assert step._eval_condition("not valid syntax", None) is False
    assert step._eval_condition("", None) is False

    # Test with None value
    assert step._eval_condition("value is None", None) is True
    assert step._eval_condition("value is not None", 5) is True


def test_condition_with_complex_values():
    """Test conditions with complex data structures."""
    step = ConditionStep(condition="", task=None)  # Dummy values for testing

    # Test with dictionaries
    data = {"count": 5, "active": True}
    assert step._eval_condition("value['count'] > 3", data) is True
    assert step._eval_condition("value.get('active', False)", data) is True

    # Test with nested access (should fail safely)
    assert step._eval_condition("value['missing']", {}) is False
