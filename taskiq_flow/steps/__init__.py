"""Package contenant les steps par défaut pour les pipelines.

Ce module expose les différents steps (sequential, mapper, filter,
group, branch, condition, reduce) et fournit la fonction parse_step
pour instancier un step à partir de sa représentation sérialisée.

Auteur: SoniqueBay Team
Version: 0.3.2
"""

from logging import getLogger
from typing import Any

from taskiq_flow.abc import AbstractStep
from taskiq_flow.steps.branch import BranchStep
from taskiq_flow.steps.condition import ConditionStep
from taskiq_flow.steps.filter import FilterStep
from taskiq_flow.steps.group import GroupStep
from taskiq_flow.steps.mapper import MapperStep
from taskiq_flow.steps.reduce import ReduceStep
from taskiq_flow.steps.sequential import SequentialStep

logger = getLogger(__name__)


def parse_step(step_type: str, step_data: dict[str, Any]) -> AbstractStep:
    step_cls = AbstractStep._known_steps.get(step_type)
    if step_cls is None:
        logger.warning(f"Unknown step type: {step_type}")
        raise ValueError("Unknown step type.")
    return step_cls(**step_data)


__all__ = [
    "BranchStep",
    "ConditionStep",
    "FilterStep",
    "GroupStep",
    "MapperStep",
    "ReduceStep",
    "SequentialStep",
]
