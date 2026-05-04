"""Exceptions spécifiques à taskiq-flow.

Ce module définit la hiérarchie des exceptions utilisées dans
taskiq-flow, including:
- PipelineError: Erreur générique de pipeline
- StepError: Erreur lors d'une étape
- AbortPipeline: Interruption volontaire du pipeline
- MappingError, FilterError: Erreurs spécifiques aux steps

Auteur: SoniqueBay Team
Version: 0.3.2
"""

from typing import ClassVar

from taskiq import TaskiqError


class PipelineError(TaskiqError):
    """Generic pipeline error."""


class StepError(PipelineError):
    """Error found while mapping step."""

    __template__ = (
        "Task {task_id} returned an error. {_STEP_NAME} failed. Reason: {error}"
    )
    _STEP_NAME: ClassVar[str]

    task_id: str
    error: BaseException | None


class MappingError(StepError):
    """Error found while mapping step."""

    _STEP_NAME = "mapping"


class FilterError(StepError):
    """Error found while filtering step."""

    _STEP_NAME = "filtering"


class AbortPipeline(PipelineError):  # noqa: N818
    """
    Abort current pipeline execution.

    This error can be thrown from
    act method of a step.

    It immediately aborts current pipeline
    execution.
    """

    __template__ = "Pipeline was aborted. {reason}"

    reason: str = "No reason provided."
