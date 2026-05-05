"""Décorateurs pour les tâches de pipeline.

Fournit @pipeline_task et @pipeline_task_multi_output pour déclarer
les dépendances de données des tâches. Ces décorateurs enregistrent
les métadonnées nécessaires à la construction automatique du DAG.

Auteur: SoniqueBay Team
Version: 0.3.2
"""

import inspect
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import wraps
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class PipelineTaskMetadata:
    """Metadata for a pipeline task."""

    output: str
    inputs: list[str] | None = None
    retries: int = 0
    task_name: str = ""
    is_pipeline_task: bool = True
    multiple_outputs: bool = False
    output_types: dict[str, Any] = field(default_factory=dict)
    resources: dict[str, Any] = field(default_factory=dict)
    retry_delay: float = 1.0
    retry_backoff: float = 2.0
    retry_jitter: bool = True
    max_retry_time: int = 300

    def __post_init__(self) -> None:
        """Validate metadata after initialization."""
        if not self.output:
            raise ValueError("Pipeline task must specify an output name")

        if self.retries < 0:
            raise ValueError("Retries must be non-negative")

        if self.inputs is not None and not isinstance(self.inputs, list):
            raise ValueError("Inputs must be a list of strings or None")

        if self.retry_delay < 0:
            raise ValueError("Retry delay must be non-negative")

        if self.retry_backoff < 1:
            raise ValueError("Retry backoff must be at least 1")


class PipelineTaskRegistry:
    """Registry for pipeline task metadata."""

    def __init__(self) -> None:
        self._tasks: dict[Any, PipelineTaskMetadata] = {}
        self._outputs: dict[str, Any] = {}

    def register_task(
        self,
        task: Any,
        metadata: PipelineTaskMetadata,
    ) -> None:
        """Register a task with its metadata."""
        self._tasks[task] = metadata
        self._outputs[metadata.output] = task

    def get_metadata(self, task: Any) -> PipelineTaskMetadata | None:
        """Get metadata for a task."""
        return self._tasks.get(task)

    def get_task_by_output(self, output_name: str) -> Any | None:
        """Get task that produces the given output."""
        return self._outputs.get(output_name)

    def get_all_outputs(self) -> list[str]:
        """Get all registered output names."""
        return list(self._outputs.keys())

    def validate_outputs(self) -> None:
        """Validate all registered outputs for conflicts."""
        # Check for duplicate output names across different tasks
        output_to_task: dict[Any, Any] = {}
        for task, metadata in self._tasks.items():
            if metadata.output in output_to_task:
                existing_task = output_to_task[metadata.output]
                if existing_task != task:
                    raise ValueError(
                        f"Duplicate output name '{metadata.output}' used by "
                        f"different tasks",
                    )
            output_to_task[metadata.output] = task

    def clear(self) -> None:
        """Clear all registered tasks and outputs."""
        self._tasks.clear()
        self._outputs.clear()


# Global registry instance
_task_registry: PipelineTaskRegistry = PipelineTaskRegistry()


def pipeline_task(
    output: str,
    inputs: list[str] | None = None,
    retries: int = 0,
    retry_delay: float = 1.0,
    retry_backoff: float = 2.0,
    retry_jitter: bool = True,
    max_retry_time: int = 300,
    resources: dict[str, Any] | None = None,
) -> Callable[..., Any]:
    """
    Décorateur pour marquer une fonction comme tâche de pipeline.

    Ce décorateur attache les métadonnées nécessaires à l'analyse
    automatique des dépendances de données dans les pipelines
    de type DataflowPipeline.

    Le décorateur enregistre la tâche dans un registre global avec:
    - Le nom de la sortie produite (output)
    - La liste des entrées requises (inputs, optionnel, inféré si omis)
    - Le nombre de tentatives en cas d'échec (retries)
    - Les paramètres de retry (delay, backoff, jitter, max_retry_time)
    - Les ressources estimées (memory, cpu)

    Args:
        output: Nom du flux de données produit par cette tâche.
                Ce nom sera utilisé pour lier les dépendances.
                Exemple: "audio_features", "mir_features"
        inputs: Liste des noms de flux de données consommés.
                Si None (défaut), les noms sont inférés depuis
                les paramètres de la fonction.
                Exemple: ["track_paths", "config"]
        retries: Nombre de tentatives en cas d'échec (défaut: 0)
        retry_delay: Délai initial entre les retries en secondes (défaut: 1.0)
        retry_backoff: Multiplicateur pour le délai entre les retries (défaut: 2.0)
        retry_jitter: Ajouter un jitter aléatoire au délai (défaut: True)
        max_retry_time: Temps maximum en secondes pour tous les retries (défaut: 300)
        resources: Dictionnaire avec les ressources estimées:
                   {"estimated_memory_mb": 500, "estimated_cpu_cores": 1.0}

    Returns:
        Décorateur qui transforme la fonction en tâche de pipeline
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        # Infer inputs if not provided
        inferred_inputs = inputs
        if inferred_inputs is None:
            inferred_inputs = _infer_inputs_from_signature(func)

        # Create metadata
        metadata = PipelineTaskMetadata(
            output=output,
            inputs=inferred_inputs,
            retries=retries,
            task_name=getattr(func, "__name__", str(func)),
            resources=resources or {},
            retry_delay=retry_delay,
            retry_backoff=retry_backoff,
            retry_jitter=retry_jitter,
            max_retry_time=max_retry_time,
        )

        # Choose appropriate wrapper based on function type
        if _is_async_function(func):

            @wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                return await func(*args, **kwargs)

            wrapper = async_wrapper
        else:

            @wraps(func)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                return func(*args, **kwargs)

            wrapper = sync_wrapper

        # Register the task (only the wrapper to avoid conflicts)
        _task_registry.register_task(wrapper, metadata)

        # Attach metadata to wrapper for easy access
        wrapper._pipeline_metadata = metadata  # type: ignore

        logger.info(
            "Pipeline task registered",
            extra={
                "task_name": metadata.task_name,
                "output": metadata.output,
                "inputs": metadata.inputs,
                "retries": metadata.retries,
                "retry_delay": metadata.retry_delay,
                "retry_backoff": metadata.retry_backoff,
                "is_async": _is_async_function(func),
            },
        )

        return wrapper

    return decorator


def get_pipeline_metadata(func: Any) -> dict[str, Any]:
    """
    Récupère les métadonnées de pipeline d'une fonction.

    Inspecte la fonction (ou tâche) pour déterminer si elle
    est décorée avec @pipeline_task ou @pipeline_task_multi_output
    et renvoie ses métadonnées.

    Args:
        func: Fonction ou tâche décorée à inspecter

    Returns:
        Dictionnaire avec les clés:
        - "output": nom de la sortie principale
        - "inputs": liste des noms d'entrées
        - "retries": nombre de tentatives
        - "is_pipeline_task": True si c'est une tâche pipeline
        - "multiple_outputs": True si multi-sorties
        - "output_types": dict des types de sorties si multi

    Example:
        from taskiq_flow.decorators import get_pipeline_metadata

        @pipeline_task(output="result")
        async def my_task(x: int) -> int:
            return x * 2

        meta = get_pipeline_metadata(my_task)
        # meta = {"output": "result", "inputs": ["x"], ...}
    """
    # Check if it's a TaskiqDecoratedTask
    if hasattr(func, "original_function"):
        original = getattr(func, "original_function", None)
        if original is not None:
            metadata = _task_registry.get_metadata(original)
            if metadata:
                return {
                    "output": metadata.output,
                    "inputs": metadata.inputs,
                    "retries": metadata.retries,
                    "retry_delay": metadata.retry_delay,
                    "retry_backoff": metadata.retry_backoff,
                    "retry_jitter": metadata.retry_jitter,
                    "max_retry_time": metadata.max_retry_time,
                    "resources": metadata.resources,
                    "is_pipeline_task": metadata.is_pipeline_task,
                    "multiple_outputs": metadata.multiple_outputs,
                    "output_types": metadata.output_types,
                }

    # Check directly in registry
    metadata = _task_registry.get_metadata(func)
    if metadata:
        return {
            "output": metadata.output,
            "inputs": metadata.inputs,
            "retries": metadata.retries,
            "retry_delay": metadata.retry_delay,
            "retry_backoff": metadata.retry_backoff,
            "retry_jitter": metadata.retry_jitter,
            "max_retry_time": metadata.max_retry_time,
            "resources": metadata.resources,
            "is_pipeline_task": metadata.is_pipeline_task,
            "multiple_outputs": metadata.multiple_outputs,
            "output_types": metadata.output_types,
        }

    # Check if func has attached metadata (legacy support)
    if hasattr(func, "_pipeline_metadata"):
        meta = func._pipeline_metadata
        return {
            "output": meta.output,
            "inputs": meta.inputs,
            "retries": meta.retries,
            "retry_delay": getattr(meta, "retry_delay", 1.0),
            "retry_backoff": getattr(meta, "retry_backoff", 2.0),
            "retry_jitter": getattr(meta, "retry_jitter", True),
            "max_retry_time": getattr(meta, "max_retry_time", 300),
            "resources": getattr(meta, "resources", {}),
            "is_pipeline_task": meta.is_pipeline_task,
            "multiple_outputs": meta.multiple_outputs,
            "output_types": meta.output_types,
        }

    # Legacy support for old attribute-based metadata
    if hasattr(func, "_pipeline_task"):
        return {
            "output": getattr(func, "_pipeline_output", ""),
            "inputs": getattr(func, "_pipeline_inputs", []),
            "retries": getattr(func, "_pipeline_retries", 0),
            "retry_delay": getattr(func, "_pipeline_retry_delay", 1.0),
            "retry_backoff": getattr(func, "_pipeline_retry_backoff", 2.0),
            "retry_jitter": getattr(func, "_pipeline_retry_jitter", True),
            "max_retry_time": getattr(func, "_pipeline_max_retry_time", 300),
            "resources": getattr(func, "_pipeline_resources", {}),
            "is_pipeline_task": True,
            "multiple_outputs": False,
            "output_types": {},
        }

    # Check if it's a function that was decorated
    # by looking at its __wrapped__ attribute (from functools.wraps)
    if hasattr(func, "__wrapped__"):
        wrapped = func.__wrapped__
        metadata = _task_registry.get_metadata(wrapped)
        if metadata:
            return {
                "output": metadata.output,
                "inputs": metadata.inputs,
                "retries": metadata.retries,
                "retry_delay": metadata.retry_delay,
                "retry_backoff": metadata.retry_backoff,
                "retry_jitter": metadata.retry_jitter,
                "max_retry_time": metadata.max_retry_time,
                "resources": metadata.resources,
                "is_pipeline_task": metadata.is_pipeline_task,
                "multiple_outputs": metadata.multiple_outputs,
                "output_types": metadata.output_types,
            }

    return {}


def is_pipeline_task(func: Any) -> bool:
    """
    Vérifie si une fonction est une tâche de pipeline.

    Args:
        func: Fonction à vérifier

    Returns:
        True si la fonction est décorée avec @pipeline_task ou
        @pipeline_task_multi_output, False sinon
    """
    metadata = get_pipeline_metadata(func)
    return metadata.get("is_pipeline_task", False)


def get_task_outputs(func: Any) -> list[str]:
    """
    Récupère tous les noms de sortie produits par une tâche.

    Pour une tâche standard, renvoie [output].
    Pour une tâche multi-sorties, renvoie [output_principal, ...sorties_supp].

    Args:
        func: Tâche de pipeline

    Returns:
        Liste des noms de sortie
    """
    metadata = get_pipeline_metadata(func)
    if not metadata:
        return []

    outputs = [metadata["output"]]
    if metadata.get("multiple_outputs"):
        output_types = metadata.get("output_types", {})
        outputs.extend(output_types.keys())

    return outputs


def validate_pipeline_outputs(tasks: list[Any]) -> None:
    """
    Valide que toutes les tâches ont des noms de sortie uniques.

    Vérifie l'absence de conflits: deux tâches différentes ne peuvent
    pas produire le même flux de données.

    Args:
        tasks: Liste de tâches de pipeline à valider

    Raises:
        PipelineError: Si des noms de sortie sont dupliqués

    Example:
        validate_pipeline_outputs([task1, task2, task3])
    """
    _task_registry.validate_outputs()


def get_all_pipeline_outputs() -> list[str]:
    """
    Récupère tous les noms de sortie enregistrés.

    Parcourt le registre global et renvoie la liste complète
    de tous les flux de données produits par les tâches
    de pipeline enregistrées.

    Returns:
        Liste de tous les output names
    """
    return _task_registry.get_all_outputs()


def get_task_by_output(output_name: str) -> Any | None:
    """
    Récupère la tâche qui produit un flux de données donné.

    Args:
        output_name: Nom du flux produit à rechercher

    Returns:
        Tâche correspondante ou None si non trouvée
    """
    return _task_registry.get_task_by_output(output_name)


def pipeline_task_multi_output(
    outputs: dict[str, Any],
    inputs: list[str] | None = None,
    retries: int = 0,
) -> Callable[..., Any]:
    """
    Décorateur pour tâche de pipeline produisant plusieurs sorties.

    Similaire à @pipeline_task mais la tâche renvoie un dictionnaire
    mappant plusieurs noms de sorties à leurs valeurs. Permet de
    produire plusieurs flux de données depuis une unique tâche.

    Args:
        outputs: Dictionnaire {nom_sortie: type} pour toutes les sorties.
                 Le premier clé est utilisée comme sortie principale.
                 Exemple: {"features": dict, "metadata": dict}
        inputs: Liste des noms de données consommées (inférés si None)
        retries: Nombre de tentatives en cas d'échec

    Returns:
        Décorateur transformant la fonction en tâche multi-sorties

    Example:
        @broker.task
        @pipeline_task_multi_output(
            outputs={"features": dict, "metadata": dict},
            retries=2
        )
        async def process_audio(track_path: str) -> dict:
            features = extract_features(track_path)
            metadata = extract_metadata(track_path)
            return {
                "features": features,
                "metadata": metadata
            }

        # Les deux sorties 'features' et 'metadata' sont
        # enregistrées et peuvent être consommées par des tâches dépendantes

    Raises:
        ValueError: Si outputs est vide ou si retries < 0
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        # Infer inputs if not provided
        inferred_inputs = inputs
        if inferred_inputs is None:
            inferred_inputs = _infer_inputs_from_signature(func)

        # Create metadata for multiple outputs
        primary_output = next(iter(outputs.keys()))  # Use first output as primary
        metadata = PipelineTaskMetadata(
            output=primary_output,
            inputs=inferred_inputs,
            retries=retries,
            task_name=getattr(func, "__name__", str(func)),
            multiple_outputs=True,
            output_types=outputs,
        )

        # Choose appropriate wrapper based on function type
        if _is_async_function(func):

            @wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                return await func(*args, **kwargs)

            wrapper = async_wrapper
        else:

            @wraps(func)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                return func(*args, **kwargs)

            wrapper = sync_wrapper

        # Register the task (only the wrapper to avoid conflicts)
        _task_registry.register_task(wrapper, metadata)

        # Register additional outputs
        for output_name in outputs:
            if output_name != primary_output:
                _task_registry._outputs[output_name] = wrapper

        # Attach metadata to wrapper for easy access
        wrapper._pipeline_metadata = metadata  # type: ignore

        return wrapper

    return decorator


def _infer_inputs_from_signature(func: Callable[..., Any]) -> list[str]:
    """
    Infer input parameter names from function signature.

    Args:
        func: Function to analyze

    Returns:
        List of parameter names that should be treated as inputs
    """
    try:
        sig = inspect.signature(func)
        params = list(sig.parameters.values())

        # Skip 'self' and 'cls' parameters, plus parameters with defaults
        # (assuming parameters with defaults are configuration, not data inputs)
        input_names = []
        for param in params:
            if (
                param.name not in ("self", "cls")
                and param.default is inspect.Parameter.empty
            ):
                input_names.append(param.name)

        return input_names
    except (AttributeError, TypeError, ValueError):
        return []


def _is_async_function(func: Callable[..., Any]) -> bool:
    """
    Check if a function is async.

    Args:
        func: Function to check

    Returns:
        True if the function is async
    """
    return inspect.iscoroutinefunction(func)


# Note: Legacy pipeline_task_legacy removed in favor of unified pipeline_task decorator


__all__ = [
    "get_all_pipeline_outputs",
    "get_pipeline_metadata",
    "get_task_by_output",
    "get_task_outputs",
    "is_pipeline_task",
    "pipeline_task",
    "pipeline_task_multi_output",
    "validate_pipeline_outputs",
]
