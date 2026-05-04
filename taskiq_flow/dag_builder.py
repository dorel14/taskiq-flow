"""Constructeur de DAG pour la construction automatique de pipelines.

Analyse les métadonnées des tâches et construit le graphe de
dépendances. Valide également le graphe (détection de cycles,
connexité et calcul des niveaux topologiques).

Auteur: SoniqueBay Team
Version: 0.3.1
"""

import inspect
from collections.abc import Callable
from typing import Any

from taskiq import AsyncTaskiqDecoratedTask

from taskiq_flow.dataflow.dag import DAG, DAGNode
from taskiq_flow.dataflow.registry import DataflowRegistry
from taskiq_flow.decorators import _task_registry, get_pipeline_metadata


class DAGBuilder:
    """
    Constructeur de DAG à partir de tâches de pipeline.

    Analyse les métadonnées des tâches (via @pipeline_task) pour
    construire automatiquement un graphe de dépendances. Le DAG
    résultant peut être utilisé par ExecutionEngine pour l'exécution
    parallèle intelligente.

    Le builder peut aussi analyser les tâches non-enregistrées pour
    détecter les dépendances manquantes.

    Attributes:
        None (classe statique, utilitaire)
    """

    @staticmethod
    def from_tasks(
        tasks: list[Any],
        registry: DataflowRegistry | None = None,
        external_inputs: list[str] | None = None,
    ) -> DAG:
        """
        Construit un DAG à partir d'une liste de tâches.

        Processus:
        1. Enregistre toutes les tâches dans un registre (créé si None)
        2. Enregistre les entrées externes spécifiées
        3. Construit le DAG à partir des dépendances
        4. Valide le DAG (cycles, connexité, niveaux)

        Args:
            tasks: Liste de tâches décorées (AsyncTaskiqDecoratedTask)
            registry: Registre optionnel pré-peuplé (si None, crée un nouveau)
            external_inputs: Noms des données fournies à l'exécution
                           (non produites par une tâche du pipeline)

        Returns:
            DAG représentant les dépendances entre tâches

        Raises:
            ValueError: Si un input externe conflite avec une sortie
                       de tâche, ou si le DAG est invalide (cycle, etc.)

        Example:
            dag = DAGBuilder.from_tasks(
                [task_a, task_b, task_c],
                external_inputs=["initial_data"]
            )
        """
        if registry is None:
            registry = DataflowRegistry()

        # Register all tasks
        for task in tasks:
            DAGBuilder._register_task(task, registry)

        # Register external inputs
        if external_inputs:
            for input_name in external_inputs:
                # Check for conflicts with task outputs
                if registry.get_producer(input_name) is not None:
                    raise ValueError(
                        f"External input '{input_name}' conflicts with task output. "
                        f"External inputs must not match any task output names.",
                    )
                # Register as external input
                registry.register_external_input(input_name)

        # Build and return DAG
        dag = registry.build_dag()

        # Validate the complete DAG
        DAGBuilder.validate_dag(dag)

        return dag

    @staticmethod
    def from_callable_tasks(
        tasks: list[Callable[..., Any]],
        registry: DataflowRegistry | None = None,
    ) -> DAG:
        """
        Build a DAG from callable tasks (decorated functions).

        Args:
            tasks: List of decorated callable tasks
            registry: Optional pre-populated registry

        Returns:
            A DAG representing task dependencies
        """
        if registry is None:
            registry = DataflowRegistry()

        # Register all tasks
        for task in tasks:
            DAGBuilder._register_callable_task(task, registry)

        # Build and return DAG
        return registry.build_dag()

    @staticmethod
    def _register_task(
        task: Any,
        registry: DataflowRegistry,
    ) -> None:
        """
        Register a single task with the registry.

        Extracts metadata from the task and registers it.
        """
        # Get metadata from the new registry system
        metadata_obj = _task_registry.get_metadata(task)

        if metadata_obj is None:
            # Fallback to legacy metadata system
            metadata = get_pipeline_metadata(task)
            if not metadata:
                # Task is not decorated with @pipeline_task
                return

            # Convert legacy dict format to new format
            output = metadata.get("output")
            inputs = metadata.get("inputs")
            retries = metadata.get("retries", 0)
            task_name = getattr(task, "task_name", str(task))

            if not output:
                # Try to infer from task name or signature
                output = DAGBuilder._infer_output_name(task)

            if inputs is None:
                # Infer from function signature
                inputs = DAGBuilder._infer_inputs(task)

            registry.register_task(
                task,
                output=output,
                inputs=inputs,
                retries=retries,
                task_name=task_name,
            )
        else:
            # Use new metadata system
            registry.register_task(
                task,
                output=metadata_obj.output,
                inputs=metadata_obj.inputs,
                retries=metadata_obj.retries,
                task_name=metadata_obj.task_name,
            )

    @staticmethod
    def _register_callable_task(
        task: Callable[..., Any],
        registry: DataflowRegistry,
    ) -> None:
        """
        Register a callable task with the registry.

        Extracts metadata from the decorated function.
        """
        # Get metadata from the new registry system
        metadata_obj = _task_registry.get_metadata(task)

        if metadata_obj is None:
            # Fallback to legacy metadata system
            metadata = get_pipeline_metadata(task)

            if not metadata:
                # Not a pipeline task
                return

            output = metadata.get("output")
            inputs = metadata.get("inputs")
            retries = metadata.get("retries", 0)

            if not output:
                raise ValueError(f"Task {task.__name__} must specify output name")

            if inputs is None:
                # Infer from function signature
                inputs = DAGBuilder._infer_inputs_from_callable(task)

            # Note: We can't register the callable directly since it's not
            # an AsyncTaskiqDecoratedTask yet. This will be handled when
            # the task is actually decorated by TaskIQ.
            # For now, we store the metadata for later use.
            registry.register_task(
                task,  # This will be replaced with the actual task later
                output=output,
                inputs=inputs,
                retries=retries,
                task_name=task.__name__,
            )
        else:
            # Use new metadata system
            registry.register_task(
                task,
                output=metadata_obj.output,
                inputs=metadata_obj.inputs,
                retries=metadata_obj.retries,
                task_name=metadata_obj.task_name,
            )

    @staticmethod
    def _infer_output_name(task: Any) -> str:
        """Infer output name from task."""
        # Use task name as default
        return task.task_name

    @staticmethod
    def _infer_inputs(task: AsyncTaskiqDecoratedTask[Any, Any]) -> list[str]:
        """
        Infer input names from task signature.

        Examines the task's function signature to determine
        which data inputs it requires.
        """
        try:
            # Try to get the original function
            func = getattr(task, "original_function", None)
            if func is None:
                # Try to get the wrapped function
                func = task

            # If it's still not a function, try to unwrap
            if not callable(func):
                return []

            sig = inspect.signature(func)
            params = list(sig.parameters.values())

            # Skip 'self' and 'cls' parameters
            # Include POSITIONAL_OR_KEYWORD, POSITIONAL_ONLY,
            # and KEYWORD_ONLY parameters
            input_names = []
            for param in params:
                if (
                    param.name not in ("self", "cls")
                    and param.default is inspect.Parameter.empty
                    and param.kind
                    in (
                        inspect.Parameter.POSITIONAL_OR_KEYWORD,
                        inspect.Parameter.POSITIONAL_ONLY,
                        inspect.Parameter.KEYWORD_ONLY,
                    )
                ):
                    input_names.append(param.name)

            return input_names
        except (AttributeError, TypeError, ValueError):
            return []

    @staticmethod
    def _infer_inputs_from_callable(task: Callable[..., Any]) -> list[str]:
        """
        Infer input names from a callable.

        Examines the function signature to determine
        which data inputs it requires.
        """
        try:
            sig = inspect.signature(task)
            params = list(sig.parameters.values())

            # Skip 'self' and 'cls' parameters, plus parameters with defaults
            # Also skip *args and **kwargs
            # Include POSITIONAL_OR_KEYWORD, POSITIONAL_ONLY,
            # and KEYWORD_ONLY parameters
            input_names = []
            for param in params:
                if (
                    param.name not in ("self", "cls")
                    and param.default is inspect.Parameter.empty
                    and param.kind
                    in (
                        inspect.Parameter.POSITIONAL_OR_KEYWORD,
                        inspect.Parameter.POSITIONAL_ONLY,
                        inspect.Parameter.KEYWORD_ONLY,
                    )
                ):
                    input_names.append(param.name)

            return input_names
        except (AttributeError, TypeError):
            return []

    @staticmethod
    def validate_dag(dag: DAG) -> None:
        """
        Valide un DAG pour détecter les problèmes courants.

        Effectue une série de vérifications:
        - DAG non vide
        - Absence de cycles (dépendances circulaires)
        - Possibilité de calculer les niveaux topologiques
        - Aucun composant déconnecté (toutes les tâches accessibles)
        - Niveaux valides (non-négatifs)

        Args:
            dag: DAG à valider

        Raises:
            ValueError: Si le DAG échoue à une validation, avec
                      un message détaillé identifiant le problème
        """
        DAGBuilder._validate_not_empty(dag)
        DAGBuilder._validate_no_cycles(dag)
        DAGBuilder._validate_levels_computable(dag)
        DAGBuilder._validate_connected(dag)
        DAGBuilder._validate_valid_levels(dag)

    @staticmethod
    def _validate_not_empty(dag: DAG) -> None:
        """Validate that the DAG contains at least one node."""
        if not dag.nodes:
            raise ValueError(
                "DAG validation failed: DAG contains no nodes. "
                "Ensure that decorated tasks are provided to the DAG builder.",
            )

    @staticmethod
    def _validate_no_cycles(dag: DAG) -> None:
        """Validate that the DAG has no circular dependencies."""
        try:
            dag.topological_sort()
        except ValueError as e:
            error_msg = str(e)
            if "Circular dependency" in error_msg:
                raise ValueError(
                    "DAG validation failed: Circular dependency detected in task "
                    f"graph. Tasks form a dependency cycle that cannot be resolved. "
                    f"Original error: {error_msg}",
                ) from e
            raise ValueError(
                f"DAG validation failed: Invalid DAG structure. {error_msg}",
            ) from e

    @staticmethod
    def _validate_levels_computable(dag: DAG) -> None:
        """Validate that topological levels can be computed."""
        try:
            dag.compute_levels()
        except Exception as e:
            raise ValueError(
                f"DAG validation failed: Cannot compute execution levels. "
                f"This may indicate corrupted dependency information. "
                f"Original error: {e}",
            ) from e

    @staticmethod
    def _validate_connected(dag: DAG) -> None:
        """Validate that all nodes are connected (no disconnected components)."""
        if len(dag.nodes) <= 1:
            return

        visited: set[int] = set()
        DAGBuilder._dfs_explore(dag.nodes[0], visited)

        if len(visited) < len(dag.nodes):
            raise ValueError(
                f"DAG validation failed: DAG contains disconnected components. "
                f"Found {len(dag.nodes)} total nodes but only {len(visited)} "
                f"are reachable from the starting node. Ensure all tasks are "
                f"properly connected through data dependencies.",
            )

    @staticmethod
    def _dfs_explore(node: DAGNode, visited: set[int]) -> None:
        """Depth-first search to explore connected nodes."""
        visited.add(id(node))
        for neighbor in node.dependents + node.dependencies:
            if id(neighbor) not in visited:
                DAGBuilder._dfs_explore(neighbor, visited)

    @staticmethod
    def _validate_valid_levels(dag: DAG) -> None:
        """Validate that all nodes have valid (non-negative) levels."""
        invalid_levels = [node for node in dag.nodes if node.level < 0]
        if invalid_levels:
            raise ValueError(
                f"DAG validation failed: Found nodes with invalid levels: "
                f"{[node.task_name for node in invalid_levels]}. "
                f"This indicates corrupted dependency level computation.",
            )

    @staticmethod
    def analyze_missing_dependencies(
        tasks: list[Any],
        registry: DataflowRegistry,
    ) -> dict[str, list[str]]:
        """
        Analyze which dependencies are missing for each task.

        Args:
            tasks: List of tasks to analyze
            registry: Registry containing task metadata

        Returns:
            Dictionary mapping task names to lists of missing input names
        """
        missing_deps: dict[str, list[str]] = {}

        for task in tasks:
            # Try new metadata system first
            metadata_obj = _task_registry.get_metadata(task)

            if metadata_obj is not None:
                # Use new metadata system
                inputs = metadata_obj.inputs
                task_name = metadata_obj.task_name
            else:
                # Fallback to legacy metadata system
                metadata = get_pipeline_metadata(task)
                if not metadata:
                    continue

                inputs = metadata.get("inputs", [])
                if hasattr(task, "task_name"):
                    task_name = task.task_name
                else:
                    task_name = getattr(task, "__name__", str(task))
                    # Remove module prefix if present
                    if ":" in task_name:
                        task_name = task_name.split(":")[-1]

            missing_inputs = []
            if inputs is not None:
                for input_name in inputs:
                    if registry.get_producer(input_name) is None:
                        # Check if it's registered as external input
                        data_node = registry.data_nodes.get(input_name)
                        if not (data_node and data_node.is_external):
                            missing_inputs.append(input_name)

            if missing_inputs:
                missing_deps[task_name] = missing_inputs

        return missing_deps


class PipelineDAGBuilder(DAGBuilder):
    """
    DAG builder specifically for Pipeline class.

    Integrates with the Pipeline class to build DAGs from
    registered tasks.
    """

    def __init__(self, pipeline: Any) -> None:
        """
        Initialize the builder.

        Args:
            pipeline: The pipeline instance
        """
        self.pipeline = pipeline
        self.registry = DataflowRegistry()

    def build(self) -> DAG:
        """Build the DAG from pipeline tasks."""
        return self.registry.build_dag()
