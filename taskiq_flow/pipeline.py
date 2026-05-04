"""Enhanced Pipeline class with dataflow support."""

import logging
from typing import Any

from taskiq import AsyncBroker
from taskiq.decor import AsyncTaskiqDecoratedTask

from taskiq_flow.dag_builder import DAGBuilder
from taskiq_flow.dataflow.dag import DAG
from taskiq_flow.dataflow.registry import DataflowRegistry
from taskiq_flow.execution_engine import ExecutionEngine
from taskiq_flow.map_reduce import MapReduce
from taskiq_flow.pipeliner import Pipeline as OriginalPipeline
from taskiq_flow.scheduling.scheduler import LabelBasedScheduler
from taskiq_flow.visualization import DAGVisualizer

logger = logging.getLogger(__name__)


class DataflowPipeline(OriginalPipeline[Any, Any]):
    """
    Pipeline avec orchestration basée sur les dépendances de données.

    Étend la classe Pipeline standard avec:
    - Construction automatique d'un DAG (Directed Acyclic Graph) à partir
      des dépendances déclarées via @pipeline_task
    - Exécution parallèle automatique des tâches indépendantes
    - Support natif des opérations map-reduce
    - Visualisation du graphe d'exécution

    Cette classe maintient une compatibilité totale avec Pipeline
    tout en offrant des fonctionnalités avancées pour les workflows
    complexes. L'orchestration dataflow détermine automatiquement
    l'ordre d'exécution en analysant les flux de données entre tâches.

    Example:
        @broker.task
        @pipeline_task(output="audio")
        async def load_audio(paths: list[str]) -> dict:
            return {"audio": ...}

        @broker.task
        @pipeline_task(output="features", inputs=["audio"])
        async def extract_features(audio: dict) -> dict:
            return {"features": ...}

        # Construction automatique du DAG
        pipeline = DataflowPipeline.from_tasks(
            broker,
            [load_audio, extract_features]
        )

        # Exécution pilotée par les données
        results = await pipeline.kiq_dataflow(paths=["file1.wav"])

    Note:
        - Hérite de toutes les méthodes de Pipeline (call_next, map, filter...)
        -with_tracking(), with_hooks() et with_options() fonctionnent également
        - La construction du DAG est lazy (à l'appel de kiq_dataflow())
    """

    def __init__(self, broker: AsyncBroker, *args: Any, **kwargs: Any) -> None:
        """
        Initialize the dataflow pipeline.

        Args:
            broker: TaskIQ broker
            *args: Passed to parent class
            **kwargs: Passed to parent class
        """
        super().__init__(broker, *args, **kwargs)

        # Dataflow-specific attributes
        self._registry: DataflowRegistry | None = None
        self._dag: DAG | None = None
        self._dataflow_tasks: list[AsyncTaskiqDecoratedTask[Any, Any]] = []
        self._registered_tasks: list[dict[str, Any]] = []
        self._data_cache: dict[str, Any] = {}
        self._is_dataflow_built: bool = False
        self._map_operations: list[dict[str, Any]] = []
        self._reduce_operations: list[dict[str, Any]] = []

    @classmethod
    def from_tasks(
        cls,
        broker: AsyncBroker,
        tasks: list[AsyncTaskiqDecoratedTask[Any, Any]],
    ) -> "DataflowPipeline":
        """
        Crée un pipeline dataflow à partir d'une liste de tâches décorées.

        C'est la méthode recommandée pour construire un pipeline dataflow.
        Elle enregistre automatiquement toutes les tâches et construit
        le DAG de dépendances basé sur les métadonnées @pipeline_task.

        Args:
            broker: Broker TaskIQ pour l'exécution
            tasks: Liste de tâches préalablement décorées avec @broker.task
                   et @pipeline_task (dans l'ordre d'ajout, non critique)

        Returns:
            DataflowPipeline prêt à être exécuté

        Example:
            pipeline = DataflowPipeline.from_tasks(
                broker,
                [
                    extract_audio,    # output="audio_features"
                    compute_mir,       # inputs=["audio_features"]
                    generate_tags,     # inputs=["mir_features"]
                    create_embedding   # inputs=["mir_features", "tags"]
                ]
            )

        Note:
            L'ordre de la liste n'affecte pas l'exécution (le DAG
            détermine l'ordre), mais un ordre logique aide la lecture.
        """
        pipeline = cls(broker)
        pipeline._dataflow_tasks = tasks
        pipeline._build_dataflow_dag()
        return pipeline

    def _build_dataflow_dag(self) -> None:
        """
        Build the dataflow DAG from registered tasks.

        Analyzes task metadata and constructs a DAG representing
        data dependencies between tasks.
        """
        if self._dataflow_tasks:
            self._dag = DAGBuilder.from_tasks(
                self._dataflow_tasks,
            )
            self._is_dataflow_built = True

    def add_dataflow_task(
        self,
        task: AsyncTaskiqDecoratedTask[Any, Any],
    ) -> "DataflowPipeline":
        """
        Add a task to the dataflow pipeline.

        Args:
            task: Decorated task to add

        Returns:
            Self for chaining
        """
        self._dataflow_tasks.append(task)
        self._is_dataflow_built = False
        return self

    async def kiq_dataflow(
        self,
        **inputs: Any,
    ) -> Any:
        """
        Exécute le pipeline avec orchestration dataflow.

        Construit le DAG (si pas déjà fait), puis utilise ExecutionEngine
        pour exécuter les tâches dans l'ordre topologique avec parallélisme
        maximal. Les dépendances de données sont résolues automatiquement.

        Args:
            **inputs: Données externes fournies au pipeline.
                      Les clés doivent correspondre aux inputs requis
                      par les tâches sans producteur interne.

        Returns:
            Dictionnaire de toutes les sorties produites par le pipeline,
            indexées par leur nom de flux.

        Example:
            results = await pipeline.kiq_dataflow(
                track_paths=["track1.wav", "track2.wav"]
            )
            # results = {
            #   "audio_features": {...},
            #   "mir_features": {...},
            #   "tags": [...]
            # }

        Raises:
            ValueError: Si aucun DAG n'est construit
            PipelineError: Si l'exécution échoue

        Note:
            Le DAG est construit automatiquement au premier appel
            si des tâches ont été ajoutées via from_tasks() ou add_dataflow_task().
        """
        if not self._is_dataflow_built:
            self._build_dataflow_dag()

        if not self._dag:
            raise ValueError("No DAG built. Add tasks first.")

        # Create execution engine
        engine = ExecutionEngine(
            broker=self.broker,
            dag=self._dag,
            fail_fast=self.options.fail_fast,
            continue_on_error=self.options.continue_on_error,
            skip_failed=self.options.skip_failed,
        )

        # Execute
        return await engine.execute(
            inputs=inputs,
            pipeline_id=self.pipeline_id,
        )

    def map(  # type: ignore[override]
        self,
        task: AsyncTaskiqDecoratedTask[Any, Any],
        items: list[Any],
        output: str,
        **kwargs: Any,
    ) -> "DataflowPipeline":
        """
        Ajoute une opération map au pipeline dataflow.

        Contrairement à la méthode map de Pipeline qui s'applique au
        résultat précédent, cette version prend une collection explicite
        d'items et les traite en parallèle. Le résultat est stocké
        sous le nom 'output' et peut être consommé par des tâches ultérieures.

        Args:
            task: Tâche à appliquer à chaque item
            items: Liste des items à traiter
            output: Nom du flux de données produisant les résultats
            **kwargs: Options additionnelles:
                     - chunk_config: ChunkConfig pour chunking intelligent
                     - max_parallel: Limite de tâches parallèles
                     - param_name: Nom du paramètre (défaut: premier param)

        Returns:
            Self pour chaînage fluent

        Example:
            pipeline.map(
                process_track,
                track_list,
                output="track_features",
                chunk_config=ChunkConfig(chunk_size=50),
                max_parallel=10,
            )
        """
        # Store map operation for execution
        if not hasattr(self, "_map_operations"):
            self._map_operations = []

        # Also add task to dataflow tasks for DAG building
        if task not in self._dataflow_tasks:
            self._dataflow_tasks.append(task)
        self._is_dataflow_built = False

        self._map_operations.append(
            {
                "type": "map",
                "task": task,
                "items": items,
                "output": output,
                "kwargs": kwargs,
            },
        )

        logger.info(
            "Map operation added to pipeline",
            extra={
                "task_name": task.task_name,
                "output": output,
                "items_count": len(items) if hasattr(items, "__len__") else None,
                "chunk_config": kwargs.get("chunk_config"),
                "max_parallel": kwargs.get("max_parallel"),
            },
        )

        return self

    def reduce(  # type: ignore[override]
        self,
        task: AsyncTaskiqDecoratedTask[Any, Any],
        input_name: str,
        output: str,
        **kwargs: Any,
    ) -> "DataflowPipeline":
        """
        Ajoute une opération de réduction au pipeline.

        Agrège les résultats d'une opération map précédente en une
        seule valeur. La réduction peut être effectuée avec ou
        sans tâche de pré-traitement des items.

        Args:
            task: Tâche de réduction (optionnel, ex: sum, aggregate)
            input_name: Nom du flux de données contenant les items à réduire
            output: Nom du flux de données produisant le résultat
            **kwargs: Options additionnelles:
                     - chunk_size: Taille de chunk pour réduction par lots
                     - initial: Valeur initiale de l'accumulateur

        Returns:
            Self pour chaînage fluent

        Example:
            pipeline.reduce(
                aggregate_features,
                "track_features",
                output="playlist_stats",
                chunk_size=100,
                initial={}
            )
        """
        # Store reduce operation for execution
        if not hasattr(self, "_reduce_operations"):
            self._reduce_operations = []

        # Also add task to dataflow tasks for DAG building
        if task not in self._dataflow_tasks:
            self._dataflow_tasks.append(task)
        self._is_dataflow_built = False

        self._reduce_operations.append(
            {
                "type": "reduce",
                "task": task,
                "input_name": input_name,
                "output": output,
                "kwargs": kwargs,
            },
        )

        logger.info(
            "Reduce operation added to pipeline",
            extra={
                "task_name": task.task_name,
                "input_name": input_name,
                "output": output,
            },
        )

        return self

    async def kiq_map_reduce(
        self,
        **inputs: Any,
    ) -> Any:
        """
        Exécute le pipeline en mode map-reduce.

        Alternative à kiq_dataflow() qui exécute explicitement les
        opérations map et reduce enchaînées. Utile pour les pipelines
        de traitement par lots.

        Args:
            **inputs: Données externes pour le pipeline

        Returns:
            Résultat final de la dernière étape de réduction

        Example:
            result = await pipeline.kiq_map_reduce(
                track_list=tracks,
                metadata=meta
            )
        """
        logger.info(
            "Starting map-reduce pipeline execution",
            extra={
                "map_operations": len(getattr(self, "_map_operations", [])),
                "reduce_operations": len(getattr(self, "_reduce_operations", [])),
                "inputs": list(inputs.keys()),
            },
        )

        results: dict[str, Any] = {}

        # Execute map operations
        if hasattr(self, "_map_operations"):
            for op in self._map_operations:
                op_dict: dict[str, Any] = op  # type: ignore[assignment]
                # Use advanced map with chunking if configured
                kwargs = op_dict.get("kwargs", {})
                chunk_config = kwargs.get("chunk_config")
                max_parallel = kwargs.get("max_parallel")

                logger.info(
                    "Executing map operation",
                    extra={
                        "task_name": op_dict["task"].task_name,
                        "output": op_dict["output"],
                        "items_count": len(op_dict["items"]),
                        "has_chunk_config": chunk_config is not None,
                        "max_parallel": max_parallel,
                    },
                )

                if chunk_config:
                    # Use advanced map with chunking
                    map_result = await MapReduce.map(
                        self.broker,
                        op_dict["task"],
                        op_dict["items"],
                        output=op_dict["output"],
                        param_name=op_dict.get("param_name"),
                        max_parallel=max_parallel,
                        chunk_config=chunk_config,
                        **{
                            k: v
                            for k, v in kwargs.items()
                            if k not in ("chunk_config", "max_parallel")
                        },
                    )
                    results[op_dict["output"]] = map_result.results
                    # Store metadata for potential use
                    results[f"{op_dict['output']}_metadata"] = {
                        "items_processed": map_result.items_processed,
                        "duration": map_result.duration,
                        "success_rate": map_result.success_rate,
                        "errors": len(map_result.errors),
                    }

                    logger.info(
                        "Map operation completed",
                        extra={
                            "task_name": op_dict["task"].task_name,
                            "output": op_dict["output"],
                            "items_processed": map_result.items_processed,
                            "duration": map_result.duration,
                            "success_rate": map_result.success_rate,
                        },
                    )
                else:
                    # Standard map
                    result = await MapReduce.map(
                        self.broker,
                        op_dict["task"],
                        op_dict["items"],
                        output=op_dict["output"],
                        param_name=op_dict.get("param_name"),
                        max_parallel=max_parallel,
                        **{
                            k: v
                            for k, v in kwargs.items()
                            if k not in ("max_parallel",)
                        },
                    )
                    results[op_dict["output"]] = result.results

                    logger.info(
                        "Standard map operation completed",
                        extra={
                            "task_name": op_dict["task"].task_name,
                            "output": op_dict["output"],
                        },
                    )

        # Execute reduce operations
        if hasattr(self, "_reduce_operations"):
            for op in self._reduce_operations:
                op_dict_reduce: dict[str, Any] = op  # type: ignore[assignment]
                input_data = results.get(op_dict_reduce["input_name"], [])
                kwargs = op_dict_reduce.get("kwargs", {})
                chunk_size = kwargs.get("chunk_size")
                initial = kwargs.get("initial")

                logger.info(
                    "Executing reduce operation",
                    extra={
                        "task_name": op_dict_reduce["task"].task_name,
                        "input_name": op_dict_reduce["input_name"],
                        "output": op_dict_reduce["output"],
                        "input_count": (
                            len(input_data) if hasattr(input_data, "__len__") else None
                        ),
                        "has_chunk_size": chunk_size is not None,
                    },
                )

                if chunk_size:
                    # Use chunked reduction
                    result = await MapReduce.reduce(
                        self.broker,
                        op_dict_reduce["task"],
                        input_data,
                        output=op_dict_reduce["output"],
                        chunk_size=chunk_size,
                        initial=initial if initial is not None else 0,
                        **{
                            k: v
                            for k, v in kwargs.items()
                            if k not in ("chunk_size", "initial")
                        },
                    )
                else:
                    # Standard reduction
                    # Ensure initial is in kwargs if not present
                    reduce_kwargs = kwargs.copy()
                    if "initial" not in reduce_kwargs:
                        reduce_kwargs["initial"] = 0

                    result = await MapReduce.reduce(
                        self.broker,
                        op_dict_reduce["task"],
                        input_data,
                        output=op_dict_reduce["output"],
                        **reduce_kwargs,
                    )

                results[op_dict_reduce["output"]] = result

                logger.info(
                    "Reduce operation completed",
                    extra={
                        "task_name": op_dict_reduce["task"].task_name,
                        "output": op_dict_reduce["output"],
                    },
                )

        logger.info(
            "Map-reduce pipeline execution completed",
            extra={
                "results_count": len(results),
                "result_keys": list(results.keys()),
            },
        )

        # Return the final result (last reduce output or first map output)
        if hasattr(self, "_reduce_operations") and self._reduce_operations:
            return results[self._reduce_operations[-1]["output"]]
        if hasattr(self, "_map_operations") and self._map_operations:
            return results[self._map_operations[-1]["output"]]
        return results

    async def schedule_with_labels(
        self,
        scheduler: "LabelBasedScheduler",
        label: str,
        cron: str | None = None,
        interval_seconds: int | None = None,
        **inputs: Any,
    ) -> str:
        """Schedule the pipeline with label-based scheduling.

        This method schedules the pipeline using TaskIQ LabelScheduleSource,
        which is a lightweight alternative to APScheduler.

        Args:
            scheduler: LabelBasedScheduler instance
            label: Unique label for this schedule
            cron: Cron expression (e.g., "0 9 * * *" for daily at 9 AM)
            interval_seconds: Interval in seconds (alternative to cron)
            **inputs: External inputs to the pipeline

        Returns:
            The schedule ID

        Raises:
            ValueError: If neither cron nor interval is specified
        """
        return await scheduler.schedule_with_label(
            pipeline=self,
            label=label,
            cron=cron,
            interval_seconds=interval_seconds,
            args=(),
            kwargs=inputs,
            enabled=True,
        )

    async def schedule_with_cron(
        self,
        scheduler: "LabelBasedScheduler",
        label: str,
        cron: str,
        **inputs: Any,
    ) -> str:
        """Schedule the pipeline with a cron expression.

        Args:
            scheduler: LabelBasedScheduler instance
            label: Unique label for this schedule
            cron: Cron expression (e.g., "0 9 * * *" for daily at 9 AM)
            **inputs: External inputs to the pipeline

        Returns:
            The schedule ID
        """
        return await scheduler.schedule_with_cron(
            pipeline=self,
            label=label,
            cron=cron,
            args=(),
            kwargs=inputs,
            enabled=True,
        )

    async def schedule_with_interval(
        self,
        scheduler: "LabelBasedScheduler",
        label: str,
        interval_seconds: int,
        **inputs: Any,
    ) -> str:
        """Schedule the pipeline with a fixed interval.

        Args:
            scheduler: LabelBasedScheduler instance
            label: Unique label for this schedule
            interval_seconds: Interval in seconds
            **inputs: External inputs to the pipeline

        Returns:
            The schedule ID
        """
        return await scheduler.schedule_with_interval(
            pipeline=self,
            label=label,
            interval_seconds=interval_seconds,
            args=(),
            kwargs=inputs,
            enabled=True,
        )

    async def kiq_map_reduce_advanced(
        self,
        map_task: AsyncTaskiqDecoratedTask[Any, Any],
        reduce_task: AsyncTaskiqDecoratedTask[Any, Any],
        items: list[Any],
        map_output: str = "mapped",
        reduce_output: str = "reduced",
        map_param_name: str | None = None,
        max_parallel: int | None = None,
        reduce_chunk_size: int | None = None,
        **kwargs: Any,
    ) -> Any:
        """
        Execute advanced map-reduce with full feature support.

        Combines map and reduce operations with support for:
        - Parallel map execution
        - Intelligent chunking
        - Chunked reduction
        - Progress tracking

        Args:
            map_task: Task to apply to each item
            reduce_task: Task to aggregate results
            items: List of items to process
            map_output: Name for map output
            reduce_output: Name for reduce output
            map_param_name: Parameter name for map items
            max_parallel: Maximum parallel map tasks
            reduce_chunk_size: Chunk size for reduction
            **kwargs: Additional kwargs (passed to both map and reduce tasks)

        Returns:
            Reduced result

        Example:
            result = await pipeline.kiq_map_reduce_advanced(
                extract_features,
                aggregate_features,
                track_list,
                max_parallel=10,
                reduce_chunk_size=100,
            )
        """
        return await MapReduce.map_reduce(
            self.broker,
            map_task,
            reduce_task,
            items,
            map_output=map_output,
            reduce_output=reduce_output,
            map_param_name=map_param_name,
            max_parallel=max_parallel,
            reduce_chunk_size=reduce_chunk_size,
            **kwargs,
        )

    async def kiq_map_sweep(
        self,
        task: AsyncTaskiqDecoratedTask[Any, Any],
        param_values: dict[str, list[Any]],
        output: str,
        max_parallel: int | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Execute multi-dimensional parameter sweep.

        Creates a Cartesian product of all parameter values and
        executes the task for each combination.

        Args:
            task: Task to apply
            param_values: Dictionary mapping parameter names to lists of values
            output: Name for the output list
            max_parallel: Maximum parallel tasks
            **kwargs: Additional kwargs

        Returns:
            Dictionary with results and metadata

        Example:
            result = await pipeline.kiq_map_sweep(
                train_model,
                param_values={
                    "learning_rate": [0.01, 0.001, 0.0001],
                    "batch_size": [32, 64, 128],
                },
                output="experiments",
                max_parallel=5,
            )
        """
        map_result = await MapReduce.map_sweep(
            self.broker,
            task,
            param_values,
            output=output,
            max_parallel=max_parallel,
            **kwargs,
        )

        return {
            output: map_result.results,
            "metadata": {
                "items_processed": map_result.items_processed,
                "duration": map_result.duration,
                "success_rate": map_result.success_rate,
                "errors": len(map_result.errors),
            },
        }

    def visualize(self) -> dict[str, Any]:
        """
        Génère une représentation JSON du DAG du pipeline.

        Returns:
            Dictionnaire avec clés 'nodes', 'edges', 'levels' pour
            visualisation par une interface web ou outil externe.

        Example:
            dag_json = pipeline.visualize()
            print(json.dumps(dag_json, indent=2))
        """
        if not self._dag:
            self._build_dataflow_dag()

        if not self._dag:
            raise ValueError("No DAG to visualize")

        return DAGVisualizer.to_json(self._dag)

    def visualize_dot(self) -> str:
        """
        Génère une représentation DOT (Graphviz) du DAG.

        Returns:
            Chaîne au format DOT pouvant être convertie en image
            avec Graphviz: `dot -Tpng graph.dot -o graph.png`

        Example:
            dot = pipeline.visualize_dot()
            with open("pipeline.dot", "w") as f:
                f.write(dot)
        """
        if not self._dag:
            self._build_dataflow_dag()

        if not self._dag:
            raise ValueError("No DAG to visualize")

        return DAGVisualizer.to_dot(self._dag)

    def print_dag(self) -> None:
        """
        Print ASCII representation of DAG.

        Example:
            pipeline.print_dag()
        """
        if not self._dag:
            self._build_dataflow_dag()

        if not self._dag:
            raise ValueError("No DAG to print")

        DAGVisualizer.print_ascii(self._dag)


# Re-export the original Pipeline for backward compatibility
Pipeline = OriginalPipeline

__all__ = [
    "DataflowPipeline",
    "Pipeline",
]
