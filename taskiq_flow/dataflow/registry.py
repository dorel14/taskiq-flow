"""Registre dataflow pour le suivi des métadonnées de tâches.

Ce module contient la classe DataflowRegistry qui enregistre les tâches,
leurs métadonnées (outputs, inputs) et construit le graphe de dépendances.
C'est le cœur de l'analyse automatique des flux de données.

Auteur: SoniqueBay Team
Version: 0.3.1
"""

from typing import Any

from taskiq import AsyncTaskiqDecoratedTask

from taskiq_flow.dataflow.dag import DAG, DAGNode
from taskiq_flow.dataflow.node import DataNode


class DataflowRegistry:
    """
    Registre central pour l'analyse des dépendances de données.

    Collecte les métadonnées des tâches enregistrées (output, inputs)
    et maintient un index data -> producteur/consommateurs.
    Construit ensuite le DAG à partir de ces dépendances.

    C'est le cœur de l'analyse dataflow automatique.

    Usage interne principalement, mais peut être utilisé pour
    inspecter les dépendances avant exécution.

    Attributes:
        tasks: Liste des tâches enregistrées
        task_metadata: Dict {task: {output, inputs, ...}}
        data_nodes: Dict {nom_data: DataNode}
        data_producers: Dict {nom_data: tâche_productrice}
    """

    def __init__(self) -> None:
        """Initialise un registre vide."""
        self.tasks: list[Any] = []
        self.task_metadata: dict[Any, dict[str, Any]] = {}
        self.data_nodes: dict[str, DataNode] = {}
        self.data_producers: dict[str, Any] = {}

    def register_task(
        self,
        task: Any,
        output: str,
        inputs: list[str] | None = None,
        **metadata: Any,
    ) -> None:
        """
        Register a task with its data dependencies.

        Args:
            task: The task to register
            output: Name of the data produced by this task
            inputs: Optional list of data names consumed by this task
            **metadata: Additional metadata
        """
        if task not in self.tasks:
            self.tasks.append(task)

        self.task_metadata[task] = {
            "output": output,
            "inputs": inputs or [],
            **metadata,
        }

        # Create or update data node for output
        if output not in self.data_nodes:
            self.data_nodes[output] = DataNode(name=output)

        self.data_nodes[output].set_producer(task)
        self.data_producers[output] = task

        # Track inputs
        if inputs:
            for input_name in inputs:
                if input_name not in self.data_nodes:
                    self.data_nodes[input_name] = DataNode(
                        name=input_name,
                        is_external=True,
                    )
                self.data_nodes[input_name].add_consumer(task)

    def get_task_metadata(
        self,
        task: AsyncTaskiqDecoratedTask[Any, Any],
    ) -> dict[str, Any]:
        """Get metadata for a task."""
        return self.task_metadata.get(task, {})

    def get_data_dependencies(
        self,
        task: AsyncTaskiqDecoratedTask[Any, Any],
    ) -> list[str]:
        """Get the data dependencies for a task."""
        metadata = self.task_metadata.get(task, {})
        return metadata.get("inputs", [])

    def get_producer(self, data_name: str) -> AsyncTaskiqDecoratedTask[Any, Any] | None:
        """Get the task that produces the given data."""
        return self.data_producers.get(data_name)

    def get_consumers(self, data_name: str) -> list[AsyncTaskiqDecoratedTask[Any, Any]]:
        """Get tasks that consume the given data."""
        node = self.data_nodes.get(data_name)
        return node.consumers if node else []

    def build_dag(self) -> DAG:
        """
        Construit le DAG à partir des tâches enregistrées.

        Algorithme:
        1. Crée un DAGNode pour chaque tâche
        2. Pour chaque tâche, regarde ses inputs declarés
        3. Pour chaque input, trouve le producteur dans data_producers
        4. Ajoute une arête producteur -> consommateur
        5. Calcul les niveaux topologiques

        Returns:
            DAG prêt pour exécution

        Raises:
            ValueError: Si un input requis n'a aucun producteur
                      (s'il n'est pas enregistré comme entrée externe)

        Example:
            registry = DataflowRegistry()
            registry.register_task(task_a, output="features", inputs=[])
            registry.register_task(task_b, output="result", inputs=["features"])
            dag = registry.build_dag()
            # dag contient: task_a -> task_b
        """
        dag = DAG()
        task_to_node: dict[AsyncTaskiqDecoratedTask[Any, Any], DAGNode] = {}

        # Create nodes for all tasks
        for task in self.tasks:
            node = DAGNode(task=task)
            dag.add_node(node)
            task_to_node[task] = node

        # Create edges based on data dependencies
        for task in self.tasks:
            metadata = self.task_metadata.get(task, {})
            inputs = metadata.get("inputs", [])

            for input_name in inputs:
                producer = self.get_producer(input_name)

                if producer is None:
                    # Check if it's an external input
                    if input_name not in self.data_nodes:
                        raise ValueError(
                            f"No producer found for '{input_name}' "
                            f"required by task '{task.task_name}'",
                        )
                    # External input - no edge needed
                    continue

                # Add edge from producer to consumer
                producer_node = task_to_node[producer]
                consumer_node = task_to_node[task]
                dag.add_edge(producer_node, consumer_node)

        # Compute topological levels for parallel execution
        dag.compute_levels()

        return dag

    def get_external_inputs(self) -> list[str]:
        """Get list of external inputs (data with no producer)."""
        return [
            name
            for name, node in self.data_nodes.items()
            if node.is_external or node.producer_task is None
        ]

    def get_outputs(self) -> list[str]:
        """Get list of outputs (data produced by tasks)."""
        return list(self.data_producers.keys())

    def register_external_input(self, input_name: str) -> None:
        """
        Register an external input to the registry.

        External inputs are data values provided at pipeline execution time
        that are not produced by any task in the pipeline.

        Args:
            input_name: Name of the external input
        """
        if input_name not in self.data_nodes:
            self.data_nodes[input_name] = DataNode(
                name=input_name,
                is_external=True,
            )
