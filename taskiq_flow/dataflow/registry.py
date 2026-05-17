"""
Registre dataflow pour le suivi des métadonnées de tâches.

Ce module contient la classe DataflowRegistry qui enregistre les tâches,
leurs métadonnées (outputs, inputs) et construit le graphe de dépendances.
C'est le cœur de l'analyse automatique des flux de données.

Auteur: SoniqueBay Team
Version: 1.0.2
"""

from typing import Any

from taskiq import AsyncTaskiqDecoratedTask

from taskiq_flow.dataflow.dag import DAG, DAGNode
from taskiq_flow.dataflow.node import DataNode


class DataflowRegistry:
    """
    Registre central pour l'analyse des dépendances de données.

    Cette classe collecte les métadonnées des tâches enregistrées (output, inputs)
    et maintient un index data -> producteur/consommateurs. Elle construit ensuite
    le DAG (Directed Acyclic Graph) à partir de ces dépendances.

    C'est le cœur de l'analyse dataflow automatique. Le registry permet :
    - L'enregistrement des tâches avec leurs dépendances de données
    - La追踪 (traçabilité) des flux de données à travers le pipeline
    - La construction automatique du graphe de dépendances
    - L'inspection des métadonnées avant exécution
    - La détection des entrées externes et des sorties finales

    Usage interne principalement, mais peut être utilisé pour
    inspecter les dépendances avant exécution ou construire des DAGs
    manuellement pour des cas avancés.

    Attributes:
        tasks: Liste des tâches enregistrées dans l'ordre d'enregistrement
        task_metadata: Dictionnaire {task: {output, inputs, ...}} contenant
                      toutes les métadonnées associées à chaque tâche
        data_nodes: Dictionnaire {nom_data: DataNode} représentant tous les
                   points de données du flux (entrées, sorties, intermédiaires)
        data_producers: Dictionnaire {nom_data: tâche_productrice} permettant
                       de retrouver rapidement qui produit chaque donnée

    Example:
        Création d'un registre et enregistrement de tâches simples:

        >>> registry = DataflowRegistry()
        >>> registry.register_task(task_load_data, output="raw_data", inputs=[])
        >>> registry.register_task(task_process, output="features", inputs=["raw_data"])
        >>> registry.register_task(
        ...     task_train,
        ...     output="model",
        ...     inputs=["features"],
        ...     version="1.0"
        ... )

    Example:
        Interrogation du registre pour inspecter les dépendances:

        >>> features_producer = registry.get_producer("features")
        >>> print(f"features est produit par: {features_producer.task_name}")
        >>> consumers = registry.get_consumers("raw_data")
        >>> print(f"raw_data est consommé par {len(consumers)} tâches")

    Example:
        Construction du DAG et analyse du flux:

        >>> dag = registry.build_dag()
        >>> print(f"Le DAG contient {len(dag.nodes)} tâches")
        >>> external_inputs = registry.get_external_inputs()
        >>> print(f"Entrées externes: {external_inputs}")
        >>> outputs = registry.get_outputs()
        >>> print(f"Sorties produites: {outputs}")

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
        Enregistre une tâche avec ses dépendances de données.

        Cette méthode constitue le cœur du système d'autodécouverte du flux.
        En déclarant qu'une tâche produit une sortie et consomme certaines entrées,
        le registry construit automatiquement la carte des dépendances.

        Args:
            task: La tâche à enregistrer (decorated TaskIQ task)
            output: Nom de la donnée produite par cette tâche
            inputs: Liste optionnelle des noms de données consommées par cette tâche
            **metadata: Métadonnées additionnelles (version, tags, etc.)

        Raises:
            ValueError: Si la tâche est déjà enregistrée avec un output différent

        Example:
            Enregistrement d'une tâche simple sans dépendances:

            >>> registry.register_task(
            ...     task=preprocess_data,
            ...     output="preprocessed_data"
            ... )

        Example:
            Enregistrement d'une tâche avec plusieurs dépendances:

            >>> registry.register_task(
            ...     task=merge_features,
            ...     output="merged_features",
            ...     inputs=["features_a", "features_b", "features_c"],
            ...     version="2.1",
            ...     tags=["merge", "critical"]
            ... )

        Note:
            - Une tâche ne peut produire qu'une seule sortie primaire
            - Les inputs peuvent référencer des données externes non produites
              dans le pipeline (elles seront marquées comme is_external=True)
            - L'ordre d'enregistrement n'affecte pas la construction du DAG

        """
        if task in self.task_metadata:
            existing_output = self.task_metadata[task]["output"]
            if existing_output != output:
                raise ValueError(
                    f"Task already registered with output '{existing_output}', "
                    f"cannot re-register with different output '{output}'",
                )

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
        """
        Récupère les métadonnées d'une tâche.

        Args:
            task: La tâche dont on veut les métadonnées

        Returns:
            Dictionnaire contenant 'output', 'inputs' et toutes les
            métadonnées additionnelles enregistrées. Retourne un
            dictionnaire vide si la tâche n'est pas enregistrée.

        Example:
            >>> metadata = registry.get_task_metadata(my_task)
            >>> print(f"Output: {metadata.get('output')}")
            >>> print(f"Inputs: {metadata.get('inputs')}")
            >>> print(f"Version: {metadata.get('version', 'N/A')}")

        """
        return self.task_metadata.get(task, {})

    def get_data_dependencies(
        self,
        task: AsyncTaskiqDecoratedTask[Any, Any],
    ) -> list[str]:
        """
        Récupère les dépendances de données d'une tâche.

        Args:
            task: La tâche dont on veut les dépendances

        Returns:
            Liste des noms de données requis par cette tâche.
            Retourne une liste vide si la tâche n'est pas enregistrée.

        Example:
            >>> deps = registry.get_data_dependencies(process_task)
            >>> if "raw_data" in deps:
            ...     print("process_task dépend de raw_data")

        """
        metadata = self.task_metadata.get(task, {})
        inputs = metadata.get("inputs", [])
        if not isinstance(inputs, list):
            return []
        return [str(x) for x in inputs]

    def get_producer(
        self,
        data_name: str,
    ) -> AsyncTaskiqDecoratedTask[Any, Any] | None:
        """
        Récupère la tâche qui produit une donnée donnée.

        Args:
            data_name: Nom de la donnée dont on cherche le producteur

        Returns:
            La tâche productrice ou None si la donnée n'est pas produite
            par une tâche (entrée externe ou donnée inconnue)

        Example:
            >>> producer = registry.get_producer("features")
            >>> if producer:
            ...     print(f"Producteur: {producer.task_name}")
            >>> else:
            ...     print("'features' est une entrée externe")

        """
        return self.data_producers.get(data_name)

    def get_consumers(
        self,
        data_name: str,
    ) -> list[AsyncTaskiqDecoratedTask[Any, Any]]:
        """
        Récupère les tâches qui consomment une donnée donnée.

        Args:
            data_name: Nom de la donnée dont on cherche les consommateurs

        Returns:
            Liste des tâches qui consomment cette donnée.
            Retourne une liste vide si la donnée n'existe pas.

        Example:
            >>> consumers = registry.get_consumers("preprocessed_data")
            >>> print(f"Nombre de consommateurs: {len(consumers)}")
            >>> for consumer in consumers:
            ...     print(f" - {consumer.task_name}")

        """
        node = self.data_nodes.get(data_name)
        return node.consumers if node else []

    def build_dag(self) -> DAG:
        """
        Construit le DAG à partir des tâches enregistrées.

        Algorithme de construction:
        1. Crée un DAGNode pour chaque tâche
        2. Pour chaque tâche, examine ses inputs déclarés
        3. Pour chaque input, trouve le producteur dans data_producers
        4. Ajoute une arête producteur -> consommateur
        5. Calcule les niveaux topologiques pour l'exécution parallèle

        L'autodécouverte du flux fonctionne ainsi : chaque tâche déclare
        sa sortie et ses entrées. En liant producteurs et consommateurs
        via les noms de données, le registry reconstruit automatiquement
        le graphe complet des dépendances.

        Returns:
            DAG prêt pour exécution, avec tous les nœuds et arêtes
            correctement connectés et les niveaux topologiques calculés.

        Raises:
            ValueError: Si un input requis n'a aucun producteur
                      (s'il n'est pas enregistré comme entrée externe)
            ValueError: Si une dépendance circulaire est détectée dans le DAG

        Example:
            Construction d'un DAG simple:

            >>> registry = DataflowRegistry()
            >>>
            >>> # Déclaration des tâches et leurs flux de données
            >>> registry.register_task(extract, output="raw_data", inputs=[])
            >>> registry.register_task(
            ...     transform,
            ...     output="processed_data",
            ...     inputs=["raw_data"]
            ... )
            >>> registry.register_task(
            ...     load,
            ...     output="final_result",
            ...     inputs=["processed_data"]
            ... )
            >>>
            >>> # Construction automatique du DAG
            >>> dag = registry.build_dag()
            >>>
            >>> # Analyse du DAG
            >>> print(f"Tâches: {len(dag.nodes)}")  # 3
            >>> print(f"Exécution en {dag.get_max_level()} niveaux")

        Example:
            Pipeline avec plusieurs dépendances et entrées externes:

            >>> registry = DataflowRegistry()
            >>>
            >>> # Entrées externes déclarées
            >>> registry.register_external_input("user_config")
            >>> registry.register_external_input("api_key")
            >>>
            >>> # Pipeline de traitement
            >>> registry.register_task(fetch, output="api_data", inputs=["api_key"])
            >>> registry.register_task(
            ...     enrich,
            ...     output="enriched_data",
            ...     inputs=["api_data", "user_config"]
            ... )
            >>> registry.register_task(
            ...     aggregate,
            ...     output="report",
            ...     inputs=["enriched_data"]
            ... )
            >>>
            >>> dag = registry.build_dag()
            >>>
            >>> # Vérification
            >>> print(registry.get_external_inputs())
            >>> # ['user_config', 'api_key']
            >>>
            >>> print(registry.get_outputs())
            >>> # ['api_data', 'enriched_data', 'report']

        Note:
            - L'ordre d'enregistrement n'affecte pas la structure du DAG
            - Toutes les tâches doivent être enregistrées avant build_dag()
            - Une tâche ne peut avoir qu'un seul output primaire
            - Les entrées externes sont automatiquement détectées

        """
        dag = DAG()
        task_to_node: dict[Any, DAGNode] = {}

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
                            f"required by task "
                            f"'{getattr(task, 'task_name', str(task))}'",
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
        """
        Récupère la liste des entrées externes.

        Les entrées externes sont des données qui n'ont pas de producteur
        dans le pipeline. Ce sont les points d'entrée du flux de données.

        Returns:
            Liste des noms de données externes

        Example:
            >>> registry = DataflowRegistry()
            >>> registry.register_external_input("user_id")
            >>> registry.register_external_input("config_file")
            >>> print(registry.get_external_inputs())
            ['user_id', 'config_file']

        """
        return [
            name
            for name, node in self.data_nodes.items()
            if node.is_external or node.producer_task is None
        ]

    def get_outputs(self) -> list[str]:
        """
        Récupère la liste des sorties produites par les tâches.

        Returns:
            Liste des noms de données produites par au moins une tâche

        Example:
            >>> outputs = registry.get_outputs()
            >>> print(f"Le pipeline produit {len(outputs)} sorties")

        """
        return list(self.data_producers.keys())

    def register_external_input(self, input_name: str) -> None:
        """
        Enregistre une entrée externe dans le registry.

        Les entrées externes sont des valeurs de données fournies au moment
        de l'exécution du pipeline qui ne sont produites par aucune tâche
        dans le pipeline. C'est utile pour déclarer explicitement les
        paramètres d'entrée du pipeline.

        Args:
            input_name: Nom de l'entrée externe

        Example:
            >>> registry = DataflowRegistry()
            >>> # Déclaration des paramètres d'entrée du pipeline
            >>> registry.register_external_input("input_file_path")
            >>> registry.register_external_input("threshold")
            >>> registry.register_external_input("model_version")

        """
        if input_name not in self.data_nodes:
            self.data_nodes[input_name] = DataNode(
                name=input_name,
                is_external=True,
            )

    def get_tasks(self) -> list[Any]:
        """
        Récupère la liste de toutes les tâches enregistrées.

        Returns:
            Liste des tâches dans l'ordre d'enregistrement

        Example:
            >>> all_tasks = registry.get_tasks()
            >>> for task in all_tasks:
            ...     print(task.task_name)

        """
        return self.tasks.copy()

    def clear(self) -> None:
        """
        Vide complètement le registry.

        Supprime toutes les tâches, métadonnées et nœuds de données.
        Utile pour réinitialiser l'état entre différents pipelines.

        Example:
            >>> registry = DataflowRegistry()
            >>> # ... enregistrement de tâches ...
            >>> registry.clear()
            >>> # Registry vide, prêt pour un nouveau pipeline

        """
        self.tasks.clear()
        self.task_metadata.clear()
        self.data_nodes.clear()
        self.data_producers.clear()

    def __len__(self) -> int:
        """
        Retourne le nombre de tâches enregistrées.

        Returns:
            Nombre de tâches dans le registry

        Example:
            >>> registry = DataflowRegistry()
            >>> len(registry)
            0
            >>> registry.register_task(task_a, output="data_a")
            >>> len(registry)
            1

        """
        return len(self.tasks)

    def __contains__(
        self,
        task: Any,
    ) -> bool:
        """
        Vérifie si une tâche est enregistrée dans le registry.

        Args:
            task: La tâche à vérifier

        Returns:
            True si la tâche est enregistrée, False sinon

        Example:
            >>> if my_task in registry:
            ...     print("Tâche déjà enregistrée")

        """
        return task in self.task_metadata

    def __repr__(self) -> str:
        """
        Représentation string du registry.

        Returns:
            Chaîne décrivant le nombre de tâches, données et dépendances

        Example:
            >>> print(repr(registry))
            <DataflowRegistry tasks=5, data_nodes=8, external_inputs=2>

        """
        external = len(self.get_external_inputs())
        return (
            f"<DataflowRegistry tasks={len(self.tasks)}, "
            f"data_nodes={len(self.data_nodes)}, "
            f"external_inputs={external}>"
        )
