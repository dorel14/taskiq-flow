"""Graphe Orienté Acyclique (DAG) pour l'exécution de pipeline.

Ce module définit les classes DAG et DAGNode pour représenter
la structure de dépendances entre tâches. Le DAG permet de
déterminer l'ordre d'exécution et les niveaux de parallélisme.

Auteur: SoniqueBay Team
Version: 0.3.2
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass(eq=False)
class DAGNode:
    """
    Nœud du graphe représentant une tâche et ses dépendances.

    Chaque DAGNode encapsule une tâche (AsyncTaskiqDecoratedTask)
    et maintient les listes de dépendances (prédécesseurs) et
    de dépendants (successeurs). Le niveau (level) indique la
    profondeur topologique et permet le parallélisme.

    Attributes:
        task: La tâche TaskIQ décorée
        dependencies: Liste des nœuds dont ce nœud dépend
        dependents: Liste des nœuds dépendant de celui-ci
        level: Niveau topologique (0 = pas de dépendances)

    Example:
        Création d'un nœud pour une tâche:

        >>> node = DAGNode(task=my_task)
        >>> print(f"Tâche: {node.task_name}")
        >>> print(f"Niveau initial: {node.level}")
    """

    task: Any
    dependencies: list["DAGNode"] = field(default_factory=list)
    dependents: list["DAGNode"] = field(default_factory=list)
    level: int = 0  # Topological level (0 = no dependencies)

    def __hash__(self) -> int:
        """Hash basé sur la tâche (uniquauté)."""
        return hash(self.task)

    def __eq__(self, other: object) -> bool:
        """Deux DAGNode sont égaux si ils représentent la même tâche."""
        if not isinstance(other, DAGNode):
            return False
        return self.task is other.task

    @property
    def task_name(self) -> str:
        """Nom de la tâche (propriété de convenance)."""
        return self.task.task_name


@dataclass
class DAG:
    """
    Graphe Orienté Acyclique (DAG) des dépendances de tâches.

    Structure centrale de l'orchestration dataflow. Permet de:
    - Ajouter des nœuds et des arêtes
    - Calculer un ordre topologique d'exécution
    - Déterminer les niveaux de parallélisme
    - Identifier les tâches prêtes à être exécutées

    Le DAG est construit automatiquement par DataflowRegistry.build_dag()
    mais peut aussi être construit manuellement pour des cas avancés.

    Example:
        Construction manuelle d'un DAG simple:

        >>> dag = DAG()
        >>>
        >>> # Création des nœuds
        >>> node_a = DAGNode(task=task_a)
        >>> node_b = DAGNode(task=task_b)
        >>> node_c = DAGNode(task=task_c)
        >>>
        >>> # Ajout des nœuds au graphe
        >>> dag.add_node(node_a)
        >>> dag.add_node(node_b)
        >>> dag.add_node(node_c)
        >>>
        >>> # Définition des dépendances: a -> b -> c
        >>> dag.add_edge(node_a, node_b)
        >>> dag.add_edge(node_b, node_c)
        >>>
        >>> # Calcul des niveaux topologiques
        >>> dag.compute_levels()
        >>> print(f"Nombre de niveaux: {len(dag.levels)}")
        >>> print(f"Tâches au niveau 0: {len(dag.levels[0])}")

    Example:
        Utilisation avec DataflowRegistry:

        >>> from taskiq_flow.dataflow import DataflowRegistry
        >>> registry = DataflowRegistry()
        >>> registry.register_task(task_a, output="data_a", inputs=[])
        >>> registry.register_task(task_b, output="data_b", inputs=["data_a"])
        >>> dag = registry.build_dag()
        >>>
        >>> # Afficher l'ordre d'exécution
        >>> ordered = dag.topological_sort()
        >>> for i, node in enumerate(ordered):
        ...     print(f"{i}: {node.task_name}")
    """

    nodes: list[DAGNode] = field(default_factory=list)
    edges: list[tuple[DAGNode, DAGNode]] = field(default_factory=list)
    levels: list[list[DAGNode]] = field(default_factory=list)

    def add_node(self, node: DAGNode) -> None:
        """
        Ajoute un nœud au graphe DAG.

        Si le nœud est déjà présent, il n'est pas ajouté de nouveau.

        Args:
            node: Le nœud DAGNode à ajouter

        Example:
            >>> dag = DAG()
            >>> my_node = DAGNode(task=my_task)
            >>> dag.add_node(my_node)
            >>> len(dag.nodes)
            1
        """
        if node not in self.nodes:
            self.nodes.append(node)

    def add_edge(self, from_node: DAGNode, to_node: DAGNode) -> None:
        """
        Ajoute une arête dirigée de from_node vers to_node.

        Crée une dépendance où from_node doit s'exécuter avant to_node.
        Si l'arête existe déjà, elle n'est pas dupliquée.

        Args:
            from_node: Nœud source (producteur)
            to_node: Nœud destination (consommateur)

        Raises:
            ValueError: Si from_node ou to_node ne sont pas dans le DAG

        Example:
            >>> dag = DAG()
            >>> node_a = DAGNode(task=task_a)
            >>> node_b = DAGNode(task=task_b)
            >>> dag.add_node(node_a)
            >>> dag.add_node(node_b)
            >>> dag.add_edge(node_a, node_b)
            >>> len(dag.edges)
            1
        """
        if from_node not in self.nodes:
            raise ValueError(f"from_node {from_node} not in DAG")
        if to_node not in self.nodes:
            raise ValueError(f"to_node {to_node} not in DAG")

        if (from_node, to_node) not in self.edges:
            self.edges.append((from_node, to_node))
            from_node.dependents.append(to_node)
            to_node.dependencies.append(from_node)

    def get_node_by_task(self, task: Any) -> DAGNode | None:
        """
        Trouve le nœud correspondant à une tâche donnée.

        Args:
            task: La tâche à rechercher

        Returns:
            Le DAGNode correspondant si trouvé, None sinon

        Example:
            >>> dag = DAG()
            >>> # ... ajout de nœuds ...
            >>> node = dag.get_node_by_task(my_task)
            >>> if node:
            ...     print(f"Tâche trouvée: {node.task_name}")
        """
        for node in self.nodes:
            if node.task == task:
                return node
        return None

    def topological_sort(self) -> list[DAGNode]:
        """
        Retourne les nœuds dans un ordre topologique.

        L'ordre topologique garantit que chaque nœud apparaît après
        tous ses prédécesseurs (dépendances). C'est l'ordre d'exécution
        séquentiel valide pour le DAG.

        Returns:
            Liste des nœuds en ordre topologique

        Raises:
            ValueError: Si une dépendance circulaire est détectée

        Example:
            >>> dag = DAG()
            >>> # Construction d'un DAG: a -> b -> c
            >>> dag.add_edge(node_a, node_b)
            >>> dag.add_edge(node_b, node_c)
            >>> order = dag.topological_sort()
            >>> # Vérifier l'ordre: node_a doit venir avant node_b, etc.
            >>> assert order.index(node_a) < order.index(node_b)
            >>> assert order.index(node_b) < order.index(node_c)
        """
        # Use index-based tracking instead of node as dict key
        in_degree = [len(node.dependencies) for node in self.nodes]
        queue = [i for i, node in enumerate(self.nodes) if in_degree[i] == 0]
        result = []

        while queue:
            idx = queue.pop(0)
            node = self.nodes[idx]
            result.append(node)

            for dependent in node.dependents:
                dep_idx = self.nodes.index(dependent)
                in_degree[dep_idx] -= 1
                if in_degree[dep_idx] == 0:
                    queue.append(dep_idx)

        if len(result) != len(self.nodes):
            raise ValueError("Circular dependency detected in DAG")

        return result

    def compute_levels(self) -> None:
        """
        Calcule les niveaux topologiques pour l'exécution parallèle.

        Après cet appel, chaque nœud a son attribut `level` défini,
        et la liste `levels` contient les groupes de nœuds pouvant
        s'exécuter en parallèle (niveau 0 = pas de dépendances,
        niveau 1 = dépend du niveau 0, etc.).

        Example:
            >>> dag = DAG()
            >>> # Construction: a -> (b, c) -> d
            >>> dag.add_edge(node_a, node_b)
            >>> dag.add_edge(node_a, node_c)
            >>> dag.add_edge(node_b, node_d)
            >>> dag.add_edge(node_c, node_d)
            >>>
            >>> dag.compute_levels()
            >>>
            >>> # Vérifier les niveaux
            >>> print(f"Niveaux totaux: {len(dag.levels)}")
            >>> print(f"Niveau 0: {[n.task_name for n in dag.levels[0]]}")
            >>> print(f"Niveau 1: {[n.task_name for n in dag.levels[1]]}")
            >>> print(f"Niveau 2: {[n.task_name for n in dag.levels[2]]}")

        Note:
            Cette méthode appelle `topological_sort()` en interne.
            Si une dépendance circulaire est détectée, une ValueError est levée.
        """
        sorted_nodes = self.topological_sort()

        # Reset levels
        for node in self.nodes:
            node.level = 0

        # Compute levels based on dependencies
        for node in sorted_nodes:
            if node.dependencies:
                node.level = max(dep.level for dep in node.dependencies) + 1

        # Group nodes by level
        max_level = max((node.level for node in self.nodes), default=0)
        self.levels = [[] for _ in range(max_level + 1)]
        for node in self.nodes:
            self.levels[node.level].append(node)

    def get_ready_tasks(self, completed: set[DAGNode]) -> list[DAGNode]:
        """
        Retourne les tâches prêtes pour exécution.

        Une tâche est prête si toutes ses dépendances ont été complétées
        (présentes dans l'ensemble `completed`).

        Args:
            completed: Ensemble des nœuds déjà exécutés

        Returns:
            Liste des nœuds prêts à être exécutés

        Example:
            >>> dag = DAG()
            >>> # Construction: a -> b, a -> c
            >>> dag.add_edge(node_a, node_b)
            >>> dag.add_edge(node_a, node_c)
            >>> dag.compute_levels()
            >>>
            >>> # Après exécution de a
            >>> completed = {node_a}
            >>> ready = dag.get_ready_tasks(completed)
            >>> ready_tasks = {n.task for n in ready}
            >>> assert node_b.task in ready_tasks
            >>> assert node_c.task in ready_tasks
            >>> print(f"Tâches prêtes: {[n.task_name for n in ready]}")
        """
        ready = []
        for node in self.nodes:
            if node not in completed and all(
                dep in completed for dep in node.dependencies
            ):
                ready.append(node)
        return ready
