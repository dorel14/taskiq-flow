"""Graphe Orienté Acyclique (DAG) pour l'exécution de pipeline.

Ce module définit les classes DAG et DAGNode pour représenter
la structure de dépendances entre tâches. Le DAG permet de
déterminer l'ordre d'exécution et les niveaux de parallélisme.

Auteur: SoniqueBay Team
Version: 0.3.2
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass
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
    """

    task: Any
    dependencies: list["DAGNode"] = field(default_factory=list)
    dependents: list["DAGNode"] = field(default_factory=list)
    level: int = 0  # Topological level (0 = no dependencies)

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

    Utilisation:
        dag = DAG()
        node_a = DAGNode(task=task_a)
        node_b = DAGNode(task=task_b)
        dag.add_node(node_a)
        dag.add_node(node_b)
        dag.add_edge(node_a, node_b)  # a -> b

        order = dag.topological_sort()
        dag.compute_levels()  # pour exécution parallèle par niveau
    """

    nodes: list[DAGNode] = field(default_factory=list)
    edges: list[tuple[DAGNode, DAGNode]] = field(default_factory=list)
    levels: list[list[DAGNode]] = field(default_factory=list)

    def add_node(self, node: DAGNode) -> None:
        """Add a node to the DAG."""
        if node not in self.nodes:
            self.nodes.append(node)

    def add_edge(self, from_node: DAGNode, to_node: DAGNode) -> None:
        """Add a directed edge from -> to."""
        if (from_node, to_node) not in self.edges:
            self.edges.append((from_node, to_node))
            from_node.dependents.append(to_node)
            to_node.dependencies.append(from_node)

    def get_node_by_task(self, task: Any) -> DAGNode | None:
        """Find a node by its task."""
        for node in self.nodes:
            if node.task == task:
                return node
        return None

    def topological_sort(self) -> list[DAGNode]:
        """Return nodes in topological order."""
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
        """Compute topological levels for parallel execution."""
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
        Get tasks that are ready to execute.

        A task is ready if all its dependencies are completed.
        """
        ready = []
        for node in self.nodes:
            if node not in completed and all(
                dep in completed for dep in node.dependencies
            ):
                ready.append(node)
        return ready
