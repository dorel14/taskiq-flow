"""DAG representation for pipeline execution."""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class DAGNode:
    """Represents a task node in the DAG."""

    task: Any
    dependencies: list["DAGNode"] = field(default_factory=list)
    dependents: list["DAGNode"] = field(default_factory=list)
    level: int = 0  # Topological level (0 = no dependencies)

    @property
    def task_name(self) -> str:
        """Get the task name."""
        return self.task.task_name


@dataclass
class DAG:
    """Directed Acyclic Graph representing task dependencies."""

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
