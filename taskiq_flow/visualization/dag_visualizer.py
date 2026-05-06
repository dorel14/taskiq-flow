"""Visualiseur de DAG avec NetworkX.

Ce module fournit des fonctionnalités avancées de visualisation
utilisant NetworkX pour l'analyse de graphes.

Auteur: SoniqueBay Team
Version: 0.4.5
"""

from typing import Any

import networkx as nx

from taskiq_flow.dataflow.dag import DAG


class DAGVisualizer:
    """Visualiseur de DAG avec NetworkX."""

    @staticmethod
    def to_json(dag: DAG) -> dict[str, Any]:
        """
        Convertit le DAG en représentation JSON.

        Args:
            dag: DAG à convertir

        Returns:
            Dictionnaire sérialisable en JSON
        """
        nodes = []
        for node in dag.nodes:
            metadata = {
                "id": node.task.task_name,
                "label": node.task.task_name,
                "level": node.level,
                "state": "pending",
            }
            nodes.append(metadata)

        edges = []
        for from_node, to_node in dag.edges:
            edges.append(
                {
                    "source": from_node.task.task_name,
                    "target": to_node.task.task_name,
                },
            )

        return {
            "nodes": nodes,
            "edges": edges,
            "levels": [[node.task.task_name for node in level] for level in dag.levels],
        }

    @staticmethod
    def to_json_extended(dag: DAG) -> dict[str, Any]:
        """
        Convertit le DAG en représentation JSON étendue.

        Args:
            dag: DAG à convertir

        Returns:
            Dictionnaire sérialisable en JSON avec métadonnées
        """
        base_json = DAGVisualizer.to_json(dag)
        base_json["is_cyclic"] = not dag.is_dag()
        base_json["node_count"] = len(dag.nodes)
        base_json["edge_count"] = len(dag.edges)
        base_json["level_count"] = len(dag.levels)
        return base_json

    @staticmethod
    def to_cytoscape_json(dag: DAG) -> dict[str, Any]:
        """
        Convertit le DAG en format Cytoscape.js.

        Args:
            dag: DAG à convertir

        Returns:
            Dictionnaire au format Cytoscape.js
        """
        elements: dict[str, list[Any]] = {"nodes": [], "edges": []}

        for node in dag.nodes:
            elements["nodes"].append(
                {
                    "data": {
                        "id": node.task.task_name,
                        "label": node.task.task_name,
                        "level": node.level,
                    },
                    "position": {"x": 0, "y": 0},
                }
            )

        for from_node, to_node in dag.edges:
            elements["edges"].append(
                {
                    "data": {
                        "id": f"{from_node.task.task_name}->{to_node.task.task_name}",
                        "source": from_node.task.task_name,
                        "target": to_node.task.task_name,
                    }
                }
            )

        return elements

    @staticmethod
    def detect_critical_path(dag: DAG) -> list[str]:
        """
        Identifie le chemin critique dans le DAG.

        Args:
            dag: DAG à analyser

        Returns:
            Liste des noms de tâches formant le chemin critique

        Raises:
            ValueError: Si le DAG contient des cycles
        """
        graph = nx.DiGraph()
        for node in dag.nodes:
            graph.add_node(node.task.task_name, level=node.level)
        for from_node, to_node in dag.edges:
            graph.add_edge(from_node.task.task_name, to_node.task.task_name)

        if not nx.is_directed_acyclic_graph(graph):
            raise ValueError("Cannot compute critical path for cyclic graph")

        longest_paths: dict[str, tuple[int, list[str]]] = {
            node: (0, []) for node in graph.nodes()
        }

        for node in nx.topological_sort(graph):
            for predecessor in graph.predecessors(node):
                pred_length, pred_path = longest_paths[predecessor]
                if pred_length + 1 > longest_paths[node][0]:
                    longest_paths[node] = (
                        pred_length + 1,
                        [*pred_path, predecessor],
                    )

        end_node = max(longest_paths, key=lambda n: longest_paths[n][0])
        _, path = longest_paths[end_node]
        return [*path, end_node]

    @staticmethod
    def find_parallelizable_groups(dag: DAG) -> list[list[str]]:
        """
        Identifie les groupes de tâches parallélisables.

        Args:
            dag: DAG à analyser

        Returns:
            Liste de groupes de tâches parallélisables

        Raises:
            ValueError: Si le DAG contient des cycles
        """
        graph = nx.DiGraph()
        for node in dag.nodes:
            graph.add_node(node.task.task_name, level=node.level)
        for from_node, to_node in dag.edges:
            graph.add_edge(from_node.task.task_name, to_node.task.task_name)

        if not nx.is_directed_acyclic_graph(graph):
            raise ValueError("Cannot compute parallel groups for cyclic graph")

        levels: dict[int, list[str]] = {}
        for node in nx.topological_sort(graph):
            level = 0
            for pred in graph.predecessors(node):
                pred_level = next(
                    (lvl for lvl, nodes in levels.items() if pred in nodes), 0
                )
                level = max(level, pred_level + 1)

            if level not in levels:
                levels[level] = []
            levels[level].append(node)

        return [levels[level] for level in sorted(levels.keys())]

    @staticmethod
    def to_networkx(dag: DAG) -> Any:
        """
        Convert DAG to NetworkX graph.

        Args:
            dag: DAG à convertir

        Returns:
            NetworkX DiGraph object
        """
        graph = nx.DiGraph()

        for node in dag.nodes:
            graph.add_node(node.task.task_name, level=node.level)

        for from_node, to_node in dag.edges:
            graph.add_edge(
                from_node.task.task_name,
                to_node.task.task_name,
            )

        return graph

    @staticmethod
    def to_dot(dag: DAG) -> str:
        """
        Convert DAG to DOT format.

        Args:
            dag: DAG à convertir

        Returns:
            DOT format string
        """
        lines = ["digraph DAG {"]
        lines.append("  rankdir=LR;")
        lines.append("  node [shape=box, style=rounded];")
        lines.append("")

        for level_idx, level in enumerate(dag.levels):
            lines.append(f"  // Level {level_idx}")
            for node in level:
                label = node.task.task_name
                lines.append(f'  "{label}";')
            lines.append("")

        lines.append("  // Dependencies")
        for from_node, to_node in dag.edges:
            lines.append(
                f'  "{from_node.task.task_name}" -> "{to_node.task.task_name}";',
            )

        lines.append("}")
        return "\n".join(lines)

    @staticmethod
    def print_ascii(dag: DAG) -> None:
        """
        Print ASCII representation of DAG.

        Args:
            dag: DAG to print
        """
        if not dag.levels:
            dag.compute_levels()

        print("\nPipeline DAG:")  # noqa: T201
        print("=" * 50)  # noqa: T201

        for level_idx, level in enumerate(dag.levels):
            print(f"\nLevel {level_idx}:")  # noqa: T201
            for node in level:
                deps = [d.task.task_name for d in node.dependencies]
                if deps:
                    print(f"  {node.task.task_name}")  # noqa: T201
                    print(f"    <- {', '.join(deps)}")  # noqa: T201
                else:
                    print(f"  {node.task.task_name} (input)")  # noqa: T201

        print("\n" + "=" * 50)  # noqa: T201


__all__ = ["DAGVisualizer"]
