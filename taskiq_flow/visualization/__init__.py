"""Visualisation du DAG pour le suivi des pipelines.

Ce module fournit DAGVisualizer capable de générer des représentations
du graphe de dépendances en JSON (pour UI web), DOT (pour Graphviz)
et ASCII (pour terminal). Utile pour le debug et le monitoring.

Auteur: SoniqueBay Team
Version: 0.3.1
"""

from typing import Any

from taskiq_flow.dataflow.dag import DAG

__all__ = ["DAG"]


class DAGVisualizer:
    """
    Générateur de visualisations du DAG en plusieurs formats.

    Fournit des méthodes statiques pour convertir un DAG en:
    - JSON (pour UI web)
    - DOT (pour Graphviz -> images PNG/SVG)
    - ASCII (pour terminal)
    - NetworkX (pour analyse Python)

    Usage:
        json_repr = DAGVisualizer.to_json(dag)
        dot = DAGVisualizer.to_dot(dag)
        DAGVisualizer.print_ascii(dag)
    """

    @staticmethod
    def to_json(dag: DAG) -> dict[str, Any]:
        """
        Convertit le DAG en représentation JSON.

        Structure:
            {
                "nodes": [{"id":..., "label":..., "level":...}, ...],
                "edges": [{"source":..., "target":...}, ...],
                "levels": [[node_name_per_level0], [level1], ...]
            }

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
    def to_dot(dag: DAG) -> str:
        """
        Convert DAG to DOT format for Graphviz.

        Args:
            dag: DAG to convert

        Returns:
            DOT format string

        Example:
            dot = DAGVisualizer.to_dot(dag)
            print(dot)
            # Save to file: graph.dot
            # Generate image: dot -Tpng graph.dot -o graph.png
        """
        lines = ["digraph DAG {"]
        lines.append("  rankdir=LR;")
        lines.append("  node [shape=box, style=rounded];")
        lines.append("")

        # Group nodes by level
        for level_idx, level in enumerate(dag.levels):
            lines.append(f"  // Level {level_idx}")
            for node in level:
                label = node.task.task_name
                lines.append(f'  "{label}";')
            lines.append("")

        # Add edges
        lines.append("  // Dependencies")
        for from_node, to_node in dag.edges:
            lines.append(
                f'  "{from_node.task.task_name}" -> "{to_node.task.task_name}";',
            )

        lines.append("}")

        return "\n".join(lines)

    @staticmethod
    def to_networkx(dag: DAG) -> Any:
        """
        Convert DAG to NetworkX graph.

        Args:
            dag: DAG to convert

        Returns:
            NetworkX DiGraph object

        Raises:
            ImportError: If NetworkX is not installed

        Example:
            import networkx as nx
            import matplotlib.pyplot as plt

            graph = DAGVisualizer.to_networkx(dag)
            nx.draw(graph, with_labels=True)
            plt.show()
        """
        try:
            import networkx as nx  # noqa: PLC0415
        except ImportError as err:
            raise ImportError(
                "NetworkX is required for DAG visualization. "
                "Install with: pip install networkx",
            ) from err

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
    def print_ascii(dag: DAG) -> None:
        """
        Print ASCII representation of DAG.

        Args:
            dag: DAG to print

        Example:
            DAGVisualizer.print_ascii(dag)
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


def visualize_pipeline(pipeline: Any) -> dict[str, Any]:
    """
    Visualize a pipeline's DAG.

    Args:
        pipeline: Pipeline instance

    Returns:
        JSON representation of DAG

    Example:
        pipeline = Pipeline.from_tasks(broker, tasks)
        viz = visualize_pipeline(pipeline)
        print(json.dumps(viz, indent=2))
    """
    if hasattr(pipeline, "dag") and pipeline.dag:
        dag = pipeline.dag
    elif hasattr(pipeline, "_build_dag"):
        pipeline._build_dag()
        dag = pipeline.dag
    else:
        raise ValueError("Pipeline has no DAG. Call _build_dag() first.")

    return DAGVisualizer.to_json(dag)


__all__ = [
    "DAGVisualizer",
    "visualize_pipeline",
]
