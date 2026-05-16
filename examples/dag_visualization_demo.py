"""
Démonstration de visualisation avancée de DAG pour Taskiq-Flow.

Ce module démontre les capacités de visualisation et d'analyse de DAG
utilisant NetworkX pour l'analyse de graphes, Mermaid pour les diagrammes
et les formats d'export JSON, DOT, Cytoscape et ASCII.

Exemples inclus :
    - DAGVisualizer : analyse critique, groupes parallèles, exports multiples
    - MermaidGenerator : génération de diagrammes stylisés
    - Visualisation ASCII pour terminal

Auteur: SoniqueBay Team
Version: 1.2.0
"""

import asyncio
import logging
from typing import Any

from taskiq import InMemoryBroker

from taskiq_flow import DataflowPipeline, pipeline_task
from taskiq_flow.visualization.dag_visualizer import DAGVisualizer
from taskiq_flow.visualization.mermaid import MermaidGenerator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create broker
broker = InMemoryBroker(await_inplace=True)


# Define tasks for a sample audio processing pipeline
@broker.task
@pipeline_task(output="audio_features")
def extract_features(audio_path: str) -> dict[str, Any]:
    """Extract audio features."""
    return {"duration": 180.0, "tempo": 120.0, "sample_rate": 44100}


@broker.task
@pipeline_task(output="tags")
def generate_tags(audio_features: dict[str, Any]) -> list[str]:
    """Generate tags based on audio features."""
    return ["electronic", "dance", "upbeat"]


@broker.task
@pipeline_task(output="embedding")
def compute_embedding(audio_features: dict[str, Any]) -> list[float]:
    """Compute vector embedding."""
    return [0.1, 0.2, 0.3, 0.4, 0.5]


@broker.task
@pipeline_task(output="metadata")
def create_metadata(
    audio_features: dict[str, Any], tags: list[str], embedding: list[float]
) -> dict[str, Any]:
    """Combine all results into final metadata."""
    return {
        "features": audio_features,
        "tags": tags,
        "embedding": embedding,
    }


# Build pipeline
pipeline = DataflowPipeline.from_tasks(
    broker,
    [
        extract_features,  # type: ignore
        generate_tags,  # type: ignore
        compute_embedding,  # type: ignore
        create_metadata,  # type: ignore
    ],
)
pipeline.pipeline_id = "audio_analysis_demo"


async def main() -> None:
    """Run the DAG visualization demo."""
    logger.info("=== Taskiq-Flow DAG Visualization Demo ===\n")

    # Build DAG (without executing) - internal method
    pipeline._build_dataflow_dag()

    # Get the DAG
    dag = pipeline._dag
    if dag is None:
        logger.warning("No DAG built")
        return

    logger.info(f"DAG has {len(dag.nodes)} nodes and {len(dag.edges)} edges\n")

    # 1. NetworkX-based visualization
    logger.info("1. NetworkX DAG Analysis")
    logger.info("-" * 40)

    # Export as JSON
    json_data = DAGVisualizer.to_json(dag)
    logger.info(f"   Nodes: {len(json_data['nodes'])}")
    logger.info(f"   Edges: {len(json_data['edges'])}")
    logger.info(f"   Topological order: {json_data['levels'][:3]}...")

    # Critical path
    critical_path = DAGVisualizer.detect_critical_path(dag)
    logger.info(f"   Critical path: {' -> '.join(critical_path)}")

    # Parallel groups
    parallel_groups = DAGVisualizer.find_parallelizable_groups(dag)
    logger.info(f"   Parallel groups: {len(parallel_groups)} levels")
    for i, group in enumerate(parallel_groups):
        logger.info(f"     Level {i}: {group}")

    # 2. Mermaid generation
    logger.info("\n2. Mermaid Diagram")
    logger.info("-" * 40)
    mermaid_gen = MermaidGenerator(dag)
    mermaid_code = mermaid_gen.to_mermaid_with_styling(orientation="LR")
    logger.info(mermaid_code)

    # 3. Graphviz DOT
    logger.info("\n3. Graphviz DOT")
    logger.info("-" * 40)
    dot = DAGVisualizer.to_dot(dag)
    logger.info(dot[:500] + "..." if len(dot) > 500 else dot)

    # 4. Cytoscape JSON (abbreviated)
    logger.info("\n4. Cytoscape JSON (for web visualization)")
    logger.info("-" * 40)
    cytoscape = DAGVisualizer.to_cytoscape_json(dag)
    logger.info(
        f"Elements: {len(cytoscape['nodes'])} nodes,\
                {len(cytoscape['edges'])} edges"
    )

    logger.info("\n=== Demo Complete ===")
    logger.info("\nAll visualization formats generated successfully!")
    logger.info("Check the output above for Mermaid, DOT, and JSON formats.")


if __name__ == "__main__":
    asyncio.run(main())
