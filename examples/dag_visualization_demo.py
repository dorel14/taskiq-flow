"""DAG Visualization Demo

This example demonstrates the advanced DAG visualization capabilities
using NetworkX, Mermaid, and NiceGUI integration.

Author: SoniqueBay Team
Version: 0.4.5
"""

import asyncio
from taskiq import InMemoryBroker
from taskiq_flow import DataflowPipeline, pipeline_task
from taskiq_flow.visualization.dag_visualizer import DAGVisualizer
from taskiq_flow.visualization.mermaid import MermaidGenerator
from taskiq_flow.dataflow.dag import DAG


# Create broker
broker = InMemoryBroker(await_inplace=True)


# Define tasks for a sample audio processing pipeline
@broker.task
@pipeline_task(output="audio_features")
def extract_features(audio_path: str) -> dict:
    """Extract audio features."""
    return {"duration": 180.0, "tempo": 120.0, "sample_rate": 44100}


@broker.task
@pipeline_task(output="tags")
def generate_tags(audio_features: dict) -> list[str]:
    """Generate tags based on audio features."""
    return ["electronic", "dance", "upbeat"]


@broker.task
@pipeline_task(output="embedding")
def compute_embedding(audio_features: dict) -> list[float]:
    """Compute vector embedding."""
    return [0.1, 0.2, 0.3, 0.4, 0.5]


@broker.task
@pipeline_task(output="metadata")
def create_metadata(
    audio_features: dict,
    tags: list[str],
    embedding: list[float]
) -> dict:
    """Combine all results into final metadata."""
    return {
        "features": audio_features,
        "tags": tags,
        "embedding": embedding,
    }


# Build pipeline
pipeline = DataflowPipeline.from_tasks(
    broker,
    [extract_features, generate_tags, compute_embedding, create_metadata]
)
pipeline.pipeline_id = "audio_analysis_demo"


async def main():
    print("=== Taskiq-Flow DAG Visualization Demo ===\n")

    # Build DAG (without executing)
    dag = pipeline.build_dag()

    print(f"DAG has {len(dag.nodes)} nodes and {len(dag.edges)} edges\n")

    # 1. NetworkX-based visualization
    print("1. NetworkX DAG Analysis")
    print("-" * 40)
    visualizer = DAGVisualizer(dag)

    # Export as JSON
    json_data = visualizer.to_json()
    print(f"   Nodes: {len(json_data['nodes'])}")
    print(f"   Edges: {len(json_data['edges'])}")
    print(f"   Is DAG: {not json_data['is_cyclic']}")
    print(f"   Topological order: {json_data['topological_order'][:3]}...")

    # Critical path
    critical_path = visualizer.detect_critical_path()
    print(f"   Critical path: {' -> '.join(critical_path)}")

    # Parallel groups
    parallel_groups = visualizer.find_parallelizable_groups()
    print(f"   Parallel groups: {len(parallel_groups)} levels")
    for i, group in enumerate(parallel_groups):
        print(f"     Level {i}: {group}")

    # 2. Mermaid generation
    print("\n2. Mermaid Diagram")
    print("-" * 40)
    mermaid_gen = MermaidGenerator(dag)
    mermaid_code = mermaid_gen.to_mermaid_with_styling(orientation="LR")
    print(mermaid_code)

    # 3. ASCII art
    print("\n3. ASCII Art")
    print("-" * 40)
    ascii_art = visualizer.visualize_ascii()
    print(ascii_art)

    # 4. Graphviz DOT
    print("\n4. Graphviz DOT")
    print("-" * 40)
    dot = visualizer.to_graphviz()
    print(dot[:500] + "..." if len(dot) > 500 else dot)

    # 5. Cytoscape JSON (abbreviated)
    print("\n5. Cytoscape JSON (for web visualization)")
    print("-" * 40)
    cytoscape = visualizer.to_cytoscape_json()
    print(f"   Elements: {len(cytoscape['nodes'])} nodes, {len(cytoscape['edges'])} edges")

    print("\n=== Demo Complete ===")
    print("\nAll visualization formats generated successfully!")
    print("Check the output above for Mermaid, ASCII, DOT, and JSON formats.")


if __name__ == "__main__":
    asyncio.run(main())
