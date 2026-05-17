"""
Example: Audio processing pipeline using dataflow architecture.

This example demonstrates how to build an audio processing pipeline
using the dataflow-based approach, where tasks automatically determine
their execution order based on data dependencies.
"""

import asyncio
from typing import Any

from taskiq import InMemoryBroker

from taskiq_flow import (
    DataflowPipeline,
    pipeline_task,
)

broker = InMemoryBroker(await_inplace=True)
# ============================================================================
# Define audio processing tasks
# ============================================================================


@broker.task
@pipeline_task(output="audio_features")
async def extract_audio_features(track_paths: list[str]) -> dict[str, Any]:
    """
    Extract audio features from track paths.

    This task produces 'audio_features' which will be consumed
    by downstream tasks.
    """
    # Simulate feature extraction
    features = {
        "track": track_paths[0] if track_paths else "unknown",
        "duration": 180.0,
        "sample_rate": 44100,
        "channels": 2,
    }
    await asyncio.sleep(0.1)  # Simulate processing
    return features


@broker.task
@pipeline_task(output="mir_features")
async def compute_mir_features(audio_features: dict[str, Any]) -> dict[str, Any]:
    """
    Compute Music Information Retrieval (MIR) features.

    This task automatically receives 'audio_features' as input
    because it declares a parameter with that name.
    """
    # Simulate MIR computation
    mir = {
        "tempo": 120.0,
        "key": "C major",
        "loudness": -12.5,
        "spectral_centroid": 2500.0,
    }
    await asyncio.sleep(0.2)  # Simulate processing
    return mir


@broker.task
@pipeline_task(output="tags")
async def generate_tags(mir_features: dict[str, Any]) -> list[str]:
    """
    Generate genre/style tags from MIR features.

    Automatically receives 'mir_features' as input.
    """
    # Simulate tag generation
    tags = ["electronic", "dance", "upbeat"]
    await asyncio.sleep(0.1)  # Simulate processing
    return tags


@broker.task
@pipeline_task(output="vector")
async def create_embedding(
    mir_features: dict[str, Any],
    tags: list[str],
) -> list[float]:
    """
    Create embedding vector from features and tags.

    This task receives BOTH 'mir_features' and 'tags' as inputs,
    demonstrating multiple dependencies.
    """
    # Simulate embedding creation
    vector = [0.1, 0.5, 0.8, 0.3, 0.9]
    await asyncio.sleep(0.15)  # Simulate processing
    return vector


# ============================================================================
# Example 1: Sequential pipeline with automatic dependencies
# ============================================================================


async def example_sequential_pipeline() -> None:
    """
    Example 1: Sequential pipeline with automatic dependency resolution.

    The pipeline automatically determines that:
    1. extract_audio_features runs first (no dependencies)
    2. compute_mir_features runs second (depends on audio_features)
    3. generate_tags runs third (depends on mir_features)
    4. create_embedding runs last (depends on mir_features and tags)
    """
    # Create broker
    broker = InMemoryBroker(await_inplace=True)

    # Create pipeline from tasks
    pipeline = DataflowPipeline.from_tasks(
        broker,
        [
            extract_audio_features,  # type: ignore
            compute_mir_features,  # type: ignore
            generate_tags,  # type: ignore
            create_embedding,  # type: ignore
        ],
    )

    # Visualize the pipeline
    pipeline.print_dag()

    # Execute pipeline
    results = await pipeline.kiq_dataflow(track_paths=["track1.mp3", "track2.mp3"])

    for _key, _value in results.items():
        pass


# ============================================================================
# Example 2: Parallel execution
# ============================================================================


@broker.task
@pipeline_task(output="spectral_features")
async def extract_spectral_features(audio_features: dict[str, Any]) -> dict[str, float]:
    """Extract spectral features (runs in parallel with compute_mir_features)."""
    # Simulate spectral feature extraction
    await asyncio.sleep(0.2)
    return {"spectral_rolloff": 5000.0, "zero_crossing_rate": 0.05}


@broker.task
@pipeline_task(output="combined_features")
async def combine_features(
    mir_features: dict[str, Any],
    spectral_features: dict[str, float],
    tags: list[str],
) -> dict[str, Any]:
    """Combine all features."""
    return {
        **mir_features,
        **spectral_features,
        "tags": tags,
    }


async def example_parallel_pipeline() -> None:
    """
    Example 2: Pipeline with parallel execution.

    compute_mir_features and extract_spectral_features run in parallel
    because they both only depend on audio_features.
    """
    broker = InMemoryBroker(await_inplace=True)

    pipeline = DataflowPipeline.from_tasks(
        broker,
        [
            extract_audio_features,  # type: ignore
            compute_mir_features,  # type: ignore
            extract_spectral_features,  # type: ignore
            generate_tags,  # type: ignore
            combine_features,  # type: ignore
        ],
    )

    pipeline.print_dag()

    results = await pipeline.kiq_dataflow(track_paths=["track1.mp3"])

    for _key, _value in results.items():
        pass


# ============================================================================
# Example 3: Map-Reduce for batch processing
# ============================================================================


@broker.task
@pipeline_task(output="track_features")
async def process_single_track(track: str) -> dict[str, Any]:
    """Process a single track."""
    await asyncio.sleep(0.1)
    return {
        "track": track,
        "duration": 180.0,
        "bpm": 120 + hash(track) % 40,
    }


@broker.task
@pipeline_task(output="playlist_stats")
async def aggregate_track_features(
    track_features: list[dict[str, Any]],
) -> dict[str, Any]:
    """Aggregate features from multiple tracks."""
    total_duration = sum(t["duration"] for t in track_features)
    avg_bpm = sum(t["bpm"] for t in track_features) / len(track_features)
    return {
        "total_tracks": len(track_features),
        "total_duration": total_duration,
        "avg_bpm": avg_bpm,
    }


async def example_map_reduce() -> None:
    """
    Example 3: Map-Reduce for batch processing.

    Process multiple tracks in parallel, then aggregate results.
    """
    # Use the module-level broker which has tasks registered
    pipeline = DataflowPipeline(broker)

    # Add map-reduce operations
    tracks = ["track1.mp3", "track2.mp3", "track3.mp3", "track4.mp3"]

    pipeline.map(
        process_single_track,  # type: ignore
        tracks,
        output="track_features",
        max_parallel=4,
    )

    pipeline.reduce(
        aggregate_track_features,  # type: ignore
        input_name="track_features",
        output="playlist_stats",
    )

    pipeline.print_dag()

    results = await pipeline.kiq_map_reduce()

    for _key, _value in results.items():
        pass


# ============================================================================
# Example 4: Visualization
# ============================================================================


async def example_visualization() -> None:
    """
    Example 4: Pipeline visualization.

    Generate DOT format for visualization with Graphviz.
    """
    pipeline = DataflowPipeline.from_tasks(
        broker,
        [
            extract_audio_features,  # type: ignore
            compute_mir_features,  # type: ignore
            generate_tags,  # type: ignore
            create_embedding,  # type: ignore
        ],
    )

    # Generate DOT format
    pipeline.visualize_dot()

    # Generate JSON
    pipeline.visualize()


# ============================================================================
# Main
# ============================================================================


async def main() -> None:
    """Run all examples."""
    # Example 1: Sequential pipeline
    await example_sequential_pipeline()

    # Example 2: Parallel pipeline
    await example_parallel_pipeline()

    # Example 3: Map-reduce
    await example_map_reduce()

    # Example 4: Visualization
    await example_visualization()


if __name__ == "__main__":
    asyncio.run(main())
