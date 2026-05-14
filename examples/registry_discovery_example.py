"""
Example: Automatic task discovery and pipeline construction using DataflowRegistry.

This example demonstrates how to use DataflowRegistry to:
- Manually register tasks with their data dependencies
- Inspect the dataflow graph before execution
- Build a DAG automatically from registered tasks
- Execute the pipeline using the ExecutionEngine
- Understand data provenance and task dependencies

This is the core of taskiq-flow's automatic dependency resolution system.
"""

import asyncio
import logging
from typing import Any

from taskiq import InMemoryBroker

from taskiq_flow.dataflow.cache import DataCache
from taskiq_flow.dataflow.dag import DAGNode
from taskiq_flow.dataflow.registry import DataflowRegistry
from taskiq_flow.decorators import pipeline_task
from taskiq_flow.execution_engine import ExecutionEngine
from taskiq_flow.visualization import DAGVisualizer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# Create broker and define decorated tasks
# ============================================================================

broker = InMemoryBroker(await_inplace=True)


@broker.task
@pipeline_task(output="raw_data")
async def load_data(source: str) -> dict[str, Any]:
    """
    Load raw data from a source.

    This task has no dependencies (input: source parameter only).
    It produces 'raw_data' which will be used by downstream tasks.
    """
    await asyncio.sleep(0.1)
    return {
        "source": source,
        "records": [
            {"id": 1, "value": 10.5},
            {"id": 2, "value": 20.3},
            {"id": 3, "value": 15.7},
        ],
    }


@broker.task
@pipeline_task(output="cleaned_data")
async def clean_data(raw_data: dict[str, Any]) -> dict[str, Any]:
    """
    Clean raw data by removing invalid records.

    This task automatically receives 'raw_data' as input because
    it declares a parameter with that name.
    """
    await asyncio.sleep(0.2)
    records = [r for r in raw_data["records"] if r["value"] > 0]
    return {
        "source": raw_data["source"],
        "cleaned_count": len(records),
        "records": records,
    }


@broker.task
@pipeline_task(output="features")
async def extract_features(cleaned_data: dict[str, Any]) -> dict[str, Any]:
    """Extract features from cleaned data."""
    await asyncio.sleep(0.15)
    total = sum(r["value"] for r in cleaned_data["records"])
    avg = total / len(cleaned_data["records"]) if cleaned_data["records"] else 0
    return {
        "source": cleaned_data["source"],
        "total_value": total,
        "average_value": avg,
        "record_count": len(cleaned_data["records"]),
    }


@broker.task
@pipeline_task(output="report")
async def generate_report(features: dict[str, Any]) -> dict[str, Any]:
    """Generate a final report from features."""
    await asyncio.sleep(0.1)
    return {
        "report_id": "RPT-001",
        "summary": {
            "source": features["source"],
            "total": features["total_value"],
            "average": round(features["average_value"], 2),
            "count": features["record_count"],
        },
        "generated_at": "2026-05-05T00:00:00Z",
    }


# ============================================================================
# Example 1: Manual registry construction and inspection
# ============================================================================


async def example_manual_registry() -> None:
    """
    Example 1: Manual registration and inspection.

    This shows how to manually register tasks with DataflowRegistry
    and inspect the dataflow graph before execution.
    """
    logger.info("\n" + "=" * 70)
    logger.info("EXAMPLE 1: Manual Registry Construction & Inspection")
    logger.info("=" * 70)

    # Create a new registry
    registry = DataflowRegistry()

    # Register tasks manually with their data dependencies
    # This is the core of automatic dataflow discovery!
    registry.register_task(load_data, output="raw_data", inputs=["source"])
    registry.register_task(clean_data, output="cleaned_data", inputs=["raw_data"])
    registry.register_task(extract_features, output="features", inputs=["cleaned_data"])
    registry.register_task(generate_report, output="report", inputs=["features"])

    # Inspect the registry
    logger.info(f"\nRegistry: {registry}")
    logger.info(f"Number of tasks: {len(registry)}")
    logger.info(f"All tasks: {[t.task_name for t in registry.get_tasks()]}")

    # Query data dependencies
    report_deps = registry.get_data_dependencies(generate_report)  # type: ignore
    logger.info(f"\ngenerate_report depends on: {report_deps}")

    # Find producers
    features_producer = registry.get_producer("features")
    if features_producer:
        logger.info(f"'features' is produced by: {features_producer.task_name}")

    # Find consumers
    raw_data_consumers = registry.get_consumers("raw_data")
    logger.info(
        f"'raw_data' is consumed by: {[c.task_name for c in raw_data_consumers]}",
    )

    # List outputs and external inputs
    outputs = registry.get_outputs()
    external_inputs = registry.get_external_inputs()
    logger.info(f"\nPipeline outputs: {outputs}")
    logger.info(f"External inputs (must be provided at runtime): {external_inputs}")

    # Build the DAG
    dag = registry.build_dag()
    logger.info(f"\nDAG built: {len(dag.nodes)} nodes, {len(dag.edges)} edges")
    logger.info(f"Number of execution levels: {len(dag.levels)}")

    # Show execution order (topological sort)
    ordered = dag.topological_sort()
    logger.info("\nExecution order:")
    for i, node in enumerate(ordered):
        logger.info(f"  {i + 1}. {node.task_name} (level {node.level})")

    # Show parallel execution groups
    logger.info("\nParallel execution groups (by level):")
    for level_idx, level_nodes in enumerate(dag.levels):
        tasks_at_level = [n.task_name for n in level_nodes]
        logger.info(f"  Level {level_idx}: {tasks_at_level}")

    # Visualize as DOT
    viz = DAGVisualizer.to_dot(dag)
    logger.info("\nDAG DOT representation (first 200 chars):")
    logger.info(viz[:200] + "...")


# ============================================================================
# Example 2: Dynamic task registration
# ============================================================================


async def example_dynamic_registration() -> None:
    """
    Example 2: Dynamic task registration based on configuration.

    Shows how tasks can be registered dynamically at runtime,
    enabling plugin architectures or conditional pipelines.
    """
    logger.info("\n" + "=" * 70)
    logger.info("EXAMPLE 2: Dynamic Task Registration")
    logger.info("=" * 70)

    registry = DataflowRegistry()

    # Simulate dynamic task selection based on config
    available_processors = {
        "standard": clean_data,
        "advanced": clean_data,  # In real life, would be a different function
    }

    selected_processor = "standard"

    # Build pipeline dynamically
    registry.register_task(load_data, output="raw_data", inputs=["source"])
    registry.register_task(
        available_processors[selected_processor],
        output="cleaned_data",
        inputs=["raw_data"],
    )
    registry.register_task(extract_features, output="features", inputs=["cleaned_data"])
    registry.register_task(generate_report, output="report", inputs=["features"])

    logger.info(f"Dynamic pipeline configured with processor: {selected_processor}")
    logger.info(f"Registered tasks: {[t.task_name for t in registry.get_tasks()]}")

    dag = registry.build_dag()
    logger.info(f"DAG: {len(dag.nodes)} nodes, {len(dag.edges)} edges")


# ============================================================================
# Example 3: Validation and error detection
# ============================================================================


async def example_validation() -> None:
    """
    Example 3: Validation and error detection.

    Shows how DataflowRegistry catches common errors:
    - Missing dependencies
    - Circular dependencies
    - Duplicate outputs
    """
    logger.info("\n" + "=" * 70)
    logger.info("EXAMPLE 3: Validation & Error Detection")
    logger.info("=" * 70)

    # Define a broken task that depends on a non-existent output
    @broker.task
    @pipeline_task(output="result")
    async def broken_task(nonexistent_data: dict[str, Any]) -> dict[str, Any]:
        return {"result": "broken"}

    registry = DataflowRegistry()
    registry.register_task(load_data, output="raw_data", inputs=["source"])
    registry.register_task(broken_task, output="result", inputs=["nonexistent_data"])

    # Attempt to build DAG - this will raise an error
    try:
        dag = registry.build_dag()
        logger.error("ERROR: Should have raised ValueError!")
        logger.info(f"DAG built: {len(dag.nodes)} nodes, {len(dag.edges)} edges")
    except ValueError as e:
        logger.info(f"\n✓ Expected error caught: {e}")
        logger.info("This prevents broken pipelines from being deployed.")


# ============================================================================
# Example 4: Execution using ExecutionEngine directly
# ============================================================================


async def example_execution_with_engine() -> None:
    """
    Example 4: Execute the pipeline using ExecutionEngine directly.

    This shows the full manual flow: registry -> DAG -> engine -> results.
    """
    logger.info("\n" + "=" * 70)
    logger.info("EXAMPLE 4: Full Manual Execution with ExecutionEngine")
    logger.info("=" * 70)

    # Step 1: Build registry
    registry = DataflowRegistry()
    registry.register_task(load_data, output="raw_data", inputs=["source"])
    registry.register_task(clean_data, output="cleaned_data", inputs=["raw_data"])
    registry.register_task(extract_features, output="features", inputs=["cleaned_data"])
    registry.register_task(generate_report, output="report", inputs=["features"])

    logger.info("Registry built with tasks:")
    for task in registry.get_tasks():
        meta = registry.get_task_metadata(task)
        logger.info(
            f"  - {task.task_name}: output={meta['output']}, inputs={meta['inputs']}",
        )

    # Step 2: Build DAG
    dag = registry.build_dag()
    logger.info(f"\nDAG constructed: {len(dag.nodes)} nodes")

    # Step 3: Create execution engine
    engine = ExecutionEngine(
        broker=broker,
        dag=dag,
        fail_fast=True,
        max_parallel=4,
    )
    logger.info("ExecutionEngine created with max_parallel=4")

    # Step 4: Execute with external inputs
    logger.info("\nExecuting pipeline with source='local://data/file.csv'...")
    results = await engine.execute(
        inputs={"source": "local://data/file.csv"},
        pipeline_id="manual_pipeline_example",
    )

    logger.info("\n✓ Pipeline executed successfully!")
    logger.info("\nOutputs produced:")
    for output_name, output_value in results.items():
        logger.info(f"  {output_name}: {output_value}")


# ============================================================================
# Example 5: Using DataCache for manual execution
# ============================================================================


async def example_manual_execution_with_cache() -> None:
    """
    Example 5: Manual step-by-step execution with DataCache.

    Shows how to manually drive execution for fine-grained control,
    useful for debugging or custom orchestration.

    Note: This example demonstrates the internal mechanics. In practice,
    use ExecutionEngine or DataflowPipeline for real workloads.
    """
    logger.info("\n" + "=" * 70)
    logger.info("EXAMPLE 5: Manual Step-by-Step Execution")
    logger.info("=" * 70)

    registry = DataflowRegistry()
    registry.register_task(load_data, output="raw_data", inputs=["source"])
    registry.register_task(clean_data, output="cleaned_data", inputs=["raw_data"])
    registry.register_task(extract_features, output="features", inputs=["cleaned_data"])
    registry.register_task(generate_report, output="report", inputs=["features"])

    dag = registry.build_dag()

    # Manual execution loop
    cache = DataCache()

    # Initialize cache with external inputs
    external_inputs = registry.get_external_inputs()
    logger.info(f"External inputs required: {external_inputs}")

    # Provide values for external inputs (must match task signatures)
    cache.set("source", "local://data/file.csv")
    logger.info("✓ Initialized cache with external input 'source'")

    completed_nodes: set[DAGNode] = set()

    logger.info("\nStarting manual execution...")

    while True:
        ready = dag.get_ready_tasks(completed_nodes)
        if not ready:
            break

        logger.info(f"\nReady tasks at this step: {[n.task_name for n in ready]}")

        for node in ready:
            task = node.task
            task_name = task.task_name

            # Get dependencies from registry
            deps = registry.get_data_dependencies(task)
            logger.info(f"  Executing '{task_name}' with dependencies: {deps}")

            # Inject dependencies from cache
            try:
                args = cache.inject(deps)
            except KeyError as e:
                logger.error(f"  ✗ Missing dependency: {e}")
                raise

            # Execute task
            result = await task.kiq(**args)
            result_value = await result.wait_result()
            output_value = result_value.return_value

            # Store output in cache
            task_meta = registry.get_task_metadata(task)
            output_name = task_meta["output"]
            cache.set(output_name, output_value)

            logger.info(f"  ✓ '{task_name}' produced '{output_name}': {output_value}")

            completed_nodes.add(node)

    logger.info("\n✓ All tasks completed!")
    logger.info(f"\nFinal cache contents: {list(cache.keys)}")
    logger.info(f"\nFinal output (report): {cache.get('report')}")


# ============================================================================
# Example 6: Integration with DataflowPipeline using registry
# ============================================================================


async def example_integration_with_pipeline() -> None:
    """
    Example 6: Integration with DAGBuilder and validation.

    Shows how to use a pre-built registry with DAGBuilder
    to validate and obtain a DAG for advanced use cases.
    """
    logger.info("\n" + "=" * 70)
    logger.info("EXAMPLE 6: Integration with DAGBuilder")
    logger.info("=" * 70)

    from taskiq_flow.dag_builder import DAGBuilder  # noqa: PLC0415

    # Build registry manually
    registry = DataflowRegistry()
    registry.register_task(load_data, output="raw_data", inputs=["source"])
    registry.register_task(clean_data, output="cleaned_data", inputs=["raw_data"])
    registry.register_task(extract_features, output="features", inputs=["cleaned_data"])
    registry.register_task(generate_report, output="report", inputs=["features"])

    logger.info("Registry built manually with 4 tasks")

    # Build DAG from registry
    dag = registry.build_dag()
    logger.info(f"DAG built: {len(dag.nodes)} nodes, {len(dag.edges)} edges")

    # Validate the DAG using DAGBuilder
    DAGBuilder.validate_dag(dag)
    logger.info("DAG validation passed (no cycles, proper connectivity)")

    # Show pipeline statistics
    logger.info("\nPipeline statistics:")
    logger.info(f"  - Tasks: {len(registry)}")
    logger.info(f"  - Data nodes: {len(registry.data_nodes)}")
    logger.info(f"  - External inputs: {registry.get_external_inputs()}")
    logger.info(f"  - Outputs: {registry.get_outputs()}")
    logger.info(f"  - Execution levels: {len(dag.levels)}")

    # For actual execution, you'd typically use DataflowPipeline.from_tasks()
    # but here we demonstrate the registry pattern for advanced use cases
    logger.info("\nNote: For standard usage, use DataflowPipeline.from_tasks()")
    logger.info("The registry pattern is useful for dynamic/plugin pipelines,")
    logger.info("custom DAG manipulation, or inspecting dependencies before execution.")


# ============================================================================
# Main
# ============================================================================


async def main() -> None:
    """Run all registry discovery examples."""
    logger.info("TaskIQ Flow - DataflowRegistry Discovery Examples")
    logger.info("=" * 70)
    logger.info("\nThese examples demonstrate how the automatic dataflow")
    logger.info("discovery system works under the hood.")

    # Example 1: Basic registry usage
    await example_manual_registry()

    # Example 2: Dynamic registration
    await example_dynamic_registration()

    # Example 3: Error detection
    await example_validation()

    # Example 4: Execution with ExecutionEngine
    await example_execution_with_engine()

    # Example 5: Manual step-by-step execution
    await example_manual_execution_with_cache()

    # Example 6: Integration with higher-level APIs
    await example_integration_with_pipeline()

    logger.info("\n" + "=" * 70)
    logger.info("All examples completed!")
    logger.info("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
