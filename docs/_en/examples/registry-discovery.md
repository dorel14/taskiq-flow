---
permalink: /en/examples/registry-discovery/
title: Example: registry_discovery_example.py
nav_order: 43
color_scheme: dark
---
# Example: registry_discovery_example.py

**Manual DataflowRegistry construction, DAG inspection, and low-level execution**

> **Version**: 0.4.5 | **File**: `examples/registry_discovery_example.py`

---

## Overview

This advanced example demonstrates the internals of Taskiq-Flow's automatic dependency resolution system using `DataflowRegistry`. It shows how to:

- Manually register tasks with their I/O declarations
- Inspect the dataflow graph before execution
- Build and validate a DAG
- Execute pipelines using `ExecutionEngine` directly
- Understand data provenance and task dependencies

**This is the core mechanism behind `DataflowPipeline.from_tasks()`.**

---

## What This Example Shows

- Complete `DataflowRegistry` API usage
- Manual DAG construction from task metadata
- Querying task dependencies (producers/consumers)
- Topological sorting and parallel level detection
- Direct `ExecutionEngine` execution
- The `DataCache` for manual step-by-step execution
- Error detection (missing dependencies, cycles)

---

## Code Walkthrough

### Tasks Definition (same as dataflow_audio style)

```python
from taskiq_flow.dataflow.registry import DataflowRegistry
from taskiq_flow.execution_engine import ExecutionEngine
from taskiq_flow.dataflow.cache import DataCache
from taskiq_flow.visualization import DAGVisualizer

@broker.task
@pipeline_task(output="raw_data")
async def load_data(source: str) -> dict:
    return {"source": source, "records": [...]}

@broker.task
@pipeline_task(output="cleaned_data")
async def clean_data(raw_data: dict) -> dict:
    records = [r for r in raw_data["records"] if r["value"] > 0]
    return {"source": raw_data["source"], "records": records}

@broker.task
@pipeline_task(output="features")
async def extract_features(cleaned_data:dict) -> dict:
    total = sum(r["value"] for r in cleaned_data["records"])
    return {"total": total, "count": len(cleaned_data["records"])}

@broker.task
@pipeline_task(output="report")
async def generate_report(features: dict) -> dict:
    return {"report_id": "RPT-001", "summary": features}
```

---

## Example 1: Manual Registry Construction & Inspection

```python
async def example_manual_registry():
    registry = DataflowRegistry()

    # Register tasks manually
    registry.register_task(load_data, output="raw_data", inputs=["source"])
    registry.register_task(clean_data, output="cleaned_data", inputs=["raw_data"])
    registry.register_task(extract_features, output="features", inputs=["cleaned_data"])
    registry.register_task(generate_report, output="report", inputs=["features"])

    # Inspect the registry
    print(f"Tasks: {[t.task_name for t in registry.get_tasks()]}")
    # ['load_data', 'clean_data', 'extract_features', 'generate_report']

    # Query dependencies
    deps = registry.get_data_dependencies(generate_report)
    print(f"generate_report depends on: {deps}")  # ['features']

    # Find who produces 'features'
    producer = registry.get_producer("features")
    print(f"'features' produced by: {producer.task_name}")  # extract_features

    # Find who consumes 'raw_data'
    consumers = registry.get_consumers("raw_data")
    print(f"'raw_data' consumed by: {[c.task_name for c in consumers]}")  # [clean_data]

    # External inputs (not produced by any task)
    external = registry.get_external_inputs()
    print(f"External inputs: {external}")  # ['source']

    # Outputs (final results)
    outputs = registry.get_outputs()
    print(f"Pipeline outputs: {outputs}")  # ['raw_data', 'cleaned_data', 'features', 'report']
```

**Key methods**:

| Method | Returns |
|--------|---------|
| `get_tasks()` | All registered `TaskNode` objects |
| `get_outputs()` | All output keys |
| `get_external_inputs()` | Inputs not produced by any task |
| `get_producer(output_key)` | Task that produces that output |
| `get_consumers(input_key)` | Tasks needing that input |
| `get_data_dependencies(task)` | List of input keys for a task |

---

## Example 2: Building and Visualizing the DAG

```python
    # Build DAG
    dag = registry.build_dag()

    print(f"DAG: {len(dag.nodes)} nodes, {len(dag.edges)} edges")

    # Execution order (topological sort)
    order = dag.topological_sort()
    for i, node in enumerate(order):
        print(f"{i+1}. {node.task_name}")

    # Parallel execution levels
    for level_idx, level_nodes in enumerate(dag.levels):
        tasks = [n.task_name for n in level_nodes]
        print(f"Level {level_idx}: {tasks}")

    # ASCII visualization
    dag.print()

    # DOT format
    dot = DAGVisualizer.to_dot(dag)
    with open("pipeline.dot", "w") as f:
        f.write(dot)
```

**DAG properties**:
- `dag.nodes` — All nodes
- `dag.edges` — Dependency edges
- `dag.roots` — Nodes with no dependencies
- `dag.leaves` — Nodes with no dependents
- `dag.levels` — Groups of tasks that can run in parallel
- `dag.topological_sort()` — Linear execution order

---

## Example 3: Validation & Error Detection

```python
async def example_validation():
    registry = DataflowRegistry()
    registry.register_task(load_data, output="raw_data", inputs=["source"])

    # Broken: depends on nonexistent output
    @broker.task
    @pipeline_task(output="result")
    async def broken_task(nonexistent_data: dict):
        return {"result": "broken"}

    registry.register_task(broken_task, output="result", inputs=["nonexistent_data"])

    try:
        dag = registry.build_dag()  # Raises ValueError
    except ValueError as e:
        print(f"Caught expected error: {e}")
        # "Task 'broken_task' requires input 'nonexistent_data' but no task produces it"
```

**Validations performed**:
- All declared inputs must be produced by some task (or be external)
- No circular dependencies (cycles)
- No duplicate output names

---

## Example 4: Execution with ExecutionEngine

```python
async def example_execution_with_engine():
    registry = DataflowRegistry()
    registry.register_task(load_data, output="raw_data", inputs=["source"])
    registry.register_task(clean_data, output="cleaned_data", inputs=["raw_data"])
    registry.register_task(extract_features, output="features", inputs=["cleaned_data"])
    registry.register_task(generate_report, output="report", inputs=["features"])

    dag = registry.build_dag()

    engine = ExecutionEngine(
        broker=broker,
        dag=dag,
        fail_fast=True,
        max_parallel=4,
    )

    results = await engine.execute(
        inputs={"source": "local://data/file.csv"},
        pipeline_id="manual_pipeline_example",
    )

    # results = {"raw_data": ..., "cleaned_data": ..., "features": ..., "report": ...}
```

The `ExecutionEngine` is the low-level executor that runs a DAG.

---

## Example 5: Manual Step-by-Step Execution with DataCache

Shows the internal execution loop:

```python
async def example_manual_execution_with_cache():
    registry = DataflowRegistry()
    # register tasks...
    dag = registry.build_dag()

    cache = DataCache()

    # Initialize external inputs
    cache.set("source", "local://data/file.csv")

    completed_nodes = set()

    while True:
        ready = dag.get_ready_tasks(completed_nodes)
        if not ready:
            break

        for node in ready:
            task = node.task
            deps = registry.get_data_dependencies(task)

            # Inject dependencies from cache
            args = cache.inject(deps)  # {'raw_data': {...}, ...}

            # Execute task
            result = await task.kiq(**args)
            output_value = (await result.wait_result()).return_value

            # Store output in cache
            output_name = registry.get_task_metadata(task)["output"]
            cache.set(output_name, output_value)

            completed_nodes.add(node)

    # Final outputs in cache
    final_report = cache.get("report")
```

---

## Why This Matters

Understanding `DataflowRegistry` helps you:

1. **Debug complex pipelines** — Inspect DAG before running
2. **Build dynamic pipelines** — Construct pipelines at runtime based on config
3. **Implement custom orchestration** — Use `ExecutionEngine` directly
4. **Understand data provenance** — Trace where each output came from

---

## Learning Path

After this example:

1. **[Dataflow Guide]({{ '/en/guides/pipelines.md#2-dataflow-pipeline' | relative_url }})** — High-level usage
2. **[ExecutionEngine API]({{ '/en/api/execution/' | relative_url }})** — Low-level execution control
3. **[DAGBuilder]({{ '/en/api/execution.md#dagbuilder' | relative_url }})** — Programmatic DAG construction

---

*Advanced topic. Most users will use `DataflowPipeline.from_tasks()` which wraps this registry internally. Explore this only if you need dynamic pipeline construction.*
