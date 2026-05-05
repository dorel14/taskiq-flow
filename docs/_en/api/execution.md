---
title: API Reference: Execution Engine
nav_order: 32
---
# API Reference: Execution Engine

**ExecutionEngine, DAG, DAGNode, DAGBuilder, and MapReduce**

> **Version**: 0.3.2 | **Module**: `taskiq_flow.execution_engine`, `taskiq_flow.dataflow.dag`, `taskiq_flow.map_reduce`

---

## ExecutionEngine

Low-level engine for executing DAGs directly, bypassing Pipeline abstraction.

```python
from taskiq_flow import ExecutionEngine, DataflowRegistry

# Build registry manually
registry = DataflowRegistry()
registry.register_task(load, output="raw", inputs=[])
registry.register_task(process, output="clean", inputs=["raw"])
registry.register_task(save, output="saved", inputs=["clean"])

# Build DAG
dag = registry.build_dag()

# Create engine
engine = ExecutionEngine(broker, dag)

# Execute
results = await engine.execute(inputs={"source": "data.csv"})
# results = {"raw": ..., "clean": ..., "saved": ...}
```

**Constructor**:
```python
ExecutionEngine(
    broker: BaseBroker,
    dag: DAG,
    max_parallel: int = None,
    on_step_complete: callable = None
)
```

**Methods**:

| Method | Signature | Description |
|--------|-----------|-------------|
| `execute` | `execute(inputs: dict) -> dict` | Run the DAG with given inputs |
| `execute_async` | `execute_async(inputs: dict) -> AsyncIterator` | Stream results as they complete |
| `cancel` | `cancel()` | Stop running execution |

**Events**:

```python
async def on_step(task_name: str, result: Any):
    print(f"Step {task_name} completed")

engine = ExecutionEngine(broker, dag, on_step_complete=on_step)
```

---

## DAG (Directed Acyclic Graph)

Represents the execution graph of tasks.

```python
from taskiq_flow.dataflow import DAG, DAGNode

dag = DAG()
node = DAGNode(task=my_task, output="result", inputs=["input_a"])
dag.add_node(node)
```

**DAG Methods**:

| Method | Description |
|--------|-------------|
| `add_node(node: DAGNode)` | Add a task node |
| `add_edge(from_task, to_task)` | Add dependency |
| `topological_sort() -> list[DAGNode]` | Return execution order |
| `get_parallel_levels() -> list[list[DAGNode]]` | Group nodes by parallel execution level |
| `validate()` | Check for cycles, missing nodes |
| `print()` | ASCII visualization to console |

**DAG Properties**:

| Property | Type | Description |
|----------|------|-------------|
| `nodes` | `list[DAGNode]` | All nodes in graph |
| `edges` | `set[tuple[DAGNode, DAGNode]]` | Dependency edges |
| `roots` | `list[DAGNode]` | Nodes with no dependencies |
| `leaves` | `list[DAGNode]` | Nodes with no dependents |

---

## DAGNode

Represents a single task in the DAG with its I/O specification.

```python
from taskiq_flow.dataflow import DAGNode

node = DAGNode(
    task=my_task_function,
    output="result_key",
    inputs=["input_a", "input_b"],
    metadata={"description": "My task"}
)
```

**Properties**:

| Property | Type | Description |
|----------|------|-------------|
| `task` | `Callable` | The task function |
| `task_name` | `str` | Auto-generated or custom name |
| `output` | `str` | Output key (single) |
| `outputs` | `list[str]` | Output keys (multiple) |
| `inputs` | `list[str]` | Required input keys |
| `metadata` | `dict` | Arbitrary metadata |

---

## DAGBuilder

Helper to construct DAGs programmatically (less common; usually use DataflowRegistry).

```python
from taskiq_flow import DAGBuilder

builder = DAGBuilder()
builder.add_task(task1, output="a", inputs=[])
builder.add_task(task2, output="b", inputs=["a"])
builder.add_task(task3, output="c", inputs=["a", "b"])

dag = builder.build()
```

**Builder Pattern**:

```python
dag = (DAGBuilder()
    .node(load, output="raw", inputs=[])
    .node(process, output="clean", inputs=["raw"])
    .node(save, output="saved", inputs=["clean"])
    .build()
)
```

---

## MapReduce

Utility for parallel map followed by reduce.

### MapReduce.map

```python
from taskiq_flow import MapReduce

mapped = await MapReduce.map(
    broker,
    map_func,          # Task function to apply
    items: Iterable,   # Items to process
    output: str = "mapped",
    max_parallel: int = None
)
# Returns: MapReduceResult (behaves like Task)
```

### MapReduce.reduce

```python
reduced = await MapReduce.reduce(
    broker,
    reduce_func,       # Aggregation function
    mapped_result,     # Output from MapReduce.map
    input_name: str,   # Name of mapped output to consume
    output: str = "reduced"
)
# Returns: Task (with final result)
```

### MapReduce.map_reduce (combined)

```python
final = await MapReduce.map_reduce(
    broker,
    map_func,
    items,
    reduce_func,
    map_output="mapped",
    reduce_output="final",
    max_parallel=10
)
```

All three return Task objects; call `.wait_result()` to retrieve value.

---

## DataflowRegistry (Advanced)

Manual task registration for dynamic pipeline construction.

```python
from taskiq_flow import DataflowRegistry

registry = DataflowRegistry()

# Register tasks with explicit I/O
registry.register_task(
    task=extract,
    output="features",
    inputs=["audio_files"]  # external input
)
registry.register_task(
    task=tag,
    output="tags",
    inputs=["features"]  # depends on extract's output
)

# Inspect
print("Tasks:", [t.task_name for t in registry.get_tasks()])
print("Outputs:", registry.get_outputs())
print("External inputs:", registry.get_external_inputs())

# Build DAG
dag = registry.build_dag()
dag.print()

# Execute via ExecutionEngine
engine = ExecutionEngine(broker, dag)
results = await engine.execute(inputs={"audio_files": files})
```

**Registry Queries**:

| Method | Description |
|--------|-------------|
| `get_tasks()` | List all registered TaskNode objects |
| `get_outputs()` | List all output keys |
| `get_external_inputs()` | List inputs not produced by any task |
| `get_producer(output_key)` | Get task producing given output |
| `get_consumers(input_key)` | List tasks consuming given input |
| `build_dag()` | Construct DAG, validate, return ready-to-execute |

---

## Version Notes

- **ExecutionEngine** introduced in v0.3.0
- `DAG` and `DAGNode` are used internally by DataflowPipeline
- MapReduce utility available since v0.2.0

---

## Next Steps

- **[Tracking API]({{ '/en/api/tracking/' | relative_url }})** — Monitor execution with PipelineTrackingManager
- **[WebSocket API]({{ '/en/api/websocket/' | relative_url }})** — HookManager and event system
- **[Core API]({{ '/en/api/core/' | relative_url }})** — Pipeline and middleware reference

---

*For advanced use cases only. 95% of users should stick with Pipeline and DataflowPipeline abstractions.*
