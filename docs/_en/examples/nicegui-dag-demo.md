---
title: Example: nicegui_dag_demo.py
nav_order: 48
color_scheme: dark
---
# Example: nicegui_dag_demo.py

**Interactive DAG visualization with NiceGUI and MermaidGenerator**

> **Version**: {VERSION} | **File**: `examples/nicegui_dag_demo.py`

---

## Overview

This example demonstrates how to build an interactive web application using **NiceGUI** to visualize a Taskiq-Flow `DataflowPipeline` DAG. It leverages the built-in `MermaidGenerator` to render flowcharts directly in the browser.

Key themes:
- Generating Mermaid.js diagrams from a DAG programmatically
- Serving them via an interactive NiceGUI web app
- Running the pipeline builder script separately from the web server

---

## What This Example Shows

- Using `DataflowPipeline.from_tasks()` to build a simple 3-step pipeline
- Calling the internal `_build_dataflow_dag()` to obtain the DAG without executing
- Using `MermaidGenerator.to_mermaid_with_styling()` to produce Mermaid code
- Serving that diagram inside a NiceGUI `ui.markdown()` page
- A dual-mode script: async DAG builder + synchronous NiceGUI server

---

## Code Walkthrough

### 1. Pipeline Definition

```python
from taskiq import InMemoryBroker
from taskiq_flow import DataflowPipeline, pipeline_task

broker = InMemoryBroker(await_inplace=True)

@broker.task
@pipeline_task(output="raw_data")
async def load_data(source: str) -> dict[str, Any]:
    """Load raw data from a source."""
    await asyncio.sleep(0.1)
    return {"source": source, "values": [1, 2, 3, 4, 5]}

@broker.task
@pipeline_task(output="processed_data")
async def process_data(raw_data: dict[str, Any]) -> dict[str, Any]:
    """Process raw data by calculating statistics."""
    await asyncio.sleep(0.2)
    values = raw_data["values"]
    return {
        "source": raw_data["source"],
        "count": len(values),
        "sum": sum(values),
        "mean": sum(values) / len(values),
    }

@broker.task
@pipeline_task(output="result")
async def generate_report(processed_data: dict[str, Any]) -> dict[str, Any]:
    """Generate a final report from processed data."""
    await asyncio.sleep(0.1)
    return {
        "report_id": "REPORT-001",
        "source": processed_data["source"],
        "statistics": {
            "count": processed_data["count"],
            "sum": processed_data["sum"],
            "mean": processed_data["mean"],
        },
        "status": "completed",
    }
```

Pipeline DAG structure:
```
load_data → process_data → generate_report
```

---

### 2. Building the DAG (Without Execution)

```python
pipeline = DataflowPipeline.from_tasks(
    broker,
    [load_data, process_data, generate_report],
)
pipeline.pipeline_id = "data_processing_pipeline"

# Build the DAG statically — no tasks are executed
pipeline._build_dataflow_dag()
dag = pipeline._dag
```

> **Note**: `_build_dataflow_dag()` is an internal method that inspects the
> `@pipeline_task` annotations and assembles the dependency graph in memory.
> If you need a public API, use `DataflowRegistry` to build a DAG explicitly
> (see the [Registry Discovery Example]({{ '/en/examples/registry-discovery/' | relative_url }})).

---

### 3. Mermaid Diagram Generation

```python
from taskiq_flow.visualization.mermaid import MermaidGenerator

mermaid_gen = MermaidGenerator(dag)
mermaid_code = mermaid_gen.to_mermaid_with_styling()
# → flowchart LR / TD with colored nodes and arrows
```

`MermaidGenerator.to_mermaid_with_styling()` produces Mermaid.js flowchart
code with CSS class definitions for input/process/output/decision node shapes.

---

### 4. NiceGUI Application

```python
@ui.page("/")
def dag_viewer_page() -> None:
    """Main page showing the DAG visualization."""
    ui.label("TaskIQ Flow DAG Visualization").style("font-size: 24px; font-weight: bold;")
    ui.label("Data Processing Pipeline").style("font-size: 18px;")
    ui.separator()

    if mermaid_diagram_global:
        ui.markdown(f"""```mermaid\n{mermaid_diagram_global}\n```""")
    else:
        ui.label("No DAG to display").style("color: red;")

    ui.separator()
    ui.label("DAG Statistics:").style("font-weight: bold;")

ui.run(title="TaskIQ Flow DAG Viewer", port=8080)
```

NiceGUI renders `ui.markdown()` content server-side, and Mermaid.js is loaded
client-side to render the diagram in the browser.

---

## Expected Output

**Console (DAG builder)**:
```
=== Taskiq-Flow NiceGUI DAG Visualization Demo ===

DAG has 3 nodes and 2 edges
Starting NiceGUI server...
Open your browser to http://127.0.0.1:8080 to view the DAG

Mermaid diagram code:
flowchart LR
    classDef input fill:#e1f5fe,stroke:#01579b,...
    classDef process fill:#fff3e0,stroke:#e65100,...
    classDef output fill:#e8f5e9,stroke:#1b5e20,...
    load_data["load_data"]:::process
    process_data["process_data"]:::process
    generate_report["generate_report"]:::output
    load_data --> process_data
    process_data --> generate_report
```

**Browser at `http://127.0.0.1:8080`**:

A full-page NiceGUI app showing the Mermaid diagram with colored nodes,
alongside basic DAG statistics.

---

## MermaidGenerator Reference

| Method | Description |
|--------|-------------|
| `MermaidGenerator(dag)` | Create generator from a `DAG` instance |
| `.to_mermaid(orientation="TB")` | Basic Mermaid flowchart (TB/BT/LR/RL) |
| `.to_mermaid_with_styling(orientation="LR")` | Styled with color-coded classes per node type |
| `.to_mermaid_interactive()` | HTML with click handlers (for web dashboards) |

Orientation values:

| Value | Direction |
|-------|-----------|
| `"TB"` | Top → Bottom (default) |
| `"BT"` | Bottom → Top |
| `"LR"` | Left → Right |
| `"RL"` | Right → Left |

---

## Running the Demo

### Prerequisites

```bash
pip install nicegui "taskiq-flow[all]"
```

### Option 1 — Build DAG and start NiceGUI

```bash
python examples/nicegui_dag_demo.py
```

The script runs `asyncio.run(main())` which builds the DAG and generates the
Mermaid diagram, then prints instructions for launching NiceGUI.

### Option 2 — Launch NiceGUI directly

```bash
python -c "from examples.nicegui_dag_demo import run_nicegui_app; run_nicegui_app()"
```

> Opens `http://127.0.0.1:8080` in your default browser.

---

## Key Points

### NiceGUI + Taskiq-Flow Integration

| Component | Role |
|-----------|------|
| `DataflowPipeline` | Builds the DAG from task declarations |
| `MermaidGenerator` | Converts DAG → Mermaid.js code |
| `ui.markdown()` | Renders Mermaid diagram in NiceGUI |
| `ui.run()` | Starts the ASGI web server on port 8080 |

### Why Separate Builder and Server

The example keeps the async pipeline builder (`main()`) separate from the
NiceGUI app (`run_nicegui_app()`) because:
- NiceGUI's `ui.run()` is a blocking call and cannot coexist with `asyncio.run()`
- In production, generate Mermaid code at startup or on DAG change events
- Consider using `ui.timer()` or pub/sub to refresh the diagram dynamically

### Alternatives to NiceGUI for DAG Visualization

| Tool | Use Case |
|------|----------|
| `MermaidGenerator` | Static diagrams (docs, wikis, README) |
| `DAGVisualizer` | NetworkX-based analysis, JSON/DOT/ Cytoscape exports |
| `DagViewer` (NiceGUI) | Interactive web panels (when fully implemented) |

---

## Learning Path

After this example:

1. **[DAG Visualization Demo]({{ '/en/examples/dag-visualization-demo/' | relative_url }})** — NetworkX critical path, multiple export formats
2. **[Dataflow Guide]({{ '/en/guides/dataflow/' | relative_url }})** — DAG construction and inspection
3. **[Dataflow Audio Pipeline]({{ '/en/examples/dataflow-audio-pipeline/' | relative_url }})** — Full audio pipeline with parallelism
4. **[Pipelines Guide]({{ '/en/guides/pipelines/' | relative_url }})** — All pipeline types and patterns

---

*This example shows how to combine Taskiq-Flow with NiceGUI for interactive pipeline visualization.*
