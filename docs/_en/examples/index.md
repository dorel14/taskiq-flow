---
title: Example Gallery
nav_order: 40
permalink: /en/examples/
---
# Example Gallery

**Working examples demonstrating key Taskiq-Flow features and patterns**

> **Version**: {VERSION} | **Related**: [Quick Start Guide]({{ '/en/quickstart/' | relative_url }})

---

## Overview

This gallery provides in-depth walkthroughs of the example scripts included in the `examples/` directory. Each example demonstrates a specific feature or integration pattern.

---

## Example Index

| Example | Description | Key Concepts |
|---------|-------------|--------------|
| [Basic Pipeline]({{ '/en/examples/quickstart/' | relative_url }}) | Simple sequential pipeline with map, filter, and group operations | SequentialPipeline, basic steps |
| [Tracking Demo]({{ '/en/examples/tracking-demo/' | relative_url }}) | Real-time pipeline monitoring with PipelineTrackingManager | Tracking, status storage, visualization |
| [Scheduled Pipeline]({{ '/en/examples/scheduled-pipeline/' | relative_url }}) | Cron-based recurring pipeline execution | PipelineScheduler, APScheduler, timezones |
| [Dataflow Audio Pipeline]({{ '/en/examples/dataflow-audio-pipeline/' | relative_url }}) | Full DAG with parallelism, map-reduce, and visualization | DataflowPipeline, automatic DAG, parallelism |
| [DAG Visualization Demo]({{ '/en/examples/dag-visualization-demo/' | relative_url }}) | NetworkX DAG analysis: critical path, parallel groups, multi-format export | DAGVisualizer, NetworkX, critical path, exports |
| [NiceGUI DAG Demo]({{ '/en/examples/nicegui-dag-demo/' | relative_url }}) | Interactive web DAG viewer via NiceGUI and MermaidGenerator | MermaidGenerator, NiceGUI, interactive web visualization |
| [Registry Discovery]({{ '/en/examples/registry-discovery/' | relative_url }}) | Manual DataflowRegistry construction, DAG inspection, and low-level execution | DataflowRegistry, ExecutionEngine, DAG introspection |
| [WebSocket Demo]({{ '/en/examples/websocket-demo/' | relative_url }}) | Real-time event streaming via WebSockets | HookManager, WebSocket transport, live tracking |
| [REST API]({{ '/en/examples/api-example/' | relative_url }}) | FastAPI integration for remote pipeline management | PipelineVisualizationAPI, custom endpoints |

---

## Running the Examples

Each example page includes:

- **Overview** — What the example demonstrates
- **Prerequisites** — Required dependencies and setup
- **Code Walkthrough** — Line-by-line explanation
- **Key Concepts** — Core features highlighted
- **Running Instructions** — How to execute the script
- **Expected Output** — Sample output for verification
- **Common Issues** — Troubleshooting tips

**To run an example**:

```bash
# Navigate to the repository root
cd taskiq-flow

# Install dependencies if needed
pip install -e .

# Run an example script
python examples/quickstart.py
```

Some examples require additional services (Redis, etc.). See individual example pages for specifics.

---

## Example Categories

### Getting Started
- [Basic Pipeline]({{ '/en/examples/quickstart/' | relative_url }}) — Start here if you're new

### Monitoring & Operations
- [Tracking Demo]({{ '/en/examples/tracking-demo/' | relative_url }})
- [Scheduled Pipeline]({{ '/en/examples/scheduled-pipeline/' | relative_url }})
- [WebSocket Demo]({{ '/en/examples/websocket-demo/' | relative_url }})

### Advanced Workflows
- [Dataflow Audio Pipeline]({{ '/en/examples/dataflow-audio-pipeline/' | relative_url }})
- [DAG Visualization Demo]({{ '/en/examples/dag-visualization-demo/' | relative_url }})
- [NiceGUI DAG Demo]({{ '/en/examples/nicegui-dag-demo/' | relative_url }})
- [Registry Discovery]({{ '/en/examples/registry-discovery/' | relative_url }})

### Integration
- [REST API]({{ '/en/examples/api-example/' | relative_url }})

---

## Next Steps

- **[Quick Start Guide]({{ '/en/quickstart/' | relative_url }})** — Run your first pipeline
- **[User Guides]({{ '/en/guides/' | relative_url }})** — Deep dives into each feature
- **[API Reference]({{ '/en/api/' | relative_url }})** — Complete module documentation
