---
title: Example Gallery
nav_order: 40
---
# Example Gallery

**Working examples demonstrating key Taskiq-Flow features and patterns**

> **Version**: 0.3.2 | **Related**: [Quick Start Guide]({{ '/en/quickstart/' | relative_url }})

---

## Overview

This gallery provides in-depth walkthroughs of the example scripts included in the `examples/` directory. Each example demonstrates a specific feature or integration pattern.

---

## Example Index

| Example | Description | Key Concepts |
|---------|-------------|--------------|
| [Basic Pipeline](quickstart.md) | Simple sequential pipeline with map, filter, and group operations | SequentialPipeline, basic steps |
| [Tracking Demo](tracking-demo.md) | Real-time pipeline monitoring with PipelineTrackingManager | Tracking, status storage, visualization |
| [Scheduled Pipeline](scheduled-pipeline.md) | Cron-based recurring pipeline execution | PipelineScheduler, APScheduler, timezones |
| [Dataflow Audio Pipeline](dataflow-audio-pipeline.md) | Full DAG with parallelism, map-reduce, and visualization | DataflowPipeline, automatic DAG, parallelism |
| [Registry Discovery](registry-discovery.md) | Manual DataflowRegistry construction, DAG inspection, and low-level execution | DataflowRegistry, ExecutionEngine, DAG introspection |
| [WebSocket Demo](websocket-demo.md) | Real-time event streaming via WebSockets | HookManager, WebSocket transport, live tracking |
| [REST API](api-example.md) | FastAPI integration for remote pipeline management | PipelineVisualizationAPI, custom endpoints |

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
- [Basic Pipeline](quickstart.md) — Start here if you're new

### Monitoring & Operations
- [Tracking Demo](tracking-demo.md)
- [Scheduled Pipeline](scheduled-pipeline.md)
- [WebSocket Demo](websocket-demo.md)

### Advanced Workflows
- [Dataflow Audio Pipeline](dataflow-audio-pipeline.md)
- [Registry Discovery](registry-discovery.md)

### Integration
- [REST API](api-example.md)

---

## Next Steps

- **[Quick Start Guide]({{ '/en/quickstart/' | relative_url }})** — Run your first pipeline
- **[User Guides]({{ '/en/guides/' | relative_url }})** — Deep dives into each feature
- **[API Reference]({{ '/en/api/' | relative_url }})** — Complete module documentation
