---
title: Installation Guide
nav_order: 11
---
# Installation Guide

**How to install Taskiq-Flow and set up your environment**

---

## Prerequisites

- **Python** 3.9 or higher
- **pip** (Python package installer)
- Optional: **Redis** (for distributed tracking/storage)

---

## Basic Installation

Install Taskiq-Flow from PyPI:

```bash
pip install taskiq taskiq-flow
```

That's it! You're ready to start building pipelines.

---

## Optional Dependencies

Taskiq-Flow supports additional features via optional extras:

```bash
# All features (tracking, scheduling, visualization)
pip install "taskiq-flow[all]"

# Redis support for tracking/storage backends
pip install "taskiq-flow[redis]"

# Scheduling capabilities (APScheduler)
pip install "taskiq-flow[scheduler]"

# Development dependencies (including all above)
pip install "taskiq-flow[dev]"
```

---

## Verify Installation

Create a simple test file `test_install.py`:

```python
from taskiq import InMemoryBroker
from taskiq_flow import Pipeline, PipelineMiddleware

broker = InMemoryBroker()
broker.add_middlewares(PipelineMiddleware())

@broker.task
def hello(name: str) -> str:
    return f"Hello, {name}!"

async def main():
    pipeline = Pipeline(broker).call_next(hello)
    task = await pipeline.kiq("World")
    result = await task.wait_result()
    print(result.return_value)  # Should print: Hello, World!

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
```

Run:

```bash
python test_install.py
```

Expected output:
```
Hello, World!
```

---

## Next Steps

Once installed, continue with:

- **[Quick Start Guide]({{ '/en/quickstart/' | relative_url }})** — Build your first pipeline
- **[Core Concepts]({{ '/en/guides/core-concepts/' | relative_url }})** — Understand fundamental ideas

---

*Installation completed successfully? Proceed to [Quick Start]({{ '/en/quickstart/' | relative_url }}).*
