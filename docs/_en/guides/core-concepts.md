---
title: Core Concepts Guide
nav_order: 12
permalink: /en/guides/core-concepts/
---
# Core Concepts Guide

**Understanding fundamental Taskiq-Flow concepts and architecture**

---

## Overview

Taskiq-Flow is built on two core models:

1. **SequentialPipeline** — Linear step-by-step execution
2. **DataflowPipeline** — Automatic DAG construction from dependencies

Understanding these models helps you choose the right approach for your workflow.

---

## 1. The Sequential Pipeline Model

In a sequential pipeline, you explicitly define the order of operations:

```python
pipeline = (
    Pipeline(broker)
    .call_next(step1)
    .call_next(step2)
    .map(step3)        # Parallel on list
    .filter(step4)     # Conditional
)
```

**Key properties:**
- Execution order is explicit
- Each step receives the previous step's output
- `.map()` and `.filter()` process iterables in parallel
- Best for linear workflows with occasional branching

---

## 2. The Dataflow Pipeline Model

Dataflow pipelines let you declare task dependencies. The library figures out the execution order:

```python
@broker.task
@pipeline_task(output="features")
def extract(data): ...

@broker.task
@pipeline_task(output="stats")
def compute(features): ...  # automatically depends on 'extract'

pipeline = DataflowPipeline.from_tasks(broker, [extract, compute])
```

**Key properties:**
- Tasks declare what they produce (`output=`)
- Downstream tasks automatically receive needed inputs via parameter matching
- Independent tasks run in parallel automatically
- Best for complex, branched workflows

---

## 3. Tasks are Everything

Every function in a pipeline **must** be a taskiq task (decorated with `@broker.task`):

```python
@broker.task
def process(value: int) -> int:
    return value * 2
```

Tasks run asynchronously, can be retried, and are orchestrated by the broker.

---

## 4. Middleware Powers Orchestration

The `PipelineMiddleware` is required. It intercepts task completion and triggers the next step:

```python
from taskiq_flow import PipelineMiddleware

broker.add_middlewares(PipelineMiddleware())
```

Without it, pipelines won't work.

---

## 5. Result Backend is Essential

For multi-worker or distributed setups, use a persistent broker (Redis, Kafka, etc.). The `InMemoryBroker` works only for single-process development.

---

## 6. Tracking & Monitoring (Optional but Recommended)

Add real-time tracking with `PipelineTrackingManager`:

```python
from taskiq_flow import PipelineTrackingManager

tracking = PipelineTrackingManager().with_auto_storage(broker)
pipeline = Pipeline(broker).with_tracking(tracking)
```

This gives you pipeline status, step history, and metrics.

---

## 7. Comparison Table

| Feature | SequentialPipeline | DataflowPipeline |
|---------|-------------------|------------------|
| Order control | Explicit | Automatic |
| Parallelism | Manual (`.group()`) | Automatic (independent tasks) |
| Dependencies | Implicit (chaining) | Explicit (`@pipeline_task`) |
| Best for | Linear ETL | Complex branched workflows |
| Flexibility | Full control | Declarative |

---

## 8. When to Use Which?

**Use SequentialPipeline when:**
- Your workflow is a straight line
- You want fine-grained control over order
- You have occasional map/filter operations

**Use DataflowPipeline when:**
- Tasks have clear data dependencies
- You want automatic parallel execution
- You're building reusable task graphs
- Your workflow branches (fan-out/fan-in)

---

## Next Steps

Now that you understand the concepts:

- **[Installation]({{ '/en/guides/installation/' | relative_url }})** — If you haven't installed yet
- **[Quick Start]({{ '/en/quickstart/' | relative_url }})** — Hands-on tutorial
- **[Pipelines Guide]({{ '/en/guides/pipelines/' | relative_url }})** — Deep dive on pipeline types

---

*Concepts clear? Move on to [Installation]({{ '/en/guides/installation/' | relative_url }}) or [Quick Start]({{ '/en/quickstart/' | relative_url }}).*
