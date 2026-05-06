"""Retry Demo

This example demonstrates the retry middleware and error handling modes
in Taskiq-Flow v0.4.5.

Author: SoniqueBay Team
Version: 0.4.5
"""

import asyncio
import random
from taskiq import InMemoryBroker
from taskiq_flow import Pipeline, PipelineMiddleware
from taskiq_flow.middlewares.retry import PipelineRetryMiddleware
from taskiq_flow.errors import ErrorHandlingMode, PipelineErrorAggregator
from taskiq_flow.execution_engine import ExecutionEngine
from taskiq_flow.dataflow.dag import DAG
from taskiq_flow.dataflow.registry import DataflowRegistry


# Create broker
broker = InMemoryBroker(await_inplace=True)


# Define a task that fails intermittently
@broker.task
async def flaky_task(attempt: int = 0) -> str:
    """A task that fails randomly, then succeeds."""
    attempt += 1
    if random.random() < 0.7 and attempt < 3:  # 70% chance of failure, max 3 attempts
        raise RuntimeError(f"Task failed on attempt {attempt}")
    return f"Success on attempt {attempt}"


# Define a task that always fails
@broker.task
async def always_fails() -> str:
    """A task that always fails."""
    raise ValueError("This task always fails")


# Define a task that depends on flaky_task
@broker.task
async def process_result(result: str) -> dict:
    """Process the result from flaky task."""
    return {"processed": result.upper()}


async def demo_retry_middleware():
    """Demonstrate the PipelineRetryMiddleware."""
    print("=== Demo 1: Retry Middleware ===\n")

    # Add retry middleware to broker
    retry_mw = PipelineRetryMiddleware(
        max_retries=3,
        delay=0.5,
        backoff=2.0,
        jitter=True,
    )
    broker.add_middlewares(retry_mw)

    # Create simple sequential pipeline
    pipeline = Pipeline(broker).call_next(flaky_task)

    print("Executing flaky task with retry middleware...")
    print("(Task may fail 1-2 times before succeeding)\n")

    try:
        task = await pipeline.kiq(0)
        result = await task.wait_result(timeout=10)
        print(f"✅ Pipeline succeeded! Result: {result.return_value}")
    except Exception as e:
        print(f"❌ Pipeline failed after retries: {e}")

    print(f"\nRetry count stored in middleware: {retry_mw.retry_counts}")


async def demo_error_handling_modes():
    """Demonstrate different error handling modes."""
    print("\n\n=== Demo 2: Error Handling Modes ===\n")

    # Build a small DAG with two tasks, one failing
    registry = DataflowRegistry()
    registry.register_task(flaky_task, output="flaky_output", inputs=[])
    registry.register_task(process_result, output="final", inputs=["flaky_output"])

    dag = registry.build_dag()

    modes = [
        ErrorHandlingMode.FAIL_FAST,
        ErrorHandlingMode.CONTINUE_ON_ERROR,
        ErrorHandlingMode.SKIP_FAILED,
    ]

    for mode in modes:
        print(f"\n--- Mode: {mode.value} ---")

        engine = ExecutionEngine(
            broker,
            dag,
            error_mode=mode,
        )

        aggregator = PipelineErrorAggregator()

        try:
            # Reset retry counts
            if hasattr(broker, 'middlewares'):
                for mw in broker.middlewares:
                    if hasattr(mw, 'retry_counts'):
                        mw.retry_counts.clear()

            results = await engine.execute(inputs={})
            print(f"  Execution completed. Results: {list(results.keys())}")
        except Exception as e:
            print(f"  Execution raised: {type(e).__name__}: {e}")

    print("\n\nNote: ErrorHandlingMode.DEAD_LETTER would queue failures for later retry.")


async def demo_error_aggregation():
    """Demonstrate error aggregation and analysis."""
    print("\n\n=== Demo 3: Error Aggregation ===\n")

    aggregator = PipelineErrorAggregator()

    # Simulate multiple task failures
    tasks_failed = [
        ("task_a", RuntimeError("timeout")),
        ("task_b", ValueError("invalid data")),
        ("task_c", ConnectionError("network down")),
    ]

    for task_name, error in tasks_failed:
        aggregator.add_error(
            task=type('Task', (), {'task_name': task_name})(),
            error=error,
            context={"attempt": 1},
        )

    print(f"Total errors collected: {len(aggregator.errors)}")
    print(f"Failed tasks: {aggregator.failed_tasks}")
    print("\nError details:")
    for err in aggregator.errors:
        print(f"  - {err.task_name}: {type(err.error).__name__}: {err.error}")

    print("\nYou can use PipelineErrorAggregator to analyze failures and affected branches.")


async def main():
    """Run all demos."""
    # Set random seed for reproducibility
    random.seed(42)

    await demo_retry_middleware()
    await demo_error_handling_modes()
    await demo_error_aggregation()

    print("\n\n=== All Retry & Error Handling Demos Complete ===")


if __name__ == "__main__":
    asyncio.run(main())
