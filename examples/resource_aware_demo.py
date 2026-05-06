"""Resource-Aware Parallelism Demo

This example demonstrates dynamic parallelism adjustment based on
CPU and memory availability using ResourceAwareExecutor.

Author: SoniqueBay Team
Version: 0.4.5
"""

import asyncio
import time
from taskiq import InMemoryBroker
from taskiq_flow import DataflowPipeline, pipeline_task
from taskiq_flow.optimization import (
    ResourceAwareExecutor,
    TaskResourceProfile,
)
from taskiq_flow.steps import mapper


# Create broker
broker = InMemoryBroker(await_inplace=True)


# Define tasks with different resource profiles
@broker.task
@pipeline_task(
    output="light_result",
    resources=TaskResourceProfile(
        estimated_memory_mb=50,
        estimated_cpu_cores=0.2,
        io_bound=True,
    ),
)
async def light_task(item: int) -> dict:
    """Lightweight I/O-bound task."""
    await asyncio.sleep(0.1)  # Simulate I/O
    return {"item": item, "result": item * 2}


@broker.task
@pipeline_task(
    output="heavy_result",
    resources=TaskResourceProfile(
        estimated_memory_mb=200,
        estimated_cpu_cores=1.0,
        io_bound=False,
    ),
)
async def heavy_task(item: int) -> dict:
    """CPU-intensive task."""
    # Simulate CPU work
    total = 0
    for _ in range(100000):
        total += item * 2
    return {"item": item, "result": total}


@broker.task
@pipeline_task(output="combined")
def combine_results(light: list, heavy: list) -> dict:
    """Combine results from both types of tasks."""
    return {
        "light_count": len(light),
        "heavy_count": len(heavy),
        "total_items": len(light) + len(heavy),
    }


async def demo_resource_aware_executor():
    """Demonstrate resource-aware parallelism calculation."""
    print("=== Resource-Aware Parallelism Demo ===\n")

    # Create executor
    executor = ResourceAwareExecutor(
        max_cpu_percent=80.0,
        max_memory_percent=80.0,
        min_parallel=1,
        max_parallel=20,
    )

    print("Current system state:")
    print(f"  CPU Usage: ? (will query at runtime)")
    print(f"  Memory: ? (will query at runtime)")

    # Simulate checking optimal parallelism for different task types
    items = list(range(10))

    print("\n--- Light tasks (I/O bound) ---")
    light_parallel = executor.get_optimal_parallelism(
        task_memory_estimate=50,  # 50 MB per task
        task_cpu_estimate=0.2,    # 0.2 cores per task
    )
    print(f"  Optimal parallelism for light tasks: {light_parallel}")

    print("\n--- Heavy tasks (CPU bound) ---")
    heavy_parallel = executor.get_optimal_parallelism(
        task_memory_estimate=200,  # 200 MB per task
        task_cpu_estimate=1.0,      # 1.0 cores per task
    )
    print(f"  Optimal parallelism for heavy tasks: {heavy_parallel}")

    print("\nNote: Actual values depend on current system load.")


async def demo_pipeline_with_resources():
    """Demonstrate using resource profiles in a pipeline."""
    print("\n\n=== Pipeline with Resource-Aware Execution ===\n")

    # Build a pipeline that processes items in parallel with different resource needs
    pipeline = DataflowPipeline(broker, max_parallel=10)

    # Map light tasks over a list
    pipeline.map(light_task, items=list(range(20)), output="light_results")

    # Map heavy tasks over a list
    pipeline.map(heavy_task, items=list(range(10)), output="heavy_results")

    # Combine
    pipeline.add_step(combine_results, inputs=["light_results", "heavy_results"], output="final")

    pipeline.pipeline_id = "resource_demo"

    print("Pipeline structure:")
    print("  [items:20] --light_task--> [light_results]")
    print("  [items:10] --heavy_task--> [heavy_results]")
    print("  [light_results, heavy_results] --combine--> [final]")

    print("\nExecuting pipeline...")
    try:
        result = await pipeline.kiq_dataflow()
        print(f"✅ Pipeline completed: {result}")
    except Exception as e:
        print(f"❌ Pipeline error: {e}")

    print("\nTaskResourceProfile allows you to annotate tasks with resource requirements.")
    print("ResourceAwareExecutor uses these profiles to compute optimal parallelism.")


async def demo_manual_parallelism_tuning():
    """Demonstrate manual parallelism tuning based on task type."""
    print("\n\n=== Manual Parallelism Tuning ===\n")

    # Estimate system capacity
    import psutil

    cpu_count = psutil.cpu_count() or 4
    memory_gb = psutil.virtual_memory().total / (1024 ** 3)

    print(f"System: {cpu_count} CPU cores, {memory_gb:.1f} GB RAM")

    # Rule of thumb for I/O bound: 2-5x cores
    io_parallel = min(50, cpu_count * 5)
    print(f"\nRecommended max_parallel for I/O-bound tasks: {io_parallel}")

    # Rule of thumb for CPU bound: cores ± 2
    cpu_parallel = min(cpu_count + 2, 20)
    print(f"Recommended max_parallel for CPU-bound tasks: {cpu_parallel}")

    # Mixed workload: start conservative
    print("\nStart with conservative values and benchmark:")
    print("  pipeline.map(light_task, items, max_parallel=10)")
    print("  pipeline.map(heavy_task, items, max_parallel=cpu_count)")


async def main():
    """Run all demos."""
    await demo_resource_aware_executor()
    await demo_pipeline_with_resources()
    await demo_manual_parallelism_tuning()

    print("\n\n=== Resource-Aware Demo Complete ===")
    print("\nKey takeaways:")
    print("1. Use TaskResourceProfile to annotate task resource needs")
    print("2. ResourceAwareExecutor computes optimal parallelism at runtime")
    print("3. Adjust max_parallel based on task type (I/O vs CPU)")
    print("4. Monitor system resources and tune accordingly")


if __name__ == "__main__":
    asyncio.run(main())
