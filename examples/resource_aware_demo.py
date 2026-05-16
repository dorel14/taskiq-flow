"""
Démonstration du parallélisme adaptatif basé sur les ressources pour Taskiq-Flow.

Ce module démontre l'ajustement dynamique du degré de parallélisme selon
la disponibilité CPU et mémoire via `ResourceAwareExecutor` :
- Suivi en temps réel de l'utilisation CPU et mémoire
- Ajustement automatique de la concurrence
- Mode dry-run pour observer les décisions sans exécuter

Exemples :
    - Pipeline resource-aware via ExecutionEngine
    - Monitoring dynamique CPU/mémoire par tâche
    - Comparaison parallélisme fixe vs adaptatif

Auteur: SoniqueBay Team
Version: 1.2.0
"""

import asyncio
import logging
from typing import Any

import psutil
from taskiq import InMemoryBroker

from taskiq_flow import pipeline_task
from taskiq_flow.optimization import (
    ResourceAwareExecutor,
    TaskResourceProfile,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    ).model_dump(),
)
async def light_task(item: int) -> dict[str, Any]:
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
    ).model_dump(),
)
async def heavy_task(item: int) -> dict[str, Any]:
    """CPU-intensive task."""
    # Simulate CPU work
    total = 0
    for _ in range(100000):
        total += item * 2
    return {"item": item, "result": total}


async def demo_resource_aware_executor() -> None:
    """Demonstrate resource-aware parallelism calculation."""
    logger.info("=== Resource-Aware Parallelism Demo ===\n")

    # Create executor
    executor = ResourceAwareExecutor(
        max_cpu_percent=80.0,
        max_memory_percent=80.0,
        min_parallel=1,
        max_parallel=20,
    )

    logger.info("Current system state:")
    logger.info("  CPU Usage: ? (will query at runtime)")
    logger.info("  Memory: ? (will query at runtime)")

    # Simulate checking optimal parallelism for different task types
    logger.info("\n--- Light tasks (I/O bound) ---")
    light_parallel = executor.get_optimal_parallelism(
        task_memory_estimate=50,  # 50 MB per task
        task_cpu_estimate=0.2,  # 0.2 cores per task
    )
    logger.info(f"  Optimal parallelism for light tasks: {light_parallel}")

    logger.info("\n--- Heavy tasks (CPU bound) ---")
    heavy_parallel = executor.get_optimal_parallelism(
        task_memory_estimate=200,  # 200 MB per task
        task_cpu_estimate=1.0,  # 1.0 cores per task
    )
    logger.info(f"  Optimal parallelism for heavy tasks: {heavy_parallel}")

    logger.info("\nNote: Actual values depend on current system load.")


async def demo_pipeline_with_resources() -> None:
    """Demonstrate using resource profiles in a pipeline."""
    logger.info("\n\n=== Pipeline with Resource-Aware Execution ===\n")

    # ResourceAwareExecutor computes optimal parallelism based on task profiles
    executor = ResourceAwareExecutor(
        max_cpu_percent=80.0,
        max_memory_percent=80.0,
        min_parallel=1,
        max_parallel=20,
    )

    # Get optimal parallelism for different task types
    light_parallel = executor.get_optimal_parallelism(
        task_memory_estimate=50,
        task_cpu_estimate=0.2,
    )
    heavy_parallel = executor.get_optimal_parallelism(
        task_memory_estimate=200,
        task_cpu_estimate=1.0,
    )

    logger.info("Pipeline structure:")
    logger.info("  [items:20] --light_task--> [light_results]")
    logger.info("  [items:10] --heavy_task--> [heavy_results]")
    logger.info("  [light_results, heavy_results] --combine--> [final]")

    logger.info("\nRecommended parallelism:")
    logger.info(f"  Light tasks (I/O bound): max_parallel={light_parallel}")
    logger.info(f"  Heavy tasks (CPU bound): max_parallel={heavy_parallel}")

    logger.info(
        "\nTaskResourceProfile allows you to annotate\
            tasks with resource requirements."
    )
    logger.info(
        "ResourceAwareExecutor uses these profiles\
            to compute optimal parallelism."
    )


async def demo_manual_parallelism_tuning() -> None:
    """Demonstrate manual parallelism tuning based on task type."""
    logger.info("\n\n=== Manual Parallelism Tuning ===\n")

    # Estimate system capacity
    cpu_count = psutil.cpu_count() or 4
    memory_gb = psutil.virtual_memory().total / (1024**3)

    logger.info(f"System: {cpu_count} CPU cores, {memory_gb:.1f} GB RAM")

    # Rule of thumb for I/O bound: 2-5x cores
    io_parallel = min(50, cpu_count * 5)
    logger.info(f"\nRecommended max_parallel for I/O-bound tasks: {io_parallel}")

    # Rule of thumb for CPU bound: cores ± 2
    cpu_parallel = min(cpu_count + 2, 20)
    logger.info(f"Recommended max_parallel for CPU-bound tasks: {cpu_parallel}")

    # Mixed workload: start conservative
    logger.info("\nStart with conservative values and benchmark:")
    logger.info("  pipeline.map(light_task, items, max_parallel=10)")
    logger.info("  pipeline.map(heavy_task, items, max_parallel=cpu_count)")


async def main() -> None:
    """Run all demos."""
    await demo_resource_aware_executor()
    await demo_pipeline_with_resources()
    await demo_manual_parallelism_tuning()

    logger.info("\n\n=== Resource-Aware Demo Complete ===")
    logger.info("\nKey takeaways:")
    logger.info("1. Use TaskResourceProfile to annotate task resource needs")
    logger.info("2. ResourceAwareExecutor computes optimal parallelism at runtime")
    logger.info("3. Adjust max_parallel based on task type (I/O vs CPU)")
    logger.info("4. Monitor system resources and tune accordingly")


if __name__ == "__main__":
    asyncio.run(main())
