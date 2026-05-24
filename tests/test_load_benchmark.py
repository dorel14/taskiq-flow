"""
Benchmark de charge pour TaskIQ-Flow.

Utilise pytest-benchmark pour mesurer le débit et la latence
lors de l'exécution de nombreux pipelines concurrents.
"""

import asyncio
import time
from typing import Any

from taskiq import InMemoryBroker

from taskiq_flow import DataflowPipeline, pipeline_task

# Paramètres par défaut
DEFAULT_CONCURRENCY = 10
DEFAULT_NUM_TASKS = 50
DEFAULT_TASK_DURATION = 0.01  # 10 ms par tâche


def _create_pipeline(concurrency: int = DEFAULT_CONCURRENCY) -> DataflowPipeline:
    """Créer un pipeline simple avec une tâche qui attend."""
    broker = InMemoryBroker(await_inplace=False)

    @broker.task
    @pipeline_task(output="result")
    async def dummy_task() -> dict[str, Any]:
        await asyncio.sleep(DEFAULT_TASK_DURATION)
        return {"status": "ok"}

    pipeline = DataflowPipeline.from_tasks(broker, [dummy_task])
    pipeline.pipeline_id = "load-test-benchmark"
    return pipeline


def _run_throughput(num_tasks: int, concurrency: int) -> None:
    """Exécuter num_tasks tâches en parallèle limité par concurrency."""
    pipeline = _create_pipeline(concurrency)
    sem = asyncio.Semaphore(concurrency)

    async def run_single() -> None:
        async with sem:
            await pipeline.kiq_dataflow()

    async def run_all() -> None:
        await asyncio.gather(*[run_single() for _ in range(num_tasks)])

    asyncio.run(run_all())


def _run_latency(num_tasks: int, concurrency: int) -> list[float]:
    """Exécuter num_tasks tâches et retourner les latences individuelles."""
    pipeline = _create_pipeline(concurrency)
    sem = asyncio.Semaphore(concurrency)

    async def run_single() -> float:
        async with sem:
            start = time.perf_counter()
            await pipeline.kiq_dataflow()
            return time.perf_counter() - start

    async def run_all() -> list[float]:
        return list(await asyncio.gather(*[run_single() for _ in range(num_tasks)]))

    return asyncio.run(run_all())


def test_load_benchmark_throughput(benchmark: Any) -> None:
    """
    Mesure le débit (throughput) du pipeline sous charge.

    Exécute DEFAULT_NUM_TASKS tâches concurrentes.
    """
    benchmark(_run_throughput, DEFAULT_NUM_TASKS, DEFAULT_CONCURRENCY)


def test_load_benchmark_latency(benchmark: Any) -> None:
    """Mesure la latence moyenne par tâche."""
    latencies = benchmark(_run_latency, 10, DEFAULT_CONCURRENCY)
    assert len(latencies) == 10
    avg_latency = sum(latencies) / len(latencies)
    # Seuil généreux : la latence moyenne doit rester raisonnable (< 2s)
    assert 0 < avg_latency < 2.0, f"Latence moyenne anormale : {avg_latency:.3f}s"
