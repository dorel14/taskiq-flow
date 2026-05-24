"""Performance benchmarks for TaskIQ-Flow measuring memory and CPU usage."""

import asyncio
import time
import tracemalloc
from collections.abc import Generator
from typing import Any, cast

import psutil
import pytest
from taskiq import InMemoryBroker
from taskiq.decor import AsyncTaskiqDecoratedTask

from taskiq_flow import DataflowPipeline, pipeline_task


def _create_simple_pipeline() -> DataflowPipeline:
    """Create a simple pipeline with a trivial task."""
    broker = InMemoryBroker(await_inplace=False)

    @broker.task
    @pipeline_task(output="result")
    async def task(x: int) -> int:
        return x * x

    # Cast to the expected type for Pylance compatibility
    pipeline = DataflowPipeline.from_tasks(
        broker, [cast(AsyncTaskiqDecoratedTask[Any, Any], task)]
    )
    pipeline.pipeline_id = "perf-benchmark-pipeline"
    return pipeline


@pytest.fixture
def event_loop() -> Generator[Any, Any, Any]:
    """Create an event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


async def _run_pipeline_once() -> None:
    """Run the pipeline once."""
    pipeline = _create_simple_pipeline()
    await pipeline.kiq_dataflow()


def test_benchmark_performance(benchmark: Any) -> None:
    """Benchmark measuring wall time, CPU time, memory allocations, and RSS."""
    process = psutil.Process()

    tracemalloc.start()
    start_snapshot = tracemalloc.take_snapshot()

    cpu_start = process.cpu_times()
    mem_start = process.memory_info().rss
    wall_start = time.perf_counter()

    benchmark(lambda: asyncio.run(_run_pipeline_once()))

    wall_end = time.perf_counter()
    cpu_end = process.cpu_times()
    mem_end = process.memory_info().rss
    end_snapshot = tracemalloc.take_snapshot()
    tracemalloc.stop()

    wall_time = wall_end - wall_start
    cpu_time_used = (cpu_end.user - cpu_start.user) + (
        cpu_end.system - cpu_start.system
    )
    cpu_percent = (cpu_time_used / wall_time) * 100 if wall_time > 0 else 0
    mem_rss_delta = mem_end - mem_start

    mem_alloc = sum(
        stat.size_diff for stat in end_snapshot.compare_to(start_snapshot, "lineno")
    )

    benchmark.extra_info.update(
        {
            "wall_time_seconds": wall_time,
            "cpu_time_seconds": cpu_time_used,
            "cpu_percent": cpu_percent,
            "rss_delta_bytes": mem_rss_delta,
            "python_memory_allocated_bytes": mem_alloc,
            "iterations": 1,
        }
    )
