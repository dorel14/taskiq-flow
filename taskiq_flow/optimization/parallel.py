"""Resource-aware parallelism for pipeline execution.

This module provides dynamic parallelism adjustment based on CPU and memory
availability. It calculates optimal parallelism per task level based on
task resource profiles.

Author: SoniqueBay Team
Version: 0.3.2
"""

import psutil
from pydantic import BaseModel


class TaskResourceProfile(BaseModel):
    """
    Resource requirements for a task.

    Uses Pydantic for validation and serialization.

    Example:
        @pipeline_task(
            output="audio_metadata",
            resources=TaskResourceProfile(
                estimated_memory_mb=500,
                estimated_cpu_cores=1.0,
            ),
        )
        async def extract_audio_metadata(audio_file):
            # Heavy audio processing
            ...
    """

    estimated_memory_mb: int = 100
    estimated_cpu_cores: float = 0.5
    io_bound: bool = False


class ResourceAwareExecutor:
    """
    Dynamically adjusts parallelism based on CPU+RAM availability.

    For your audio discovery example:
    - extract_audio_metadata: 500MB RAM, 1.0 CPU (CPU-bound)
    - discover_artist_photos: 50MB RAM, 0.1 CPU (I/O-bound)

    System: 16GB RAM, 8 cores

    Result: Automatically runs 8 audio tasks + 8 photo tasks in parallel
    (or more photo tasks since they use fewer resources)
    """

    def __init__(
        self,
        max_cpu_percent: float = 80.0,
        max_memory_percent: float = 80.0,
        min_parallel: int = 1,
        max_parallel: int = 10,
    ) -> None:
        self.max_cpu = max_cpu_percent
        self.max_memory = max_memory_percent
        self.min_parallel = min_parallel
        self.max_parallel = max_parallel

    def get_optimal_parallelism(
        self,
        task_memory_estimate: int = 0,
        task_cpu_estimate: float = 0.5,
    ) -> int:
        """
        Calculate optimal parallelism based on current resources.

        Args:
            task_memory_estimate: Estimated memory per task in MB
            task_cpu_estimate: Estimated CPU cores per task (0.5 = half core)

        Returns:
            Number of tasks to run in parallel
        """
        try:
            cpu_available = 100 - psutil.cpu_percent(interval=0.1)
            memory = psutil.virtual_memory()
            memory_available = memory.available / (1024 * 1024)  # MB
        except Exception:
            return min(self.max_parallel, 4)

        # CPU-based limit
        if task_cpu_estimate > 0:
            cpu_parallel = int(
                (cpu_available / 100) * self.max_parallel / task_cpu_estimate
            )
        else:
            cpu_parallel = self.max_parallel

        # Memory-based limit
        if task_memory_estimate > 0:
            memory_parallel = int(memory_available / task_memory_estimate)
        else:
            memory_parallel = self.max_parallel

        # Take the most restrictive
        optimal = min(cpu_parallel, memory_parallel, self.max_parallel)
        return max(optimal, self.min_parallel)


def get_default_executor() -> ResourceAwareExecutor:
    """Get the default resource-aware executor instance."""
    return ResourceAwareExecutor()
