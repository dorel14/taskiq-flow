"""
Advanced profiling utilities for TaskIQ-Flow.

Provides CPU and memory profiling with optional flamegraph generation.
"""

import cProfile
import io
import logging
import pstats
import shutil
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import (
    TYPE_CHECKING,
    Any,
)

if TYPE_CHECKING:
    from typing import Self

# Optional import for flamegraph support
try:
    from flameprof import FlameFS, FlameGraph, PstatsReader

    HAS_FLAMEPROF = True
except ImportError:
    HAS_FLAMEPROF = False

logger = logging.getLogger(__name__)

__all__ = ["Profiler", "generate_flamegraph", "profile"]


class Profiler:
    """
    CPU profiler using cProfile with optional flamegraph output.

    Attributes:
        name: Name of the profiler instance (used in output filenames)

    """

    def __init__(self, name: str = "profile") -> None:
        self.name = name
        self._profiler = cProfile.Profile()
        self._start_time: float | None = None
        self._end_time: float | None = None

    def start(self) -> None:
        """Start profiling."""
        self._profiler.enable()
        self._start_time = time.perf_counter()

    def stop(self) -> None:
        """Stop profiling."""
        self._profiler.disable()
        self._end_time = time.perf_counter()

    def elapsed(self) -> float | None:
        """Return elapsed wall time in seconds."""
        if self._start_time is None or self._end_time is None:
            return None
        return self._end_time - self._start_time

    def get_stats(self, sort_by: str = "cumulative") -> pstats.Stats:
        """Return profiling statistics."""
        s = io.StringIO()
        ps = pstats.Stats(self._profiler, stream=s).sort_stats(sort_by)
        ps.print_stats()
        return ps

    def print(self, sort_by: str = "cumulative") -> None:
        """Print profiling statistics to the console."""
        stats = self.get_stats(sort_by)
        stats.print_stats()

    @contextmanager
    def __call__(self) -> Iterator[None]:
        """Context manager to profile a block of code."""
        self.start()
        try:
            yield
        finally:
            self.stop()

    def __enter__(self) -> "Self":
        self.start()
        return self

    def __exit__(self, *args: object) -> None:
        self.stop()


def profile(func: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator to profile a function."""

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        profiler = Profiler(name=func.__qualname__)
        with profiler:
            result = func(*args, **kwargs)
        logger.debug("=== Profiling %s ===", func.__qualname__)
        profiler.print()
        elapsed = profiler.elapsed()
        if elapsed is not None:
            logger.debug("Elapsed: %.3fs", elapsed)
        return result

    return wrapper


def generate_flamegraph(
    profiler: Profiler,
    output_path: str = "flamegraph.svg",
    *,
    flameprof_path: str = "flameprof",
) -> None:
    """
    Generate a flamegraph SVG from profiling statistics.

    Requires `flameprof` to be installed:
        pip install flameprof

    Args:
        profiler: A `Profiler` instance that has already been used.
        output_path: Path to write the SVG file.
        flameprof_path: Path to the `flameprof` executable if not in PATH.

    Raises:
        ImportError: If flameprof is not installed.
        FileNotFoundError: If flameprof executable not found.

    """
    if shutil.which(flameprof_path) is None:
        raise FileNotFoundError(f"flameprof executable not found at '{flameprof_path}'")

    if not HAS_FLAMEPROF:
        raise ImportError("flameprof package is required for flamegraph generation")

    stats = profiler.get_stats()
    reader = PstatsReader(stats)
    graph = FlameGraph()
    graph.load(reader)
    fs = FlameFS()
    fs.gen(graph)
    fs.save(output_path)
