"""Exporteur Prometheus pour Taskiq-Flow.

Ce module fournit un endpoint FastAPI pour exposer les métriques
au format Prometheus.

Auteur: SoniqueBay Team
Version: 1.0.2
"""

from collections.abc import Callable
from typing import Any

from fastapi import Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    generate_latest,
)

from taskiq_flow.metrics.collector import MetricsCollector


def get_metrics_endpoint(
    registry: CollectorRegistry | None = None,
    include_defaults: bool = False,
) -> Callable[[], Any]:
    """Return an async endpoint function that serves Prometheus metrics.

    Usage:
        ```python
        app.get("/metrics")(get_metrics_endpoint())
        ```

    Args:
        registry: Optional custom Prometheus registry. If None, uses
            the global registry or the MetricsCollector's registry.
        include_defaults: Whether to include Python/process defaults.

    Returns:
        Async callable that returns a Response with metrics payload.
    """

    async def endpoint() -> Response:
        """Generate Prometheus-format metrics response."""
        # Use provided registry or get the global collector registry
        if registry is not None:
            metrics_data = generate_latest(registry)
        else:
            # Export from our global MetricsCollector
            collector = MetricsCollector()
            metrics_data = collector.export_metrics()

        return Response(
            content=metrics_data,
            media_type=CONTENT_TYPE_LATEST,
            headers={"Content-Type": CONTENT_TYPE_LATEST},
        )

    return endpoint


__all__ = ["get_metrics_endpoint"]
