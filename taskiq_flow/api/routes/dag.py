"""Routes API pour la visualisation des DAG.

Ce module fournit des endpoints FastAPI pour interroger les DAG
et obtenir des visualisations dans différents formats.

Auteur: SoniqueBay Team
Version: 0.4.5
"""

import io
from contextlib import redirect_stdout
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Path, Request

from taskiq_flow.pipeline import DataflowPipeline
from taskiq_flow.registry import get_pipeline
from taskiq_flow.security.dependencies import verify_pipeline_access
from taskiq_flow.visualization.dag_visualizer import DAGVisualizer

router = APIRouter(prefix="/dag", tags=["dag"])


# Singleton for dependency injection
_pipeline_store: dict[str, DataflowPipeline] = {}


def get_pipeline_dep(pipeline_id: str = Path(...)) -> DataflowPipeline:
    """Dependency to get pipeline by ID."""
    if pipeline_id not in _pipeline_store:
        raise HTTPException(404, f"Pipeline '{pipeline_id}' not found")
    return _pipeline_store[pipeline_id]


@router.get("/{pipeline_id}")
async def get_dag(
    pipeline_id: str,
    request: Request,
    format: str = "json",
    user: dict[str, Any] = Depends(verify_pipeline_access),  # noqa: B008
) -> Any:
    """
    Obtient la visualisation du DAG pour un pipeline.

    Args:
        pipeline_id: Identifiant du pipeline
        format: Format de sortie (json, cytoscape, graphviz, ascii)

    Returns:
        Représentation du DAG dans le format demandé

    Raises:
        HTTPException: Si le pipeline n'est pas trouvé ou format invalide
    """
    pipeline = get_pipeline(pipeline_id)
    dag = pipeline._dag

    if dag is None:
        raise HTTPException(400, "No DAG available for this pipeline")

    if format == "json":
        return DAGVisualizer.to_json_extended(dag)
    if format == "cytoscape":
        return DAGVisualizer.to_cytoscape_json(dag)
    if format == "graphviz":
        return {"dot": DAGVisualizer.to_dot(dag)}
    if format == "ascii":
        f = io.StringIO()
        with redirect_stdout(f):
            DAGVisualizer.print_ascii(dag)
        ascii_output = f.getvalue()
        return {"ascii": ascii_output}
    raise HTTPException(400, f"Format inconnu: {format}")


@router.get("/{pipeline_id}/critical-path")
async def get_critical_path(
    pipeline_id: str,
    request: Request,
    user: dict[str, Any] = Depends(verify_pipeline_access),  # noqa: B008
) -> Any:
    """
    Obtient le chemin critique du pipeline.

    Args:
        pipeline_id: Identifiant du pipeline

    Returns:
        Chemin critique (liste des tâches)

    Raises:
        HTTPException: Si le pipeline n'est pas trouvé ou contient des cycles
    """
    pipeline = get_pipeline(pipeline_id)
    dag = pipeline._dag

    if dag is None:
        raise HTTPException(400, "No DAG available for this pipeline")

    try:
        critical_path = DAGVisualizer.detect_critical_path(dag)
        return {
            "pipeline_id": pipeline_id,
            "critical_path": critical_path,
            "length": len(critical_path),
        }
    except ValueError as e:
        raise HTTPException(400, str(e)) from e


@router.get("/{pipeline_id}/parallel-groups")
async def get_parallel_groups(
    pipeline_id: str,
    request: Request,
    user: dict[str, Any] = Depends(verify_pipeline_access),  # noqa: B008
) -> Any:
    """
    Obtient les groupes de tâches parallélisables.

    Args:
        pipeline_id: Identifiant du pipeline

    Returns:
        Groupes de tâches parallélisables

    Raises:
        HTTPException: Si le pipeline n'est pas trouvé ou contient des cycles
    """
    pipeline = get_pipeline(pipeline_id)
    dag = pipeline._dag

    if dag is None:
        raise HTTPException(400, "No DAG available for this pipeline")

    try:
        groups = DAGVisualizer.find_parallelizable_groups(dag)
        return {
            "pipeline_id": pipeline_id,
            "parallel_groups": groups,
            "group_count": len(groups),
        }
    except ValueError as e:
        raise HTTPException(400, str(e)) from e


@router.get("/{pipeline_id}/networkx")
async def get_networkx_graph(
    pipeline_id: str,
    request: Request,
    user: dict[str, Any] = Depends(verify_pipeline_access),  # noqa: B008
) -> Any:
    """
    Obtient la représentation NetworkX du DAG.

    Args:
        pipeline_id: Identifiant du pipeline

    Returns:
        Graphe NetworkX sérialisé

    Raises:
        HTTPException: Si le pipeline n'est pas trouvé
    """
    pipeline = get_pipeline(pipeline_id)
    dag = pipeline._dag

    if dag is None:
        raise HTTPException(400, "No DAG available for this pipeline")

    try:
        graph = DAGVisualizer.to_networkx(dag)
        # Convertir en format sérialisable
        nodes = list(graph.nodes(data=True))
        edges = list(graph.edges(data=True))
        return {
            "nodes": [{"id": n, **d} for n, d in nodes],
            "edges": [{"source": u, "target": v, **d} for u, v, d in edges],
        }
    except Exception as e:
        raise HTTPException(400, str(e)) from e


# _get_pipeline helper removed - using registry.get_pipeline directly
# and authorization handled by verify_pipeline_access dependency.

__all__ = ["router"]
