"""Registre global des pipelines pour l'API de visualisation.

Ce module fournit un registre simple en mémoire des pipelines
accessibles via les endpoints DAG. En production, cela pourrait
être remplacé par un backend de persistance.

Auteur: SoniqueBay Team
Version: 0.4.5
"""

from typing import Dict

from fastapi import HTTPException

from taskiq_flow.pipeline import DataflowPipeline

# Registre global (single-process pour v0.4.5)
_pipelines: Dict[str, DataflowPipeline] = {}


def register_pipeline(pipeline_id: str, pipeline: DataflowPipeline) -> None:
    """Enregistre un pipeline dans le registre global.

    Args:
        pipeline_id: Identifiant unique du pipeline
        pipeline: Instance DataflowPipeline
    """
    _pipelines[pipeline_id] = pipeline


def get_pipeline(pipeline_id: str) -> DataflowPipeline:
    """Récupère un pipeline par son ID.

    Args:
        pipeline_id: Identifiant du pipeline

    Returns:
        Instance DataflowPipeline

    Raises:
        HTTPException: 404 si le pipeline n'existe pas
    """
    if pipeline_id not in _pipelines:
        raise HTTPException(
            status_code=404,
            detail=f"Pipeline '{pipeline_id}' not found. "
            "Ensure the pipeline has been registered via API or add_pipeline().",
        )
    return _pipelines[pipeline_id]


def unregister_pipeline(pipeline_id: str) -> None:
    """Supprime un pipeline du registre."""
    _pipelines.pop(pipeline_id, None)


def list_pipeline_ids() -> list[str]:
    """Liste tous les IDs de pipelines enregistrés."""
    return list(_pipelines.keys())


__all__ = [
    "register_pipeline",
    "get_pipeline",
    "unregister_pipeline",
    "list_pipeline_ids",
]
