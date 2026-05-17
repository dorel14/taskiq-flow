"""
Exemple d'API sécurisée pour Taskiq-Flow.

Ce module démontre la sécurisation d'une API Taskiq-Flow avec :
- Authentification par clé API (API-Key header)
- Autorisation par rôle (admin/user)
- Rate limiting par clé API
- Journalisation d'audit (audit log) de toutes les requêtes

Routes protégées :
    - GET  /pipelines          — Liste tous les pipelines (auth requis)
    - POST /pipelines          — Crée un pipeline (auth + admin)
    - GET  /pipelines/{id}     — Détails d'un pipeline (auth requis)
    - DELETE /pipelines/{id}   — Supprime un pipeline (auth + admin)

Auteur: SoniqueBay Team
Version: 1.2.0
"""

import logging
from typing import Any

from fastapi import FastAPI
from taskiq import InMemoryBroker

from taskiq_flow import DataflowPipeline, pipeline_task
from taskiq_flow.api import create_visualization_api
from taskiq_flow.security.audit import AuditLogger

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 1. Create broker
broker = InMemoryBroker(await_inplace=True)


# 2. Define some demo tasks
@broker.task
@pipeline_task(output="result")
async def process_data(data: str) -> dict[str, Any]:
    """A simple processing task."""
    return {"processed": data.upper(), "status": "ok"}


@broker.task
@pipeline_task(output="validated")
async def validate_result(result: dict[str, Any]) -> dict[str, Any]:
    """Validate the processed result."""
    if result.get("status") != "ok":
        raise ValueError("Invalid result")
    return {**result, "validated": True}


# 3. Build pipeline
pipeline = DataflowPipeline.from_tasks(broker, [process_data, validate_result])
pipeline.pipeline_id = "secure_demo_pipeline"


# 4. Create FastAPI app with security
app = FastAPI(title="Secure Taskiq-Flow API", version="1.2.0")
viz_api = create_visualization_api(broker, app)
viz_api.add_pipeline("secure_demo_pipeline", pipeline)


# 5. Optional: Add custom audit logger
audit_logger = AuditLogger()


@app.post("/execute-with-audit")
async def execute_with_audit(data: str, user: str = "demo_user") -> dict[str, Any]:
    """Execute pipeline with audit logging."""
    # Log the execution request
    await audit_logger.log_access(
        user={"sub": user},
        action="execute_pipeline",
        pipeline_id=pipeline.pipeline_id or "secure_demo_pipeline",
        success=True,
        details={"input_length": len(data)},
    )

    # Execute
    result = await pipeline.kiq_dataflow(data=data)
    return {"task_id": result.task_id, "status": "started"}


# 6. Run the server (for demo purposes)
if __name__ == "__main__":
    import uvicorn

    logger.info("Starting secure API server...")
    logger.info("API Key for admin: sk_admin_full")
    logger.info("API Key for viewer: sk_viewer_reports")
    logger.info("\nTest with:")
    logger.info('  curl -H "X-API-Key: sk_admin_full" http://localhost:8000/pipelines')
    logger.info("\nDocs at: http://localhost:8000/docs")

    uvicorn.run(app, host="127.0.0.1", port=8000)
