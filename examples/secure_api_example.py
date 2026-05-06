"""Secure API Example

This example demonstrates how to secure your Taskiq-Flow API with
authentication, authorization, rate limiting, and audit logging.

Author: SoniqueBay Team
Version: 0.4.5
"""

import asyncio
from fastapi import FastAPI
from taskiq import InMemoryBroker
from taskiq_flow import DataflowPipeline, pipeline_task, TaskiqFlowConfig, create_visualization_api
from taskiq_flow.security.auth import JWTAuthProvider, APIKeyAuthProvider
from taskiq_flow.security.authorization import PipelineAuthorization, Permission
from taskiq_flow.security.audit import AuditLogger


# 1. Create broker
broker = InMemoryBroker(await_inplace=True)


# 2. Define some demo tasks
@broker.task
@pipeline_task(output="result")
async def process_data(data: str) -> dict:
    """A simple processing task."""
    return {"processed": data.upper(), "status": "ok"}


@broker.task
@pipeline_task(output="validated")
async def validate_result(result: dict) -> dict:
    """Validate the processed result."""
    if result.get("status") != "ok":
        raise ValueError("Invalid result")
    return {**result, "validated": True}


# 3. Build pipeline
pipeline = DataflowPipeline.from_tasks(broker, [process_data, validate_result])
pipeline.pipeline_id = "secure_demo_pipeline"


# 4. Configure security
config = TaskiqFlowConfig(
    security_enabled=True,
    auth_provider="api_key",
    api_keys={
        "sk_admin_full": {
            "role": "admin",
            "pipelines": ["*"],  # Access to all pipelines
        },
        "sk_viewer_reports": {
            "role": "viewer",
            "pipelines": ["report_*"],  # Only pipelines starting with 'report_'
        },
    },
    require_https=False,  # Set to True in production
    rate_limit_enabled=True,
    rate_limit_default="60/minute",
}


# 5. Create FastAPI app with security
app = FastAPI(title="Secure Taskiq-Flow API", version="0.4.5")
viz_api = create_visualization_api(broker, app, config=config)
viz_api.add_pipeline("secure_demo_pipeline", pipeline)


# 6. Optional: Add custom audit logger
audit_logger = AuditLogger()


@app.post("/execute-with-audit")
async def execute_with_audit(data: str, user: str = "demo_user"):
    """Execute pipeline with audit logging."""
    # Log the execution request
    await audit_logger.log_access(
        user={"sub": user},
        action="execute_pipeline",
        pipeline_id=pipeline.pipeline_id,
        success=True,
        details={"input_length": len(data)},
    )

    # Execute
    result = await pipeline.kiq_dataflow(data=data)
    return {"task_id": result.task_id, "status": "started"}


# 7. Run the server (for demo purposes)
if __name__ == "__main__":
    import uvicorn
    print("Starting secure API server...")
    print("API Key for admin: sk_admin_full")
    print("API Key for viewer: sk_viewer_reports")
    print("\nTest with:")
    print('  curl -H "X-API-Key: sk_admin_full" http://localhost:8000/pipelines')
    print("\nDocs at: http://localhost:8000/docs")

    uvicorn.run(app, host="0.0.0.0", port=8000)
