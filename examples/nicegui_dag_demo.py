"""
Démonstration NiceGUI de visualisation de DAG pour Taskiq-Flow.

Ce module utilise le MermaidGenerator intégré pour générer un diagramme
Mermaid.js depuis un pipeline DataflowPipeline et l'affiche dans une
application web interactive construite avec NiceGUI.

Permissions d'exécution :
    - Dépendances : pip install nicegui taskiq-flow[all]
    - Lancement   : python examples/nicegui_dag_demo.py

Auteur: SoniqueBay Team
Version: 1.2.0
"""

import asyncio
import logging
from typing import Any

from taskiq import InMemoryBroker

from taskiq_flow import DataflowPipeline, pipeline_task
from taskiq_flow.visualization.mermaid import MermaidGenerator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create broker
broker = InMemoryBroker(await_inplace=True)


# Define sample tasks for a data processing pipeline
@broker.task
@pipeline_task(output="raw_data")
async def load_data(source: str) -> dict[str, Any]:
    """Load raw data from a source."""
    await asyncio.sleep(0.1)
    return {
        "source": source,
        "values": [1, 2, 3, 4, 5],
    }


@broker.task
@pipeline_task(output="processed_data")
async def process_data(raw_data: dict[str, Any]) -> dict[str, Any]:
    """Process raw data by calculating statistics."""
    await asyncio.sleep(0.2)
    values = raw_data["values"]
    return {
        "source": raw_data["source"],
        "count": len(values),
        "sum": sum(values),
        "mean": sum(values) / len(values),
    }


@broker.task
@pipeline_task(output="result")
async def generate_report(processed_data: dict[str, Any]) -> dict[str, Any]:
    """Generate a final report from processed data."""
    await asyncio.sleep(0.1)
    return {
        "report_id": "REPORT-001",
        "source": processed_data["source"],
        "statistics": {
            "count": processed_data["count"],
            "sum": processed_data["sum"],
            "mean": processed_data["mean"],
        },
        "status": "completed",
    }


async def main() -> None:
    """Run the NiceGUI DAG visualization demo."""
    logger.info("=== Taskiq-Flow NiceGUI DAG Visualization Demo ===\n")

    # Create pipeline from tasks
    pipeline = DataflowPipeline.from_tasks(
        broker,
        [
            load_data,  # type: ignore
            process_data,  # type: ignore
            generate_report,  # type: ignore
        ],
    )
    pipeline.pipeline_id = "data_processing_pipeline"

    # Build the DAG (without executing)
    pipeline._build_dataflow_dag()
    dag = pipeline._dag

    if dag is None:
        logger.error("Failed to build DAG")
        return

    logger.info(f"DAG has {len(dag.nodes)} nodes and {len(dag.edges)} edges")
    logger.info("Starting NiceGUI server...")
    logger.info("Open your browser to http://127.0.0.1:8080 to view the DAG")

    # Generate Mermaid code for the DAG
    mermaid_gen = MermaidGenerator(dag)
    mermaid_code = mermaid_gen.to_mermaid_with_styling()

    # Store for the NiceGUI app
    global mermaid_diagram_global  # noqa: PLW0603
    mermaid_diagram_global = mermaid_code

    # logger.info the mermaid diagram for reference
    logger.info("Mermaid diagram code:")
    logger.info(mermaid_code)


# Global to store mermaid diagram
mermaid_diagram_global = ""


def run_nicegui_app() -> None:
    """
    Run the NiceGUI application to display the DAG.

    This function requires NiceGUI to be installed.
    Install with: pip install nicegui
    """
    try:
        from nicegui import ui  # type: ignore # noqa: PLC0415
    except ImportError:
        logger.error("NiceGUI is not installed. Install with: pip install nicegui")
        raise

    @ui.page("/")
    def dag_viewer_page() -> None:
        """Main page showing the DAG visualization."""
        ui.label("TaskIQ Flow DAG Visualization").style("font-size: 24px; \
                                                        font-weight: bold;")
        ui.label("Data Processing Pipeline").style("font-size: 18px;")
        ui.separator()

        # Display the Mermaid diagram
        if mermaid_diagram_global:
            ui.markdown(f"""
```mermaid
{mermaid_diagram_global}
```
            """)
        else:
            ui.label("No DAG to display").style("color: red;")

        # Display DAG statistics
        ui.separator()
        ui.label("DAG Statistics:").style("font-weight: bold;")

    ui.run(title="TaskIQ Flow DAG Viewer", port=8080)


if __name__ in {"__main__", "__mp_main__"}:
    # Run the async demo first to build the DAG and generate mermaid code
    asyncio.run(main())

    # Then run the NiceGUI app
    # Note: In practice, you would want to generate the mermaid code
    # before starting NiceGUI, or use the asyncio pattern properly
    logger.info("")
    logger.info("=" * 60)
    logger.info("To run the NiceGUI DAG visualization demo:")
    logger.info("1. Make sure you have nicegui installed: pip install nicegui")
    logger.info("2. Run: python -m examples.nicegui_dag_demo")
    logger.info("3. Open http://127.0.0.1:8080 in your browser")
    logger.info("=" * 60)
    logger.info("")
    logger.info("The Mermaid diagram code has been generated above.")
    logger.info("You can also view the DAG by running the NiceGUI app:")
    logger.info("  python -c 'from examples.nicegui_dag_demo import\
                run_nicegui_app; run_nicegui_app()'")
