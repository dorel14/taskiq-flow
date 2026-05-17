"""
Générateur de diagrammes Mermaid pour l'intégration NiceGUI.

Ce module fournit MermaidGenerator pour créer des diagrammes
Mermaid.js à partir des DAG TaskIQ-Flow.

Auteur: SoniqueBay Team
Version: 1.0.2
"""

from typing import Any

from taskiq_flow.dataflow.dag import DAG


class MermaidGenerator:
    """Génère des diagrammes Mermaid à partir des DAG."""

    def __init__(self, dag: DAG) -> None:
        """
        Initialise le générateur Mermaid.

        Args:
            dag: DAG à convertir

        """
        self.dag = dag

    def to_mermaid(self, orientation: str = "TB") -> str:
        """
        Génère le code Mermaid pour le diagramme.

        Args:
            orientation: Orientation du diagramme (TB, BT, LR, RL)

        Returns:
            Code Mermaid

        """
        orientations = {"TB": "TD", "BT": "BT", "LR": "LR", "RL": "RL"}
        direction = orientations.get(orientation, "TD")

        lines = [f"flowchart {direction}"]

        # Ajouter les nœuds
        for node in self.dag.nodes:
            lines.append(f'    {node.task.task_name}["{node.task.task_name}"]')

        # Ajouter les arêtes
        for from_node, to_node in self.dag.edges:
            lines.append(f"    {from_node.task.task_name} --> {to_node.task.task_name}")

        return "\n".join(lines)

    def to_mermaid_with_styling(self, orientation: str = "LR") -> str:
        """
        Génère un diagramme Mermaid stylisé avec couleurs et formes.

        Args:
            orientation: Orientation du diagramme

        Returns:
            Code Mermaid avec styles

        """
        directions = {"TB": "TD", "BT": "BT", "LR": "LR", "RL": "RL"}
        direction = directions.get(orientation, "LR")

        lines = [f"flowchart {direction}"]
        lines.append("    classDef input fill:#e1f5fe,stroke:#01579b,stroke-width:2px;")
        lines.append(
            "    classDef process fill:#fff3e0,stroke:#e65100,stroke-width:2px;"
        )
        lines.append(
            "    classDef output fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px;"
        )
        lines.append(
            "    classDef decision fill:#fce4ec,stroke:#880e4f,stroke-width:2px;"
        )

        # Identifier les entrées externes
        all_inputs = set()
        all_outputs = set()
        for node in self.dag.nodes:
            # Récupérer les métadonnées de la tâche
            metadata = self._get_task_metadata(node.task)
            all_inputs.update(metadata.get("inputs", []))
            all_outputs.add(metadata.get("output", node.task.task_name))

        # Ajouter les nœuds d'entrée externes
        external_inputs = all_inputs - all_outputs
        for inp in external_inputs:
            lines.append(f'    {inp}_input["{inp}"]:::input')

        # Ajouter les nœuds de tâche
        for node in self.dag.nodes:
            node_class = "process"
            task_name_lower = node.task.task_name.lower()
            if "condition" in task_name_lower or "branch" in task_name_lower:
                node_class = "decision"
            lines.append(
                f'    {node.task.task_name}["{node.task.task_name}"]:::{node_class}'
            )

        # Ajouter les arêtes depuis les entrées externes
        for inp in external_inputs:
            for node in self.dag.nodes:
                metadata = self._get_task_metadata(node.task)
                if inp in metadata.get("inputs", []):
                    lines.append(f"    {inp}_input --> {node.task.task_name}")

        # Ajouter les arêtes entre tâches
        for from_node, to_node in self.dag.edges:
            lines.append(f"    {from_node.task.task_name} --> {to_node.task.task_name}")

        return "\n".join(lines)

    def _get_task_metadata(self, task: Any) -> dict[str, Any]:
        """
        Récupère les métadonnées d'une tâche.

        Args:
            task: Tâche

        Returns:
            Métadonnées de la tâche

        """
        # Vérifier si la tâche a des métadonnées attachées
        if hasattr(task, "original_function"):
            original = getattr(task, "original_function", None)
            if original is not None and hasattr(original, "_pipeline_metadata"):
                meta = original._pipeline_metadata
                if hasattr(meta, "__dict__"):
                    return meta.__dict__
                return meta if isinstance(meta, dict) else {}

        if hasattr(task, "_pipeline_metadata"):
            meta = task._pipeline_metadata
            if hasattr(meta, "__dict__"):
                return meta.__dict__
            return meta if isinstance(meta, dict) else {}

        return {}

    def to_mermaid_interactive(self, include_status: bool = False) -> str:
        """
        Génère un diagramme Mermaid interactif avec gestionnaires de clic.

        Args:
            include_status: Inclure les informations de statut

        Returns:
            Code HTML/JavaScript pour NiceGUI

        """
        mermaid_code = self.to_mermaid_with_styling()

        return f"""
<script>
// Configuration Mermaid
const mermaidConfig = {{
    startOnLoad: true,
    theme: 'default',
    securityLevel: 'loose',
    flowchart: {{
        curve: 'basis',
        nodeSpacing: 50,
        rankSpacing: 50
    }}
}};

mermaid.initialize(mermaidConfig);

// Gestionnaire de clic sur les nœuds
function onNodeClick(nodeId) {{
    console.log('Node clicked:', nodeId);
    // Émettre un événement vers le backend Python
    // Peut récupérer des informations détaillées sur le nœud, le statut, etc.
}}
</script>

<div class="mermaid">
{mermaid_code}
</div>
"""


__all__ = ["MermaidGenerator"]
