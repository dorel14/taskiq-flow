"""
Composants d'intégration NiceGUI pour la visualisation des DAG.

Ce module fournit des composants NiceGUI pour afficher les DAG
avec des fonctionnalités interactives.

Auteur: SoniqueBay Team
Version: 1.0.2
"""

import json
from typing import Any

from nicegui import ui

from taskiq_flow.dataflow.dag import DAG
from taskiq_flow.visualization.dag_visualizer import DAGVisualizer
from taskiq_flow.visualization.mermaid import MermaidGenerator


class DAGViewer:
    """Composant NiceGUI pour la visualisation interactive des DAG."""

    def __init__(self, dag: DAG | None = None) -> None:
        """
        Initialise le visualiseur DAG.

        Args:
            dag: DAG à afficher (peut être défini plus tard)

        """
        self.dag = dag
        self.mermaid_gen = MermaidGenerator(dag) if dag else None
        self._selected_node: str | None = None
        self._status_data: dict[str, Any] = {}
        self._main_container: ui.column | None = None  # Pour le rafraîchissement

    def set_dag(self, dag: DAG) -> None:
        """
        Met à jour le DAG affiché.

        Args:
            dag: Nouveau DAG

        """
        self.dag = dag
        self.mermaid_gen = MermaidGenerator(dag)
        self.refresh()

    def set_status_data(self, status_data: dict[str, Any]) -> None:
        """
        Met à jour les informations de statut des nœuds.

        Args:
            status_data: Données de statut

        """
        self._status_data = status_data
        self.refresh()

    def render_mermaid(self) -> None:
        """Affiche le diagramme Mermaid."""
        if not self.mermaid_gen or not self.dag:
            ui.label("Aucun DAG à afficher")
            return

        mermaid_code = self.mermaid_gen.to_mermaid_with_styling()

        # Utiliser le composant Mermaid de NiceGUI
        ui.markdown(f"""
        ```mermaid
        {mermaid_code}
        ```
        """)

    def render_interactive(self) -> None:
        """Affiche un DAG interactif avec détails."""
        if not self.dag:
            return

        # Créer ou réinitialiser le conteneur principal
        if self._main_container is None:
            self._main_container = ui.column().classes("w-full")
        else:
            self._main_container.clear()

        with self._main_container, ui.row().classes("w-full"):
            # Panneau gauche: Diagramme
            with ui.column().classes("w-2/3"):
                ui.label("Pipeline DAG").classes("text-h6")
                self.render_mermaid()

            # Panneau droit: Détails
            with ui.column().classes("w-1/3"):
                ui.label("Détails").classes("text-h6")
                self._details_panel = ui.column()
                self._update_details_panel()

    def render_with_cytoscape(self) -> None:
        """Affiche le DAG avec Cytoscape.js pour une interactivité avancée."""
        if not self.dag:
            return

        cytoscape_data = DAGVisualizer.to_cytoscape_json(self.dag)

        # Injecter Cytoscape.js + Dagre (pour layout dagre)
        ui.add_head_html("""
        <script src="https://cdnjs.cloudflare.com/ajax/libs/cytoscape/3.23.0/cytoscape.min.js"></script>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/dagre/0.8.5/dagre.min.js"></script>
        <script src="https://cdn.jsdelivr.net/npm/cytoscape-dagre@2.5.0/cytoscape-dagre.min.js"></script>
        <style>
        #cy {
            width: 100%;
            height: 600px;
            border: 1px solid #ccc;
        }
        </style>
        """)

        # Créer le conteneur
        ui.html(f"""
        <div id="cy"></div>
        <script>
        document.addEventListener('DOMContentLoaded', function() {{
            // Register dagre layout with cytoscape
            if (typeof dagre !== 'undefined') {{
                cytoscape.use(dagre);
            }} else {{
                console.warn('dagre.js not loaded; dagre layout will not work');
            }}
            var cy = cytoscape({{
                container: document.getElementById('cy'),
                elements: {json.dumps(cytoscape_data)},
                style: [
                    {{
                        selector: 'node',
                        style: {{
                            'background-color': '#666',
                            'label': 'data(label)',
                            'color': '#fff',
                            'text-valign': 'center',
                            'text-halign': 'center'
                        }}
                    }},
                    {{
                        selector: 'edge',
                        style: {{
                            'width': 2,
                            'line-color': '#ccc',
                            'target-arrow-color': '#ccc',
                            'target-arrow-shape': 'triangle',
                            'curve-style': 'bezier'
                        }}
                    }}
                ],
                layout: {{
                    name: 'dagre',
                    animate: true,
                    animationDuration: 500
                }}
            }});

            cy.on('tap', 'node', function(evt) {{
                var node = evt.target;
                console.log('Node tapped:', node.id());
            }});
        }});
        </script>
        """)

    def _update_details_panel(self) -> None:
        """Met à jour le panneau de détails."""
        with self._details_panel:
            ui.label("Sélectionnez un nœud dans le diagramme").classes("text-caption")

    def refresh(self) -> None:
        """Rafraîchit l'affichage du DAG."""
        # Re-render la vue interactive avec le DAG actuel
        self.render_interactive()

    def render_critical_path(self) -> None:
        """Affiche le chemin critique du DAG."""
        if not self.dag:
            return

        try:
            critical_path = DAGVisualizer.detect_critical_path(self.dag)
            ui.label("Chemin critique:").classes("text-h6")
            ui.label(" → ".join(critical_path)).classes("text-body1")
        except Exception as e:
            ui.label(f"Erreur: {e}").classes("text-red")

    def render_parallel_groups(self) -> None:
        """Affiche les groupes de tâches parallélisables."""
        if not self.dag:
            return

        try:
            groups = DAGVisualizer.find_parallelizable_groups(self.dag)
            ui.label("Groupes parallélisables:").classes("text-h6")
            for i, group in enumerate(groups):
                with ui.row().classes("w-full"):
                    ui.label(f"Niveau {i}:").classes("text-subtitle2")
                    ui.label(", ".join(group))
        except Exception as e:
            ui.label(f"Erreur: {e}").classes("text-red")


class LiveDAGPreview:
    """Aperçu en direct du DAG avec rafraîchissement automatique."""

    def __init__(self, update_interval: float = 1.0) -> None:
        """
        Initialise l'aperçu en direct.

        Args:
            update_interval: Intervalle de rafraîchissement en secondes

        """
        self.update_interval = update_interval
        self.dag: DAG | None = None
        self._running = False
        self._card: ui.card | None = None
        self._mermaid_label: ui.markdown | None = None
        self._stats_label: ui.label | None = None

    def start(self, dag: DAG) -> None:
        """
        Démarre l'aperçu en direct.

        Args:
            dag: DAG à afficher

        """
        self.dag = dag
        self._running = True

        with ui.card().classes("w-full") as self._card:
            ui.label("Pipeline DAG en direct").classes("text-h5")
            with ui.row().classes("w-full"):
                with ui.column().classes("w-1/2"):
                    self._mermaid_label = ui.markdown("""
                    ```mermaid
                    flowchart TD
                        A[Chargement...]
                    ```
                    """)
                with ui.column().classes("w-1/2"):
                    ui.label("Statistiques").classes("text-subtitle1")
                    self._stats_label = ui.label("")

            ui.button("Pause", on_click=self._toggle_pause)
            ui.button("Rafraîchir", on_click=self._refresh)

        # Démarrer la boucle de mise à jour
        ui.timer(self.update_interval, self._update_loop)

    async def _update_loop(self) -> None:
        """Boucle de mise à jour continue."""
        if self._running and self.dag:
            self._refresh()

    def _refresh(self) -> None:
        """Rafraîchit l'affichage."""
        if not self.dag or not self._mermaid_label or not self._stats_label:
            return

        mermaid_gen = MermaidGenerator(self.dag)
        mermaid_code = mermaid_gen.to_mermaid_with_styling()

        self._mermaid_label.set_content(f"""
        ```mermaid
        {mermaid_code}
        ```
        """)

        # Mettre à jour les statistiques
        stats = f"""
        Nœuds: {len(self.dag.nodes)}
        Arêtes: {len(self.dag.edges)}
        Niveaux: {max(n.level for n in self.dag.nodes) + 1 if self.dag.nodes else 0}
        """
        self._stats_label.set_text(stats)

    def _toggle_pause(self) -> None:
        """Met en pause/reprend le rafraîchissement."""
        self._running = not self._running

    def stop(self) -> None:
        """Arrête l'aperçu."""
        self._running = False


# Fonction de commodité
def view_dag(dag: DAG, interactive: bool = True) -> DAGViewer:
    """
    Affiche un DAG dans NiceGUI.

    Args:
        dag: DAG à afficher
        interactive: Afficher en mode interactif

    Returns:
        Visualiseur DAG

    """
    viewer = DAGViewer(dag)

    if interactive:
        viewer.render_interactive()
    else:
        viewer.render_mermaid()

    return viewer


__all__ = ["DAGViewer", "LiveDAGPreview", "view_dag"]
