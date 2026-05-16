---
title: Exemple: nicegui_dag_demo.py
nav_order: 48
color_scheme: dark
---
# Exemple: nicegui_dag_demo.py

**Visualisation interactive de DAG avec NiceGUI et MermaidGenerator**

> **Version** : {VERSION} | **Fichier** : `examples/nicegui_dag_demo.py`

---

## Aperçu

Cet exemple montre comment construire une application web interactive avec
**NiceGUI** pour visualiser le DAG d'un `DataflowPipeline` Taskiq-Flow. Il
s'appuie sur le `MermaidGenerator` intégré pour générer un diagramme Mermaid.js
visible directement dans le navigateur.

Thèmes abordés :
- Génération de diagrammes Mermaid.js depuis un DAG de façon programmatique
- Diffusion du diagramme par une application web NiceGUI
- Séparation entre la construction du pipeline (async) et le serveur web (synchrone)

---

## Ce Que Cet Exemple Montre

- Utilisation de `DataflowPipeline.from_tasks()` pour créer un pipeline à 3 étapes
- Appel à `_build_dataflow_dag()` pour obtenir le DAG **sans exécuter les tâches**
- Utilisation de `MermaidGenerator.to_mermaid_with_styling()` pour générer le code Mermaid
- Affichage du diagramme dans une page `ui.markdown()` NiceGUI
- Script en mode double : builder async + serveur web synchrone

---

## Parcours Du Code

### 1. Définition du Pipeline

```python
from taskiq import InMemoryBroker
from taskiq_flow import DataflowPipeline, pipeline_task

broker = InMemoryBroker(await_inplace=True)

@broker.task
@pipeline_task(output="raw_data")
async def load_data(source: str) -> dict[str, Any]:
    """Charge les données brutes depuis une source."""
    await asyncio.sleep(0.1)
    return {"source": source, "values": [1, 2, 3, 4, 5]}

@broker.task
@pipeline_task(output="processed_data")
async def process_data(raw_data: dict[str, Any]) -> dict[str, Any]:
    """Traite les données brutes en calculant des statistiques."""
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
    """Génère un rapport final depuis les données traitées."""
    await asyncio.sleep(0.1)
    return {
        "report_id": "RAPPORT-001",
        "source": processed_data["source"],
        "statistiques": {
            "count": processed_data["count"],
            "sum": processed_data["sum"],
            "mean": processed_data["mean"],
        },
        "statut": "terminé",
    }
```

Structure du DAG :
```
load_data → process_data → generate_report
```

---

### 2. Construction du DAG (Sans Exécution)

```python
pipeline = DataflowPipeline.from_tasks(
    broker,
    [load_data, process_data, generate_report],
)
pipeline.pipeline_id = "pipeline_traitement_donnees"

# Construit le DAG de façon statique — aucune tâche n'est exécutée
pipeline._build_dataflow_dag()
dag = pipeline._dag
```

> **Note** : `_build_dataflow_dag()` est une méthode interne qui parcourt
> les annotations `@pipeline_task` pour assembler le graphe de dépendances
> en mémoire. Pour une approche publique, utilisez `DataflowRegistry`
> (voir [Exemple Découverte Registry]({{ '/fr/examples/registry-discovery/' | relative_url }})).

---

### 3. Génération du Diagramme Mermaid

```python
from taskiq_flow.visualization.mermaid import MermaidGenerator

mermaid_gen = MermaidGenerator(dag)
mermaid_code = mermaid_gen.to_mermaid_with_styling()
# → flowchart LR / TD avec nœuds colorés et flèches
```

`MermaidGenerator.to_mermaid_with_styling()` génère du code Mermaid.js
avec des classes CSS par type de nœud (entrée, processus, sortie, décision).

---

### 4. Application NiceGUI

```python
@ui.page("/")
def page_visualisation_dag() -> None:
    """Page principale de visualisation du DAG."""
    ui.label("Visualisation DAG TaskIQ Flow").style("font-size: 24px; font-weight: bold;")
    ui.label("Pipeline de Traitement de Données").style("font-size: 18px;")
    ui.separator()

    if mermaid_diagramme_global:
        ui.markdown(f"""```mermaid\n{mermaid_diagramme_global}\n```""")
    else:
        ui.label("Aucun DAG à afficher").style("color: red;")

    ui.separator()
    ui.label("Statistiques du DAG:").style("font-weight: bold;")

ui.run(title="Visionneuse DAG TaskIQ Flow", port=8080)
```

NiceGUI interprète le contenu `ui.markdown()` côté serveur, et Mermaid.js
charge le rendu du diagramme côté navigateur.

---

## Sortie Attendue

**Console (builder DAG)** :
```
=== Démo Visualisation DAG NiceGUI Taskiq-Flow ===

DAG: 3 nœuds et 2 arêtes
Serveur NiceGUI démarré...
Ouvrez http://127.0.0.1:8080 dans votre navigateur

Code du diagramme Mermaid :
flowchart LR
    classDef input fill:#e1f5fe,stroke:#01579b,...
    classDef process fill:#fff3e0,stroke:#e65100,...
    classDef output fill:#e8f5e9,stroke:#1b5e20,...
    load_data["load_data"]:::process
    process_data["process_data"]:::process
    generate_report["generate_report"]:::output
    load_data --> process_data
    process_data --> generate_report
```

**Navigateur sur `http://127.0.0.1:8080`** :

Application web NiceGUI complète affichant le diagramme Mermaid avec nœuds
colorés et les statistiques de base du DAG.

---

## Référence MermaidGenerator

| Méthode | Description |
|---------|-------------|
| `MermaidGenerator(dag)` | Crée un générateur depuis une instance de `DAG` |
| `.to_mermaid(orientation="TB")` | Flowchart Mermaid basique (TB/BT/LR/RL) |
| `.to_mermaid_with_styling(orientation="LR")` | Stylisé avec couleurs par type de nœud |
| `.to_mermaid_interactive()` | HTML avec gestionnaires de clic (pour tableaux de bord web) |

Valeurs d'orientation :

| Valeur | Direction |
|--------|-----------|
| `"TB"` | Haut → Bas (défaut) |
| `"BT"` | Bas → Haut |
| `"LR"` | Gauche → Droite |
| `"RL"` | Droite → Gauche |

---

## Lancer la Démo

### Prérequis

```bash
pip install nicegui "taskiq-flow[all]"
```

### Option 1 — Construire le DAG et démarrer NiceGUI

```bash
python examples/nicegui_dag_demo.py
```

Le script exécute `asyncio.run(main())`, qui construit le DAG et génère le
diagramme, puis affiche les instructions pour lancer NiceGUI.

### Option 2 — Lancer NiceGUI directement

```bash
python -c "from examples.nicegui_dag_demo import run_nicegui_app; run_nicegui_app()"
```

> Ouvre `http://127.0.0.1:8080` dans votre navigateur par défaut.

---

## Points Clés

### Intégration NiceGUI + Taskiq-Flow

| Composant | Rôle |
|-----------|------|
| `DataflowPipeline` | Construit le DAG depuis les déclarations de tâches |
| `MermaidGenerator` | Convertit le DAG → code Mermaid.js |
| `ui.markdown()` | Affiche le diagramme Mermaid dans NiceGUI |
| `ui.run()` | Démarre le serveur web ASGI sur le port 8080 |

### Pourquoi Séparer Builder et Serveur

L'exemple dissocie la construction du pipeline (`main()`) du serveur
NiceGUI (`run_nicegui_app()`) car :
- `ui.run()` est un appel bloquant et ne peut pas coexister avec `asyncio.run()`
- En production, générez le code Mermaid au démarrage ou lors des changements de DAG
- Utilisez `ui.timer()` ou un système pub/sub pour rafraîchir le diagramme dynamiquement

### Alternatives à NiceGUI pour la Visualisation de DAG

| Outil | Cas d'usage |
|-------|-------------|
| `MermaidGenerator` | Diagrammes statiques (docs, wikis, README) |
| `DAGVisualizer` | Analyse NetworkX, exports JSON/DOT/Cytoscape |
| `DAGViewer` (NiceGUI) | Panneaux web interactifs (en cours d'implémentation) |

---

## Chemin d'Apprentissage

Après cet exemple :

1. **[Démonstration Visualisation DAG]({{ '/fr/examples/dag-visualization-demo/' | relative_url }})** — Réseau critique, exports multi-formats NetworkX
2. **[Guide Dataflow]({{ '/fr/guides/dataflow/' | relative_url }})** — Construction et inspection du DAG
3. **[Pipeline Audio Dataflow]({{ '/fr/examples/dataflow-audio-pipeline/' | relative_url }})** — Pipeline audio complet avec parallélisme
4. **[Guide des Pipelines]({{ '/fr/guides/pipelines/' | relative_url }})** — Tous les types et patterns de pipelines

---

*Cet exemple montre comment combiner Taskiq-Flow avec NiceGUI pour une visualisation interactive de pipelines.*
