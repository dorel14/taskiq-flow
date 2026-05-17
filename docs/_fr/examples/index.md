---
title: Galerie d'Exemples
nav_order: 40
permalink: /fr/examples/
---
# Galerie d'Exemples

**Exemples fonctionnels démontrant les fonctionnalités et motifs clés de Taskiq-Flow**

> **Version** : {VERSION} | **Lié** : [Guide de Démarrage Rapide]({{ '/fr/quickstart/' | relative_url }})

---

## Aperçu

Cette galerie propose des parcours détaillés des scripts d'exemple inclus dans le répertoire `examples/`. Chaque exemple démontre une fonctionnalité spécifique ou un motif d'intégration.

---

## Index des Exemples

| Exemple | Description | Concepts Clés |
|---------|-------------|---------------|
| [Pipeline Basique]({{ '/fr/examples/quickstart/' | relative_url }}) | Pipeline séquentiel simple avec opérations map, filter, group | SequentialPipeline, étapes de base |
| [Démonstration Suivi]({{ '/fr/examples/tracking-demo/' | relative_url }}) | Surveillance en temps réel avec PipelineTrackingManager | Suivi, stockage d'état, visualisation |
| [Pipeline Planifié]({{ '/fr/examples/scheduled-pipeline/' | relative_url }}) | Exécution périodique de pipeline via cron | PipelineScheduler, APScheduler, fuseaux horaires |
| [Pipeline Audio Dataflow]({{ '/fr/examples/dataflow-audio-pipeline/' | relative_url }}) | DAG complet avec parallélisme, map-reduce et visualisation | DataflowPipeline, DAG automatique, parallélisme |
| [Démonstration Visualisation DAG]({{ '/fr/examples/dag-visualization-demo/' | relative_url }}) | Analyse DAG NetworkX : chemin critique, groupes parallèles, export | DAGVisualizer, NetworkX, chemin critique, exports |
| [Démo NiceGUI DAG]({{ '/fr/examples/nicegui-dag-demo/' | relative_url }}) | Visualiseur web interactif de DAG via NiceGUI et MermaidGenerator | MermaidGenerator, NiceGUI, visualisation web interactive |
| [Découverte Registry]({{ '/fr/examples/registry-discovery/' | relative_url }}) | Construction manuelle de DataflowRegistry, introspection DAG, exécution bas niveau | DataflowRegistry, ExecutionEngine, introspection |
| [Démo WebSocket]({{ '/fr/examples/websocket-demo/' | relative_url }}) | Streaming d'événements en temps réel via WebSockets | HookManager, transport WebSocket, suivi live |
| [API REST]({{ '/fr/examples/api-example/' | relative_url }}) | Intégration FastAPI pour gestion distante de pipelines | PipelineVisualizationAPI, endpoints personnalisés |

---

## Exécuter les Exemples

Chaque page d'exemple inclut :

- **Aperçu** — Ce que démontre l'exemple
- **Prérequis** — Dépendances et configuration requises
- **Parcours du code** — Explication ligne par ligne
- **Concepts clés** — Fonctionnalités illustrées
- **Instructions d'exécution** — Comment lancer le script
- **Sortie attendue** — Exemple de résultat pour vérification
- **Problèmes courants** — Conseils de dépannage

**Pour exécuter un exemple** :

```bash
# Se placer à la racine du dépôt
cd taskiq-flow

# Installer les dépendances si nécessaire
pip install -e .

# Lancer un script d'exemple
python examples/quickstart.py
```

Certains exemples nécessitent des services additionnels (Redis, etc.). Voir les pages individuelles pour les détails.

---

## Catégories d'Exemples

### Démarrage
- [Pipeline Basique]({{ '/fr/examples/quickstart/' | relative_url }}) — Commencez ici si vous êtes novice

### Monitoring & Opérations
- [Démonstration Suivi]({{ '/fr/examples/tracking-demo/' | relative_url }})
- [Pipeline Planifié]({{ '/fr/examples/scheduled-pipeline/' | relative_url }})
- [Démo WebSocket]({{ '/fr/examples/websocket-demo/' | relative_url }})

### Workflows Avancés
- [Pipeline Audio Dataflow]({{ '/fr/examples/dataflow-audio-pipeline/' | relative_url }})
- [Démonstration Visualisation DAG]({{ '/fr/examples/dag-visualization-demo/' | relative_url }})
- [Démo NiceGUI DAG]({{ '/fr/examples/nicegui-dag-demo/' | relative_url }})
- [Découverte Registry]({{ '/fr/examples/registry-discovery/' | relative_url }})

### Intégration
- [API REST]({{ '/fr/examples/api-example/' | relative_url }})

---

## Prochaines Étapes

- **[Guide de Démarrage Rapide]({{ '/fr/quickstart/' | relative_url }})** — Lancez votre premier pipeline
- **[Guides Utilisateur]({{ '/fr/guides/' | relative_url }})** — Approfondissements par fonctionnalité
- **[Référence API]({{ '/fr/api/' | relative_url }})** — Documentation complète des modules
