---
title: Galerie d'Exemples
nav_order: 40
---
# Galerie d'Exemples

**Exemples fonctionnels démontrant les fonctionnalités et motifs clés de Taskiq-Flow**

> **Version** : 0.3.2 | **Lié** : [Guide de Démarrage Rapide]({{ '/fr/quickstart/' | relative_url }})

---

## Aperçu

Cette galerie propose des parcours détaillés des scripts d'exemple inclus dans le répertoire `examples/`. Chaque exemple démontre une fonctionnalité spécifique ou un motif d'intégration.

---

## Index des Exemples

| Exemple | Description | Concepts Clés |
|---------|-------------|---------------|
| [Pipeline Basique](quickstart.md) | Pipeline séquentiel simple avec opérations map, filter, group | SequentialPipeline, étapes de base |
| [Démonstration Suivi](tracking-demo.md) | Surveillance en temps réel avec PipelineTrackingManager | Suivi, stockage d'état, visualisation |
| [Pipeline Planifié](scheduled-pipeline.md) | Exécution périodique de pipeline via cron | PipelineScheduler, APScheduler, fuseaux horaires |
| [Pipeline Audio Dataflow](dataflow-audio-pipeline.md) | DAG complet avec parallélisme, map-reduce et visualisation | DataflowPipeline, DAG automatique, parallélisme |
| [Découverte Registry](registry-discovery.md) | Construction manuelle de DataflowRegistry, introspection DAG, exécution bas niveau | DataflowRegistry, ExecutionEngine, introspection |
| [Démo WebSocket](websocket-demo.md) | Streaming d'événements en temps réel via WebSockets | HookManager, transport WebSocket, suivi live |
| [API REST](api-example.md) | Intégration FastAPI pour gestion distante de pipelines | PipelineVisualizationAPI, endpoints personnalisés |

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
- [Pipeline Basique](quickstart.md) — Commencez ici si vous êtes novice

### Monitoring & Opérations
- [Démonstration Suivi](tracking-demo.md)
- [Pipeline Planifié](scheduled-pipeline.md)
- [Démo WebSocket](websocket-demo.md)

### Workflows Avancés
- [Pipeline Audio Dataflow](dataflow-audio-pipeline.md)
- [Découverte Registry](registry-discovery.md)

### Intégration
- [API REST](api-example.md)

---

## Prochaines Étapes

- **[Guide de Démarrage Rapide]({{ '/fr/quickstart/' | relative_url }})** — Lancez votre premier pipeline
- **[Guides Utilisateur]({{ '/fr/guides/' | relative_url }})** — Approfondissements par fonctionnalité
- **[Référence API]({{ '/fr/api/' | relative_url }})** — Documentation complète des modules
