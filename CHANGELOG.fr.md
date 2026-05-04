# Journal des Changements

Tous les changements notables de ce projet sont documentés dans ce fichier.

Le format est basé sur [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
et ce projet adhère au [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.2]

### Ajouté

- Docstrings de module françaises complètes sur tous les modules taskiq_flow
- Amélioration des docstrings de classes et méthodes pour l'autocomplétion IDE
- Documentation améliorée de Pipeline, DataflowPipeline et Steps
- Documentation des décorateurs (@pipeline_task, @pipeline_task_multi_output)
- Documentation des composants dataflow (DAG, DAGNode, DataNode, Registry)
- Documentation du système de tracking (PipelineTrackingManager, modèles, storage)
- Documentation améliorée de ExecutionEngine
- Ajout d'exemples détaillés et sections Args/Returns partout

### Changé

- Standardisation des docstrings en français avec détails complets
- Meilleure cohérence des type hints dans les APIs publiques

## [0.3.0] - 2026-05-03

### Ajouté

- LabelBasedScheduler pour la planification légère, native TaskIQ
- PipelineTrackingManager pour le suivi d'exécution
- Backend de stockage Redis pour le suivi
- Backend de stockage PostgreSQL pour le suivi
- Backend de stockage SQLite pour le suivi
- Backend de stockage Mémoire pour le suivi
- Middleware de transport WebSocket pour les événements en temps réel
- Middleware de transport HTTP stream (stub)
- Middleware de transport Redis pub/sub (stub)
- Visualisation de DAG (JSON, DOT, NetworkX, ASCII)
- API REST pour la gestion et la visualisation de pipelines
- Map-reduce avec chunking intelligent
- Stratégies de chunking adaptatives
- Callbacks de progression
- Fonctionnalité de balayage de paramètres
- Logging structuré avec contexte de pipeline
- Système de hooks pour la gestion d'événements
- Système de retry granulitaire avec backoff exponentiel
- Modes de gestion d'erreurs (fail_fast, continue_on_error, skip_failed)
- Support des dead letter queues
- Système de middleware de pipeline
- DataCache avec injection automatique de dépendances
- Moteur d'exécution avec traitement parallèle

### Changé

- Organisation des imports améliorée
- Corrections des annotations de type dans toute la base de code
- Messages d'erreur améliorés
- Documentation mise à jour avec des exemples complets

### Corrigé

- 63 erreurs de linting ruff
- Erreurs de vérification de type dans le module scheduler
- Échecs de tests dans la factory de tracking
- Exports manquants pour PipelineTrackingManager et LabelBasedScheduler

### Déprécié

- Rien

### Supprimé

- Rien

## [0.2.0] - 2026-04-15

### Ajouté

- Version initiale
- Moteur d'exécution de pipeline
- Construction et validation de DAG
- Planification basique avec APScheduler
- Système d'événements WebSocket
- Système de suivi basique
- Opérations map-reduce
- Implémentation DataCache

### Changé

- Migration depuis taskiq-pipelines
- Amélioration de la conception de l'API
- Meilleure gestion des erreurs

### Corrigé

- Divers bugs de l'implémentation initiale

## [0.1.0] - 2026-03-01

### Ajouté

- Prototype initial
- Fonctionnalité pipeline basique
- Planification simple
- Suivi basique

[0.3.2]: https://github.com/dorel14/taskiq-flow/compare/v0.3.1...v0.3.2
[0.3.0]: https://github.com/dorel14/taskiq-flow/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/dorel14/taskiq-flow/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/dorel14/taskiq-flow/releases/tag/v0.1.0

---

> 🌐 **Documentation Internationale** : Ce projet dispose également d'une documentation en [English](CHANGELOG.md).
