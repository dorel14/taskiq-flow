---
title: Référence API
nav_order: 35
permalink: /fr/api/
color_scheme: dark
---
# Référence API

Documentation complète des modules et classes de Taskiq-Flow.

## Disponible

| Module | Description |
|--------|-------------|
| **[Composants Cœurs]({{ '/fr/api/core/' | relative_url }})** | Pipeline, DataflowPipeline, middleware, exceptions |
| **[Décorateurs]({{ '/fr/api/decorators/' | relative_url }})** | `@pipeline_task` et utilitaires |
| **[Exécution]({{ '/fr/api/execution/' | relative_url }})** | ExecutionEngine, DAG, DAGBuilder |
| **[Stockage]({{ '/fr/api/storage/' | relative_url }})** nouveau en v1.2.0 | Adaptateurs de stockage interchangeables (InMemory, Redis, SQLite), factory, StorageMiddleware |
| **[Cache]({{ '/fr/api/cache/' | relative_url }})** nouveau en v1.2.0 | Cache Dogpile (adaptateurs InMemory et Redis), CacheMiddleware |
| **[Suivi]({{ '/fr/api/tracking/' | relative_url }})** | TrackingManager et backends de stockage |
| **[Optimisation]({{ '/fr/api/optimization/' | relative_url }})** | ResourceAwareExecutor |
| **[WebSocket]({{ '/fr/api/websocket/' | relative_url }})** | HookManager et système d'événements |

---

*Pas sûr où commencer ? Voir le [Guide de Démarrage Rapide]({{ '/fr/quickstart/' | relative_url }}) ou les [Guides]({{ '/fr/guides/' | relative_url }}).*
