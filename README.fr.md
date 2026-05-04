# Taskiq-Flow

Taskiq-Flow vous permet d'enchaîner des fonctions intensives et de les exécuter sans attendre que chaque étape se termine. Pensez-y comme à un système d'orchestration pour des workflows de tâches asynchrones — vous définissez les étapes, et la bibliothèque gère automatiquement l'ordre d'exécution, le passage de données et le parallélisme.

*Version : 0.3.0*

> 🌐 **Documentation Internationale** : Ce projet dispose également d'une documentation en [English](README.md).

## Inspiration & Philosophie de Conception

Taskiq-Flow repose sur deux excellents projets :

### Inspiré de taskiq-pipelines
- **Enchaînement séquentiel de pipelines** — La classe d'origine `Pipeline` avec les opérations `.call_next()`, `.map()`, `.filter()`, et `.group()`.
- **Orchestration basée sur des middleware** — Le `PipelineMiddleware` qui intercepte la complétion des tâches et déclenche l'étape suivante.
- **Suivi & monitoring** — Suivi d'exécution de pipeline, gestion des états et backends de stockage.
- **Planification** — Planification de pipelines basée sur cron via `PipelineScheduler`.
- **Hooks WebSocket** — Streaming d'événements en temps réel grâce à un système de hooks.

### Inspiré de pipefunc
- **Dataflow déclaratif** — Construction automatique de DAG à partir des dépendances entre tâches via les annotations `@pipeline_task(output=...)`.
- **Résolution implicite des dépendances** — Les tâches déclarent ce qu'elles produisent ; les tâches en aval reçoivent automatiquement les entrées nécessaires par correspondance des noms de paramètres.
- **Exécution parallèle** — Les tâches indépendantes s'exécutent concurremment ; la bibliothèque gère le passage de données et la synchronisation.
- **Aides map-reduce** — Support de première classe pour le traitement parallèle et les motifs d'agrégation.

### Le Résultat
Taskiq-Flow combine l'orchestration éprouvée de taskiq-pipelines avec l'élégant modèle de programmation dataflow de pipefunc, vous offrant :
- **Pipelines séquentiels** pour des workflows linéaires simples.
- **Pipelines dataflow** pour des workflows complexes, ramifiés ou parallèles où les tâches dépendent naturellement les unes des autres.
- **Suivi, planification et monitoring unifiés** à travers les deux styles.
- **Support asynchrone complet** avec les brokers distribués de taskiq (Redis, Kafka, RabbitMQ, etc.).

## Installation

Installer depuis PyPI :

```bash
pip install taskiq-flow
```

Pour les fonctionnalités optionnelles :

```bash
# Toutes les fonctionnalités (suivi, planification, visualisation)
pip install taskiq-flow[all]

# Support Redis pour le stockage
pip install taskiq-flow[redis]

# Avec capacités de planification
pip install taskiq-flow[scheduler]
```

### Configuration de Base

Ajoutez le `PipelineMiddleware` à votre broker. Ce middleware est le moteur qui décide quoi exécuter après chaque fin d'étape.

```python
from taskiq_flow.middleware import PipelineMiddleware

broker = ...  # Votre broker (RedisStreamBroker, InMemoryBroker, etc.)
broker.add_middlewares(PipelineMiddleware())
```

**Important** : Votre broker a besoin d'un backend de résultats partagé (Redis, base de données, etc.) pour que les Workers puissent lire les résultats. Le `InMemoryBroker` fonctionne uniquement pour le développement local.

### Exemple Rapide

Voici un simple pipeline qui traite une valeur à travers plusieurs étapes :

```python
import asyncio
from taskiq import InMemoryBroker
from taskiq_flow import Pipeline, PipelineMiddleware

broker = InMemoryBroker()
broker.add_middlewares(PipelineMiddleware())

@broker.task
def add_one(value: int) -> int:
    return value + 1

@broker.task
def repeat(value: int, times: int) -> list[int]:
    return [value] * times

@broker.task
def is_positive(value: int) -> bool:
    return value >= 0

async def main():
    pipeline = (
        Pipeline(broker)
        .call_next(add_one)           # 1 → 2
        .call_next(repeat, times=4)   # 2 → [2, 2, 2, 2]
        .map(add_one)                  # [2, 2, 2, 2] → [3, 3, 3, 3]
        .filter(is_positive)           # [3, 3, 3, 3] (inchangé)
    )

    task = await pipeline.kiq(1)
    result = await task.wait_result()
    print("Résultat :", result.return_value)  # [3, 3, 3, 3]

if __name__ == "__main__":
    asyncio.run(main())
```

Deux choses à garder en tête :

1. **Toutes les fonctions du pipeline doivent être des tâches** (décorées avec `@broker.task`). Les fonctions régulières doivent être enveloppées.
2. Le `PipelineMiddleware` doit être ajouté à votre broker avant de créer des pipelines.

## Concepts Fondamentaux

### Types de Pipelines

Taskiq-pipelines offre deux approches principales :

#### 1. Pipeline Séquentiel Classique
La classe `Pipeline` d'origine où vous enchaînez manuellement les étapes dans l'ordre :

```python
pipeline = Pipeline(broker).call_next(task1).call_next(task2).map(task3)
```

Utilisez ceci lorsque vous avez un flux linéaire fixe avec des embranchements occasionnels (map/filter).

#### 2. Pipeline Dataflow (Recommandé pour les workflows complexes)

Le `DataflowPipeline` construit un graphe orienté acyclique (DAG) automatiquement à partir des dépendances entre tâches. Déclarez ce que chaque tâche produit et consomme, et la bibliothèque s'occupe du reste.

```python
from taskiq_flow import DataflowPipeline, pipeline_task

@broker.task
@pipeline_task(output="features")
def extract_features(data): ...

@broker.task
@pipeline_task(output="stats")
def compute_stats(features):  # reçoit automatiquement 'features'
    ...

pipeline = DataflowPipeline.from_tasks(
    broker,
    [extract_features, compute_stats]
)
results = await pipeline.kiq_dataflow(data=my_data)
```

C'est puissant pour le traitement audio/vidéo, les jobs ETL, ou tout workflow où les tâches dépendent naturellement des sorties les unes des autres.

## Étapes & Opérations Disponibles

La bibliothèque propose plusieurs types d'étapes et opérations que vous pouvez utiliser dans les pipelines.

### Étapes Séquentielles (`.call_next` / `.call_after`)

Exécute une tâche avec le résultat de l'étape précédente comme entrée :

```python
pipeline.call_next(process_data).call_next(save_result)
```

- `call_next(task, ...)` — passe le résultat précédent comme premier argument (ou via `param_name=`)
- `call_after(task, ...)` — exécute une tâche sans passer le résultat précédent (style fire-and-forget)

### Map & Filter (`.map` / `.filter`)

Opérations sur les résultats itérables :

```python
pipeline.map(process_item)     # Appliquer à chaque élément en parallèle
pipeline.filter(validate)       # Conserver les éléments où la tâche retourne True
```

Les deux nécessitent que le résultat précédent soit itérable.

### Étape Group (`.group`)

Exécute plusieurs tâches indépendantes en parallèle et collecte tous les résultats :

```python
pipeline.group(
    [task_a, task_b, task_c],
    param_names=["x", "y", "z"]
)
```

Toutes les tâches démarrent ensemble ; les résultats reviennent sous forme de liste dans l'ordre.

## Motif Map-Reduce

Pour le traitement par lots, utilisez les assistants map-reduce intégrés :

```python
from taskiq_flow import DataflowPipeline

pipeline = DataflowPipeline(broker)

# Traiter plusieurs éléments en parallèle
pipeline.map(
    process_single_track,
    track_list,
    output="track_features",
    max_parallel=10
)

# Agréger les résultats
pipeline.reduce(
    aggregate_features,
    input_name="track_features",
    output="stats"
)

results = await pipeline.kiq_map_reduce()
```

Ou utilisez l'utilitaire autonome `MapReduce` :

```python
from taskiq_flow import MapReduce

mapped = await MapReduce.map(
    broker, process_item, items, output="processed"
)
reduced = await MapReduce.reduce(
    broker, aggregate, mapped, output="final"
)
```

*Voir `examples/dataflow_audio_pipeline.py` (Exemple 3) pour une démonstration complète.*

## Planification de Pipelines

Planifiez l'exécution de pipelines périodiquement ou à des heures spécifiques avec des schedules de type cron :

```python
from taskiq_flow import Pipeline, PipelineScheduler

scheduler = PipelineScheduler(broker)

pipeline = Pipeline(broker).call_next(my_task)

# Exécuter toutes les minutes
job_id = await scheduler.schedule(
    pipeline,
    cron="* * * * *",
    args=("some", "data")
)

# Démarrer le scheduler (tourne en arrière-plan)
await scheduler.start()

# ... garder votre application en cours d'exécution ...

await scheduler.shutdown()
```

Autres options de planification :

- `scheduler.schedule_interval(pipeline, minutes=5)` — toutes les 5 minutes
- `scheduler.schedule_at(pipeline, run_at=datetime(...))` — exécution unique

*Exemple : `examples/scheduled_pipeline.py`*

## Visualisation de Pipelines

Inspectez votre pipeline dataflow comme un DAG :

```python
# Art ASCII dans la console
pipeline.print_dag()

# JSON pour interface web / APIs
viz_json = pipeline.visualize()

# Format DOT pour Graphviz
dot = pipeline.visualize_dot()
# Sauvegarder et rendre : dot -Tpng pipeline.dot -o pipeline.png
```

Utile pour déboguer des workflows complexes et partager la structure de pipeline avec les membres de l'équipe.

*Exemple : `examples/dataflow_audio_pipeline.py` (Exemple 4)*

## Suivi & Monitoring de Pipelines

Suivez les exécutions de pipeline en temps réel avec le `PipelineTrackingManager` :

```python
from taskiq_flow import Pipeline, PipelineTrackingManager

tracking = PipelineTrackingManager().with_auto_storage(broker)

pipeline = Pipeline(broker).with_tracking(tracking)
task = await pipeline.kiq(some_data)

# Vérifier le statut
status = await tracking.get_status(pipeline.pipeline_id)
print(f"Statut : {status.status}, Étapes : {len(status.steps)}")
```

Backends de stockage :
- `InMemoryPipelineStorage` — transitoire, pour le développement
- `RedisPipelineStorage` — persistant, multi-Workers

Vous pouvez aussi vous brancher sur les événements au niveau étape via les callbacks du gestionnaire de suivi.

## Pipelines Dataflow

Le `DataflowPipeline` construit un graphe orienté acyclique (DAG) automatiquement à partir des dépendances entre tâches. Au lieu d'enchaîner manuellement, vous déclarez ce que chaque tâche produit et consomme en utilisant le décorateur `@pipeline_task` — la bibliothèque s'occupe du reste.

### Le Décorateur `@pipeline_task`

Marquez les tâches avec leurs entrées/sorties :

```python
from taskiq_flow import pipeline_task

@broker.task
@pipeline_task(output="audio_features")
async def extract_audio(track_paths: list[str]) -> dict:
    # produit audio_features
    return {"duration": 180.0, "tempo": 120.0}

@broker.task
@pipeline_task(output="tags")
async def generate_tags(audio_features: dict) -> list[str]:
    # reçoit automatiquement audio_features comme entrée
    return ["electronic", "dance"]
```

Le pipeline détecte automatiquement que `generate_tags` dépend de `extract_audio` car la signature de fonction inclut `audio_features`.

### Construire & Exécuter un Pipeline Dataflow

```python
from taskiq_flow import DataflowPipeline

pipeline = DataflowPipeline.from_tasks(
    broker,
    [extract_audio, generate_tags, compute_embedding]
)

# Voir l'ordre d'exécution
pipeline.print_dag()

# Exécuter avec parallélisme automatique
results = await pipeline.kiq_dataflow(track_paths=["song.mp3"])
# results = {"audio_features": ..., "tags": ..., "embedding": ...}
```

Les tâches sans dépendances s'exécutent en premier. Les tâches indépendantes (comme `generate_tags` et `compute_mir_features`) s'exécutent en parallèle automatiquement.

*Exemple complet : `examples/dataflow_audio_pipeline.py`*

## Suivi via WebSocket

Taskiq-Flow supporte le suivi en temps réel de l'exécution de pipeline via WebSockets. Vous pouvez recevoir des mises à jour en direct sur le statut du pipeline et des étapes au fur et à mesure.

### Configuration du Suivi WebSocket

```python
import asyncio
from taskiq import InMemoryBroker
from taskiq_flow import Pipeline
from taskiq_flow.hooks import HookManager, setup_websocket_bridge
from taskiq_flow.integration.websocket import get_websocket_server

# Créer le broker et le gestionnaire de hooks
broker = InMemoryBroker()
hook_manager = HookManager()

# Configurer le pont WebSocket
setup_websocket_bridge(hook_manager)

# Créer le pipeline et attacher le gestionnaire de hooks
pipeline = Pipeline(broker)
pipeline.pipeline_id = "demo_pipeline"  # définir un ID connu pour les clients WebSocket
pipeline.with_hooks(hook_manager)

# Ajouter les étapes de votre pipeline...

# Démarrer le serveur WebSocket (configurer host/port selon vos besoins)
async def main():
    # Configurer l'hôte et le port du serveur
    server = get_websocket_server(host="127.0.0.1", port=8765)
    # Ou override au démarrage : server = get_websocket_server()
    # asyncio_server = await server.start_server("127.0.0.1", 8765)

    asyncio_server = await server.start_server()

    # Exécuter votre pipeline
    result = await pipeline.kiq(some_data)

    # Garder le serveur en marche
    await asyncio_server.serve_forever()

asyncio.run(main())
### Connexion des Clients WebSocket

Les clients WebSocket peuvent se connecter à `ws://127.0.0.1:8765` et s'abonner à des pipelines spécifiques en envoyant un message JSON :

```json
{"pipeline_id": "demo_pipeline"}
```

Une fois abonnés, les clients recevront des événements en temps réel comme :

```json
{
  "type": "PipelineStartEvent",
  "pipeline_id": "demo_pipeline",
  "timestamp": "2026-04-29T18:50:19+02:00"
}
```

Types d'événements disponibles :
- `PipelineStartEvent` - Exécution de pipeline démarrée
- `StepStartEvent` - Une étape de pipeline a démarré
- `StepCompleteEvent` - Une étape de pipeline s'est terminée
- `PipelineCompleteEvent` - Exécution de pipeline terminée
- `StepErrorEvent` - Une étape de pipeline a échoué
- `PipelineErrorEvent` - Exécution de pipeline a échoué

Voir `examples/websocket_demo.py` pour un exemple complet fonctionnel.

## API REST pour la Gestion de Pipelines

TaskIQ Flow inclut une API REST basée sur FastAPI pour la visualisation, la gestion et l'exécution à distance de pipelines. Ceci est utile pour construire des tableaux de bord, intégrations CI/CD, ou tout système qui doit interagir avec les pipelines via HTTP.

### Fonctionnalités

- Lister tous les pipelines enregistrés
- Visualiser la structure DAG d'un pipeline (JSON ou DOT)
- Exécuter des pipelines à distance avec paramètres
- Consulter les résultats d'exécution
- Endpoint de health check

### Démarrage Rapide

Installez FastAPI et uvicorn si ce n'est déjà fait :

```bash
pip install fastapi uvicorn[standard]
```

Ensuite créez votre serveur API :

```python
from fastapi import FastAPI
from taskiq import InMemoryBroker
from taskiq_flow import DataflowPipeline, pipeline_task, create_visualization_api

# Créer le broker et les tâches
broker = InMemoryBroker(await_inplace=True)

@broker.task
@pipeline_task(output="result")
async def process(data: str) -> dict:
    return {"processed": data.upper()}

# Construire le pipeline
pipeline = DataflowPipeline.from_tasks(broker, [process])
pipeline.pipeline_id = "my_pipeline"

# Créer l'application FastAPI avec l'API de visualisation
app = FastAPI()
viz_api = create_visualization_api(broker, app)
viz_api.add_pipeline("my_pipeline", pipeline)
```

Exécuter avec :

```bash
uvicorn my_app:app --reload --port 8000
```

### Endpoints API

Tous les endpoints sont automatiquement disponibles :

| Méthode | Endpoint | Description |
|---------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/pipelines` | Lister tous les pipelines enregistrés |
| POST | `/pipelines/{pipeline_id}` | Enregistrer un nouveau pipeline |
| GET | `/pipelines/{pipeline_id}/status` | Obtenir le statut du pipeline |
| GET | `/pipelines/{pipeline_id}/dag` | Obtenir le DAG en JSON |
| GET | `/pipelines/{pipeline_id}/dag/dot` | Obtenir le DAG en format DOT |
| GET | `/pipelines/{pipeline_id}/visualize` | Visualisation complète du pipeline |

**Note** : L'API de base se concentre sur la visualisation et la gestion. Pour l'exécution, vous pouvez ajouter des endpoints personnalisés (voir exemple ci-dessous).

### Extension de l'API

Vous pouvez ajouter des endpoints personnalisés pour exécuter des pipelines et récupérer les résultats :

```python
from fastapi import FastAPI, HTTPException
from taskiq_flow.api import PipelineVisualizationAPI

app = FastAPI()
viz_api = PipelineVisualizationAPI(broker, app)

@app.post("/pipelines/{pipeline_id}/execute")
async def execute_pipeline(pipeline_id: str, parameters: dict):
    """Exécute un pipeline avec les paramètres donnés."""
    if pipeline_id not in viz_api.pipelines:
        raise HTTPException(status_code=404, detail="Pipeline non trouvé")
    
    pipeline = viz_api.pipelines[pipeline_id]
    result = await pipeline.kiq_dataflow(**parameters)
    return {"task_id": result.task_id, "status": "started"}

@app.get("/pipelines/result/{task_id}")
async def get_result(task_id: str):
    """Récupère le résultat d'une exécution de pipeline."""
    result = await broker.get_result(task_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Résultat non trouvé")
    return {"task_id": task_id, "result": result}
```

*Exemple complet : `examples/api_example.py`*

## Scripts d'Exemple

Le répertoire `examples/` inclut :

- `quickstart.py` – pipeline basique avec étapes séquentielles, map et filter
- `tracking_demo.py` – monitoring du statut de pipeline avec le gestionnaire de suivi
- `scheduled_pipeline.py` – exécution récurrente de pipeline basée sur cron
- `dataflow_audio_pipeline.py` – DAG dataflow complet, parallélisme, map-reduce et visualisation
- `websocket_demo.py` – serveur WebSocket diffusant des événements de pipeline en direct
- `api_example.py` – API REST FastAPI pour la gestion et la visualisation de pipelines **(NOUVEAU)**

Exécuter un exemple directement : `python examples/quickstart.py`

---

## 📚 Documentation Internationale

Ce projet dispose d'une documentation multilingue :

- **[🇬🇧 English (Original)](README.md)** - Documentation originale en anglais
- **[🇫🇷 Français (Traduction)](README.fr.md)** - Version française de cette documentation

Les deux versions sont maintenues à jour. Merci de consulter la version anglaise pour les exemples de code les plus récents, car les extraits de code ne sont pas traduits.

---

## Architecture du Projet

Taskiq-Flow est conçu comme une extension de [Taskiq](https://github.com/taskiq-python/taskiq), apportant des capacités avancées d'orchestration de workflows sur fond de files de messages distribuées.

### Composants Principaux

- **Pipeline** - Exécution séquentielle avec enchaînement d'étapes
- **DataflowPipeline** - Construction automatique de DAG basée sur les dépendances
- **PipelineMiddleware** - Intergiciel qui orchestre l'exécution
- **PipelineScheduler** - Planification cron des pipelines
- **PipelineTrackingManager** - Surveillance et historique d'exécution
- **MapReduce** - Patrons de traitement parallèle
- **WebSocket Bridge** - Streaming d'événements en temps réel

La bibliothèque supporte plusieurs brokers Taskiq (Redis, Kafka, RabbitMQ) et peut s'étendre via des backends de stockage personnalisés.

---

*Documentation maintenue par SoniqueBay Team*
