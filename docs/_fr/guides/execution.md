---
title: Guide d'Exécution des Pipelines
nav_order: 22
---
# Guide d'Exécution des Pipelines

**Comprendre les modèles d'exécution, les modes et la gestion des résultats**

> **Version** : 0.3.2 | **S'applique à** : SequentialPipeline, DataflowPipeline, MapReduce

---

## Aperçu

Ce guide couvre l'exécution des pipelines par Taskiq-Flow, la gestion de la concurrence, la gestion des erreurs et le renvoi des résultats.

---

## 1. Modèles d'Exécution

### 1.1. Exécution Séquentielle (Pipeline Classique)

Le `Pipeline` classique exécute les étapes une après l'autre en chaîne linéaire :

```python
pipeline = Pipeline(broker).call_next(task1).call_next(task2).call_next(task3)
# Ordre d'exécution : task1 → task2 → task3 (synchroniquement)
```

**Caractéristiques**：
- Chaque étape attend que la précédente se termine
- Les résultats passent directement d'une étape à la suivante
- Ordre d'exécution prévisible et déterministe
- Adapté aux workflows linéaires

### 1.2. Exécution Parallèle (Dataflow & Map)

`DataflowPipeline` parallélise automatiquement les tâches indépendantes：

```python
@broker.task
@pipeline_task(output="features")
def extract(tracks): ...

@broker.task
@pipeline_task(output="tags")
def tag(features): ...  # Exécuté après extract

@broker.task
@pipeline_task(output="embedding")
def embed(features): ...  # Aussi après extract, parallèle à tag

pipeline = DataflowPipeline.from_tasks(broker, [extract, tag, embed])
# DAG: extract → (tag et embed en parallèle)
```

**Caractéristiques**：
- Les tâches sans dépendances non satisfaites s'exécutent concurremment
- Le DAG détermine l'ordre d'exécution
- Débit maximal pour les opérations indépendantes
- Contrôlé par le paramètre `max_parallel` sur `.map()` et `.reduce()`

### 1.3. Parallélisme Map-Reduce

L'utilitaire `MapReduce` traite explicitement les éléments en parallèle：

```python
from taskiq_flow import MapReduce

# Traiter 100 éléments avec max 10 workers concurrents
result = await MapReduce.map(
    broker,
    process_item,
    items=items_list,
    output="processed",
    max_parallel=10  # contrôle le niveau de concurrence
)
```

**Contrôle de parallélisme**：
- `max_parallel=None` → concurrence illimitée (à utiliser avec précaution)
- `max_parallel=1` → exécution séquentielle
- Recommandé : `max_parallel = nombre_de_coeurs_CPU * 2` pour les tâches liées au CPU

---

## 2. Démarrer un Pipeline

Plusieurs façons de lancer l'exécution d'un pipeline：

### 2.1. `pipeline.kiq(...)` — Fire and Forget

Retourne une `Task` immédiatement ; vous devez attendre les résultats manuellement：

```python
task = await pipeline.kiq(entrée_initiale)
# Faire d'autres choses...
result = await task.wait_result()  # bloque jusqu'à la fin
```

Utiliser quand：
- Vous avez besoin de l'ID de tâche pour des vérifications ultérieures
- Vous voulez démarrer plusieurs pipelines concurremment
- Vous construisez un système de file d'attente de tâches

### 2.2. `pipeline.kiq_dataflow(...)` — Convenance Dataflow

Identique à `kiq()` mais spécifique aux DataflowPipeline, avec une sémantique plus claire：

```python
results = await pipeline.kiq_dataflow(track_paths=["a.mp3", "b.mp3"])
# Retourne : dict mappant les noms de sortie vers les valeurs
```

### 2.3. `pipeline.kiq_map_reduce(...)` — Raccourci Map-Reduce

Combine map et reduce en un seul appel：

```python
final = await pipeline.kiq_map_reduce(
    items=items,
    map_output="processed",
    reduce_output="final"
)
```

---

## 3. Attente des Résultats

### 3.1. Attente Bloquante

```python
task = await pipeline.kiq(données)
result = await task.wait_result()  # bloque
print(result.return_value)
```

**Options**：
- `wait_result(timeout=30)` — timeout en secondes (lève `asyncio.TimeoutError`)
- `wait_result(raise_on_error=True)` — re-lance les exceptions des tâches

### 3.2. Interrogation du Statut (Polling)

```python
task = await pipeline.kiq(données)

# Vérifier périodiquement sans bloquer
while not task.is_finished:
    await asyncio.sleep(0.5)
    statut = await task.get_status()
    print(f"Statut: {statut}")
```

Utile pour les barres de progression ou applications interactives.

### 3.3. Récupération par ID de Tâche (Distribué)

Si vous n'avez que l'ID de tâche (depuis un autre processus)：

```python
from taskiq import Task
task = Task(task_id="abc123", broker=broker)
result = await task.wait_result()
```

---

## 4. Gestion des Erreurs

### 4.1. Erreurs au Niveau Tâche

Quand une tâche échoue, le pipeline :

- **S'arrête immédiatement** (par défaut) — les tâches restantes sont annulées
- **Continue** si configuré avec des politiques de gestion d'erreurs

```python
pipeline = Pipeline(broker)

# Configurer pour continuer malgré les erreurs
pipeline.on_error("continue")  # options : "stop", "continue", "retry"

# Ou utiliser une politique de retry (voir Guide Retry)
pipeline.with_retry(
    max_attempts=3,
    delay=5,
    backoff=2
)
```

### 4.2. Erreurs au Niveau Pipeline

Le pipeline entier peut échouer si：

- Une tâche critique (sans consommateurs) échoue
- Une tâche dépasse son timeout
- Le broker devient indisponible

Gérer les erreurs de pipeline avec try/except：

```python
try:
    result = await pipeline.kiq(données)
    sortie = await result.wait_result()
except TaskiqError as exc:
    print(f"Pipeline échoué: {exc}")
    # Accéder aux résultats partiels s'il y en a
    if result.is_failed:
        print(f"Échec à l'étape: {result.failed_step}")
```

### 4.3. Résultats Partiels en Cas d'Échec

Même si un pipeline échoue, vous pouvez avoir des résultats partiels des étapes complétées：

```python
result = await pipeline.kiq(données)
try:
    sortie = await result.wait_result()
except PipelineError:
    # Certaines étapes ont réussi avant l'échec
    partiel = result.partial_results  # dict des sorties complétées
    print(f"Partiel: {partiel}")
```

---

## 5. Timeouts

Définir des timeouts au niveau pipeline：

```python
pipeline = Pipeline(broker)

# Timeout global pour tout le pipeline (secondes)
pipeline.with_timeout(60)

# Ou timeout par tâche via le décorateur taskiq
@broker.task(timeout=30)
def tache_lente(): ...
```

**Comportement des timeouts**：
- Dépasser le timeout annule la tâche en cours
- `asyncio.TimeoutError` est levée
- Le statut du pipeline est défini à `ERROR`

---

## 6. Contexte d'Exécution

Chaque tâche reçoit un paramètre optionnel `context` contenant des métadonnées：

```python
from taskiq_flow import PipelineContext

@broker.task
async def ma_tache(données: str, context: PipelineContext):
    print(f"ID Pipeline: {context.pipeline_id}")
    print(f"Index d'étape: {context.step_index}")
    print(f"ID Tâche: {context.task_id}")
    return données.upper()
```

**Champs du contexte**：

| Champ | Type | Description |
|-------|------|-------------|
| `pipeline_id` | `str` | Identifiant unique de l'instance de pipeline |
| `step_index` | `int` | Index de cette étape dans la séquence |
| `task_id` | `str` | ID de la tâche taskiq sous-jacente |
| `execution_mode` | `str` | `"sequential"`, `"parallel"`, ou `"map_reduce"` |
| `started_at` | `datetime` | Horodatage de démarrage du pipeline |
| `broker` | `BaseBroker` | Référence au broker de tâches |

Activer le passage de contexte lors de la construction du pipeline：

```python
pipeline = Pipeline(broker).with_context(enable=True)
```

---

## 7. Moteurs d'Exécution Personnalisés (Avancé)

Pour un contrôle de bas niveau, utilisez `ExecutionEngine` directement：

```python
from taskiq_flow import ExecutionEngine, DAGBuilder
from taskiq_flow.dataflow import DataflowRegistry

# Construire le registry manuellement
registry = DataflowRegistry()
registry.register_task(load, output="raw", inputs=[])
registry.register_task(process, output="clean", inputs=["raw"])
registry.register_task(save, output="saved", inputs=["clean"])

# Construire le DAG
dag = registry.build_dag()

# Créer le moteur d'exécution
engine = ExecutionEngine(broker, dag)

# Exécuter avec des entrées personnalisées
résultats = await engine.execute(inputs={"source_file": "data.csv"})
print(résultats)  # {"raw": ..., "clean": ..., "saved": ...}
```

**Quand utiliser ExecutionEngine**：
- Construire des pipelines dynamiques à l'exécution
- Ordonnancement/logique personnalisée hors de l'abstraction Pipeline
- Inspecter la structure du DAG avant exécution
- Intégration avec des gestionnaires de workflow externes

---

## 8. Formes des Résultats

Les différents types de pipelines renvoient des structures de résultats différentes：

### 8.1. Résultats de Pipeline Séquentiel

```python
task = await pipeline.kiq(entrée)
result = await task.wait_result()

# result.return_value est la sortie finale après toutes les étapes
# Exemple: [3, 3, 3, 3] depuis notre pipeline quickstart
```

### 8.2. Résultats de Pipeline Dataflow

```python
result = await pipeline.kiq_dataflow(données)

# Retourne un dict mappant chaque nom de sortie vers sa valeur
{
  "features": {...},
  "tags": [...],
  "embedding": [...]
}
```

### 8.3. Résultats MapReduce

```python
mappé = await MapReduce.map(...)
print(mappé.return_value)      # Liste des résultats mappés

réduit = await MapReduce.reduce(...)
print(réduit.return_value)     # Résultat final agrégé
```

---

## 9. Inspection de l'État du Pipeline

Interroger le statut du pipeline pendant ou après exécution：

```python
from taskiq_flow import PipelineTrackingManager

tracking = PipelineTrackingManager().with_auto_storage(broker)
pipeline = Pipeline(broker).with_tracking(tracking)

task = await pipeline.kiq(données)

# Obtenir le statut détaillé
statut = await tracking.get_status(pipeline.pipeline_id)
print(f"Statut: {statut.statut}")        # EN_ATTENTE, EN_COURS, TERMINÉ, ÉCHOUÉ
print(f"Étapes: {len(statut.étapes)}")     # Nombre d'étapes complétées
print(f"Démarré: {statut.démarré_à}")
print(f"Terminé: {statut.terminé_à}")

# Obtenir l'historique étape par étape
for étape in statut.étapes:
    print(f"  {étape.nom}: {étape.statut} ({étape.durée_ms}ms)")
```

**Valeurs de statut**：

| Statut | Signification |
|--------|---------------|
| `EN_ATTENTE` | Pipeline en file, pas encore démarré |
| `EN_COURS` | En cours d'exécution |
| `TERMINÉ` | Terminé avec succès |
| `ÉCHOUÉ` | Terminé avec erreur |
| `ANNULÉ` | Annulé manuellement |

Voir [Guide de Suivi]({{ '/fr/guides/tracking.md' | relative_url }}) pour le monitoring avancé.

---

## 10. Débogage de l'Exécution

### 10.1. Activer les Logs

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Ou configurer des loggers spécifiques
logger = logging.getLogger("taskiq_flow")
logger.setLevel(logging.DEBUG)
```

### 10.2. Afficher le DAG Avant Exécution

```python
pipeline.print_dag()
# Montre les niveaux d'exécution et les dépendances
```

### 10.3. Inspecter les Arguments des Tâches

```python
@broker.task
async def tache_debug(données, context: PipelineContext):
    print(f"Reçu: {données}")
    print(f"Contexte: pipeline={context.pipeline_id}, étape={context.step_index}")
    return données
```

### 10.4. Middleware de Traçage

```python
from taskiq_flow.middleware import PipelineMiddleware

class DebugMiddleware(PipelineMiddleware):
    async def on_step_complete(self, ctx, résultat):
        print(f"Étape {ctx.task_id} complétée avec: {résultat}")
        await super().on_step_complete(ctx, résultat)

broker.add_middlewares(DebugMiddleware())
```

---

## 11. Considérations de Performance

### 11.1. Limites de Concurrence

```python
# Limiter le total des tâches parallèles globalement
from taskiq_flow.optimization.parallel import set_max_parallel_tasks
set_max_parallel_tasks(20)  # jamais plus de 20 tâches simultanées
```

### 11.2. Parallélisme Sélectif

Toutes les tâches ne bénéficient pas du parallélisme：

```python
# Tâches liées au CPU: bénéficient du parallélisme jusqu'au nombre de cœurs
# Tâches liées aux E/S: peuvent gérer un parallélisme plus élevé
# Tâches petites/rapides: le surcoût peut l'emporter sur les bénéfices

# Astuce: Profiler avec différentes valeurs max_parallel
pipeline.map(process_item, items, max_parallel=8)
```

### 11.3. Empreinte Mémoire

L'exécution parallèle charge plus de données en mémoire：

```python
# Traiter les grands jeux de données par morceaux
morceaux = diviser_en_morceaux(grande_liste, taille_morceau=100)
for morceau in morceaux:
    résultats = await pipeline.kiq_dataflow(morceau)
    # traiter les résultats avant le morceau suivant
```

Voir [Guide de Performance]({{ '/fr/guides/performance.md' | relative_url }}) pour des stratégies d'optimisation détaillées.

---

## 12. Pièges Courants

| Problème | Cause | Solution |
|---------|--------|----------|
| Tâches exécutées séquentiellement | `max_parallel=1` ou type de pipeline séquentiel | Utiliser DataflowPipeline ou augmenter le parallélisme |
| `wait_result()` reste bloqué indéfiniment | Broker non partagé, résultats perdus | Utiliser un broker persistant (Redis) avec backend de résultats |
| Tâches reçoivent de mauvaises entrées | Nommage incorrect des paramètres | S'assurer que `@pipeline_task(output=...)` correspond aux noms de paramètres en aval |
| Résultats dans le désordre | Tâches dataflow finissant à des moments différents | Le dict des résultats préserve les noms de sortie, pas l'ordre d'exécution |
| Explosion mémoire | Parallélisme illimité | Définir `max_parallel` ou traiter par lots |

---

## 13. Résumé

| Fonctionnalité | Pipeline Séquentiel | DataflowPipeline | MapReduce |
|----------------|--------------------|------------------|-----------|
| **Exécution** | Chaîne linéaire | DAG automatique | Map parallèle + reduce |
| **Parallélisme** | Aucun (sauf `.group()`) | Automatique (tâches indépendantes) | Explicitement par appel map |
| **Contrôle** | Enchaînement manuel | Dépendances déclaratives | Orienté traitement par lots |
| **Idéal pour** | Workflows linéaires simples | Workflows complexes ramifiés | Transformation de données en masse |

---

## Prochaines Étapes

- **[Guide des Pipelines]({{ '/fr/guides/pipelines.md' | relative_url }})** — Choisir entre types de pipelines et motifs
- **[Guide de Suivi]({{ '/fr/guides/tracking.md' | relative_url }})** — Surveillance du statut et historique des pipelines
- **[Guide de Performance]({{ '/fr/guides/performance.md' | relative_url }})** — Réglage pour vitesse et ressources

---

*Comprendre l'exécution est essentiel pour construire des pipelines fiables. Ensuite, apprenez sur les [Types de Pipelines]({{ '/fr/guides/pipelines.md' | relative_url }}).*
