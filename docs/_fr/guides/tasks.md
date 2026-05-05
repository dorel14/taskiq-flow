---
title: Guide des Tâches
nav_order: 21
---
# Guide des Tâches

**Définition des tâches, décorateurs, métadonnées et gestion des ressources**

> **Version** : 0.3.2 | **Lié** : [Guide des Pipelines]({{ '/fr/guides/pipelines.md' | relative_url }}), [Guide d'Exécution]({{ '/fr/guides/execution.md' | relative_url }})

---

## Aperçu

Les tâches sont les blocs de construction fondamentaux des pipelines Taskiq-Flow. Ce guide couvre :

- Définition des tâches avec `@broker.task`
- Le décorateur `@pipeline_task` pour les pipelines dataflow
- Métadonnées et annotations des tâches
- Profils de ressources et contraintes
- Configuration des retentatives
- Spécification des entrées/sorties

---

## 1. Qu'est-ce qu'une Tâche ?

Une **Tâche** est une fonction asynchrone qui peut être exécutée par un broker Taskiq, éventuellement avec une logique de retry, des timeouts et des métadonnées pour l'orchestration de pipeline.

### Définition Minimale d'une Tâche

```python
from taskiq import InMemoryBroker

broker = InMemoryBroker()

@broker.task
async def my_task(value: int) -> int:
    return value * 2
```

**Exigences** :
- Doit être une fonction `async def` (ou `def` normale pour les tâches synchrones)
- Doit être décorée avec `@broker.task` (ou `@broker.task(...)` avec options)
- Peut accepter n'importe quels paramètres sérialisables
- Doit retourner une valeur sérialisable en JSON

---

## 2. Décorateurs de Tâche

### 2.1. `@broker.task` — Tâche de Base

```python
@broker.task
def add(a: int, b: int) -> int:
    return a + b
```

**Options** :

```python
@broker.task(
    timeout=30,           # Secondes avant timeout de la tâche
    retry_policy=None,    # RetryPolicy personnalisée (voir Guide des Retentatives)
    max_retries=3,        # Remplacer la valeur globale par défaut
    queue="default",      # Router vers une file spécifique
    labels={"type": "cpu"} # Métadonnées labels personnalisées
)
async def slow_task():
    await asyncio.sleep(10)
    return "done"
```

### 2.2. `@pipeline_task` — Annotation Dataflow

Pour `DataflowPipeline`, utilisez `@pipeline_task(output=...)` pour déclarer ce que la tâche produit :

```python
from taskiq_flow import pipeline_task

@broker.task
@pipeline_task(output="features")
def extract(data: list[str]) -> dict:
    return {"features": compute_features(data)}

# La tâche en aval reçoit automatiquement le paramètre 'features' :
@broker.task
@pipeline_task(output="tags")
def tag(features: dict) -> list[str]:
    # 'features' est automatiquement passé depuis extract_task
    return generate_tags(features)
```

**Paramètres** :

| Paramètre | Type | Description |
|-----------|------|-------------|
| `output` | `str` | Nom de la clé de sortie (doit correspondre aux noms de paramètres en aval) |
| `outputs` | `list[str]` | Sorties multiples (pour les tâches renvoyant un tuple) |
| `inputs` | `list[str]` | Dépendances d'entrée explicites (remplace la détection automatique) |
| `description` | `str` | Description lisible de la tâche |

**Sorties multiples** :
```python
@broker.task
@pipeline_task(outputs=["features", "metadata"])
def split_output(data: str) -> tuple[dict, dict]:
    features = extract_features(data)
    metadata = extract_metadata(data)
    return features, metadata  # tuple déballé vers les deux sorties
```

### 2.3. `@pipeline_task_multi_output` — Alternative

Identique à `@pipeline_task(outputs=[...])` ; fourni pour plus de clarté :

```python
from taskiq_flow import pipeline_task_multi_output

@broker.task
@pipeline_task_multi_output(outputs=["x", "y"])
def split(value: int) -> tuple[int, int]:
    return value // 2, value % 2
```

---

## 3. Métadonnées des Tâches

Enrichissez les tâches avec des métadonnées pour la documentation, la surveillance et la découverte automatique.

### 3.1. Attributs Standard

```python
@broker.task(
    name="process_audio_track",  # Remplacer le nom auto-généré
    labels={
        "category": "audio_processing",
        "priority": "high"
    }
)
async def process_track(track_id: str) -> dict:
    return {"track": track_id, "status": "processed"}
```

### 3.2. Informations Personnalisées de Tâche

```python
from taskiq_flow import TaskInfo

task_info = TaskInfo(
    name="extract_spectrogram",
    description="Extraire le mel-spectrogramme d'un signal audio",
    parameters={
        "sample_rate": {"type": "int", "default": 22050},
        "n_mels": {"type": "int", "default": 128}
    },
    outputs=["spectrogram", "sample_rate"]
)

@broker.task
@pipeline_task(output="spectrogram", description=task_info.description)
def extract_spectrogram(audio: np.ndarray, sample_rate: int = 22050, n_mels: int = 128):
    # implémentation...
    return spectrogram
```

---

## 4. Profils de Ressources

Contrôlez l'allocation CPU et mémoire par tâche pour un ordonnancement conscient des ressources.

### 4.1. Profil CPU

```python
from taskiq_flow import CPUProfile

@broker.task
@CPUProfile(cpu_units=2)  # Requiert 2 cœurs CPU
def heavy_computation(data):
    # Cette tâche sera exécutée sur des workers avec au moins 2 cœurs
    pass
```

**Valeurs de `cpu_units`** :

| Valeur | Signification |
|--------|---------------|
| `0.5` | Half a core (tâche d'arrière-plan) |
| `1` | Un cœur complet (par défaut) |
| `2` | Deux cœurs (intensif en CPU) |

### 4.2. Profil RAM

```python
from taskiq_flow import RAMProfile

@broker.task
@RAMProfile(ram_mb=2048)  # Requiert 2 Go de RAM
def memory_intensive(data):
    # S'exécute uniquement sur les workers avec au moins 2 Go de RAM disponible
    pass
```

**Ordonnancement conscient des ressources** (nécessite un pool de workers compatible) :

```python
from taskiq_flow import ResourceAwareWorkerPool

pool = ResourceAwareWorkerPool(
    workers=[
        {"cpu_cores": 4, "ram_gb": 8},
        {"cpu_cores": 2, "ram_gb": 4},
    ]
)
# Les tâches sont routées vers les workers avec ressources suffisantes
```

### 4.3. Profils Combinés

```python
from taskiq_flow import CPUProfile, RAMProfile

@broker.task
@CPUProfile(cpu_units=4)
@RAMProfile(ram_mb=4096)
def gpu_style_task(data):
    # Tâche à hautes ressources
    pass
```

---

## 5. Spécification des Entrées/Sorties

### 5.1.Annotations de Type pour la Documentation

```python
@broker.task
async def process(
    text: str,                   # Entrée requise
    max_length: int = 100,       # Optionnel avec valeur par défaut
    *,
    strict: bool = False         # Argument mot-clé uniquement
) -> dict:
    return {"processed": text[:max_length]}
```

### 5.2. Modèles Pydantic (Recommandé pour Données Complexes)

```python
from pydantic import BaseModel

class AudioFeatures(BaseModel):
    duration: float
    tempo: float
    key: str

@broker.task
async def extract_features(audio_path: str) -> AudioFeatures:
    # Pydantic valide et sérialise automatiquement
    return AudioFeatures(duration=180.0, tempo=120.0, key="C")
```

### 5.3. Retourner Plusieurs Valeurs

Les tâches peuvent retourner n'importe quel type sérialisable en JSON :

```python
@broker.task
def split(data: str) -> tuple[str, str]:
    return data[:10], data[10:]  # Retourne deux valeurs

# Avec @pipeline_task(outputs=["first", "second"])
@pipeline_task(outputs=["head", "tail"])
def split(data):
    return data[:10], data[10:]
# Produit deux sorties : "head" et "tail"
```

---

## 6. Configuration des Retentatives

### 6.1. Retry au Niveau Tâche

```python
@broker.task(
    retry_policy={
        "max_retries": 3,
        "delay": 5.0,
        "backoff": 2.0  # Multiplicateur d'exponential backoff
    }
)
async def flaky_task():
    # Réessayera jusqu'à 3 fois avec des délais : 5s, 10s, 20s
    possibly_fails()
```

### 6.2. Retry au Niveau Pipeline

Appliquez une politique de retry à toutes les tâches d'un pipeline :

```python
pipeline = Pipeline(broker)
pipeline.with_retry(
    max_attempts=3,
    delay=2.0,         # Délai initial
    backoff=1.5,       # Multiplicateur de backoff
    on_retry=None      # Callback optionnel
)
```

Toutes les tâches de ce pipeline héritent de cette politique à moins qu'elles n'en aient une propre.

**Prévalence** : Le niveau tâche écrase le niveau pipeline.

### 6.3. Retry Conditionnel

Ne réessayez que pour des exceptions spécifiques :

```python
from taskiq.exceptions import RetryException

@broker.task
async def task_with_conditional_retry():
    try:
        call_external_api()
    except NetworkError:
        raise RetryException("Erreur réseau, réessai autorisé")
    except ValidationError:
        raise  # Échec immédiat, pas de réessai
```

Les stratégies de retry détaillées sont couvertes dans le [Guide des Retentatives]({{ '/fr/guides/retry.md' | relative_url }}).

---

## 7. Découverte & Registre des Tâches

### 7.1. Découverte Automatique

`DataflowPipeline.from_tasks()` détecte automatiquement les dépendances via les annotations de type et les décorateurs `@pipeline_task`.

### 7.2. Enregistrement Manuel

Pour des pipelines dynamiques, utilisez `DataflowRegistry` :

```python
from taskiq_flow import DataflowRegistry

registry = DataflowRegistry()

# Enregistrer avec un mapping E/S explicite
registry.register_task(
    task=process_data,
    output="processed",
    inputs=["raw"]  # dépend de la tâche qui produit "raw"
)

# Découverte depuis un module
import my_tasks
for task in my_tasks.ALL_TASKS:
    registry.register_task_from_object(task)
```

Voir `examples/registry_discovery_example.py`.

---

## 8. Écriture de Tâches Testables

Les tâches doivent être des fonctions pures pour faciliter les tests :

```python
@broker.task
def process(data: dict) -> dict:
    # Fonction pure : la sortie dépend uniquement de l'entrée
    return {"result": data["value"] * 2}

# Test unitaire
def test_process():
    assert process({"value": 5}) == {"result": 10}
```

**Test avec broker** :

```python
import pytest
from taskiq import InMemoryBroker

@pytest.fixture
def test_broker():
    return InMemoryBroker(await_inplace=True)

async def test_task_execution(test_broker):
    @test_broker.task
    async def my_task(x: int) -> int:
        return x + 1

    result = await my_task.kiq(5)
    value = await result.wait_result()
    assert value.return_value == 6
```

---

## 9. Motifs Courants

### 9.1. Idempotence

Concevez les tâches pour être ré-exécutables en toute sécurité :

```python
@broker.task
@pipeline_task(output="user_processed")
def process_user(user_id: str) -> dict:
    # Vérifie si déjà traité
    if cache.get(f"processed:{user_id}"):
        return {"status": "already_done"}
    # Exécute le traitement
    result = heavy_compute(user_id)
    cache.set(f"processed:{user_id}", result, ttl=3600)
    return result
```

### 9.2. Composabilité

Décomposez la logique complexe en petites tâches réutilisables :

```python
@broker.task
def validate(data): ...

@broker.task
def transform(data): ...

@broker.task
def enrich(data): ...

# Composition dans plusieurs pipelines
pipeline1 = Pipeline(broker).call_next(validate).call_next(transform)
pipeline2 = Pipeline(broker).call_next(validate).call_next(enrich)
```

### 9.3. Rapports de Progression

Pour les tâches longues, signalez la progression via des callbacks ou logs :

```python
@broker.task
async def long_task(items: list, progress_callback=None):
    for i, item in enumerate(items):
        result = process(item)
        if progress_callback:
            await progress_callback(i / len(items))
    return "done"
```

---

## 10. Antipatterns à Éviter

| Anti-pattern | Pourquoi c'est mauvais | Meilleure approche |
|--------------|----------------------|-------------------|
| Effets de bord dans les tâches | Rend les tests difficiles, logique obscure | Gardez les tâches pures ; utilisez `.call_after()` pour les effets de bord |
| Retours de valeurs volumineux | Mémoire élevée, sérialisation lente | Stockez les résultats volumineux en externe (DB, S3) ; retournez une référence |
| État mutable partagé | Conditions de course en parallèle | Chaque tâche indépendante ; passez les données via les retours |
| I/O bloquant sans async | Bloque la boucle d'événements | Utilisez des librairies async (aiohttp, asyncpg, etc.) |
| Tâches trop grosses | Difficile à réutiliser, tester, déboguer | Découpez en tâches plus petites et ciblées |

---

## 11. Résumé

Les tâches Taskiq-Flow sont :

- **Flexibles** — Fonctions Python classiques avec `@broker.task`
- **Observables** — Métadonnées, labels et suivi
- **Résilientes** — Politiques de retry, timeouts, gestion d'erreurs
- **Composables** — Petites fonctions combinées en workflows complexes
- **Conscientes des ressources** — Profils CPU/RAM pour un ordonnancement optimisé

---

## Prochaines Étapes

- **[Types de Pipelines]({{ '/fr/guides/pipelines.md' | relative_url }})** — Construire des workflows avec des tâches
- **[Guide d'Exécution]({{ '/fr/guides/execution.md' | relative_url }})** — Exécuter les pipelines et gérer les résultats
- **[Guide des Retentatives]({{ '/fr/guides/retry.md' | relative_url }})** — Stratégies robustes de récupération d'erreurs

---

*Les tâches sont vos atomes de workflow. Apprenez à les composer dans [Pipelines]({{ '/fr/guides/pipelines.md' | relative_url }}).*
