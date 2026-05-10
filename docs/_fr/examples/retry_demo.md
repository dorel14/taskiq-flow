---
permalink: /fr/examples/retry-demo/
title: Exemple: retry_demo.py
nav_order: 48
color_scheme: dark
---
# Exemple: retry_demo.py

**Middleware retry et modes de gestion d'erreurs**

> **Version** : {VERSION} | **Fichier** : `examples/retry_demo.py`

---

## Aperçu

Cet exemple démontre les mécanismes robustes de retry et gestion d'erreurs de Taskiq-Flow v0.4.5. Il couvre :

- `PipelineRetryMiddleware` avec backoff exponentiel et jitter
- Stratégies `ErrorHandlingMode` (FAIL_FAST, CONTINUE_ON_ERROR, SKIP_FAILED, DEAD_LETTER)
- `PipelineErrorAggregator` pour collecter et analyser les échecs
- Configuration des politiques de retry par pipeline

---

## Ce Que Cet Exemple Montre

- Ajout du middleware retry à un broker
- Retry automatique avec backoff pour échecs transitoires
- Changement entre modes de gestion d'erreurs
- Agrégation des erreurs pour analyse post-mortem
- Distinction échecs retryables vs non-retryables

---

## Parcours Du Code

### 1. Middleware Retry

```python
from taskiq_flow.middlewares.retry import PipelineRetryMiddleware

retry_mw = PipelineRetryMiddleware(
    max_retries=3,
    delay=0.5,
    backoff=2.0,
    jitter=True,
)
broker.add_middlewares(retry_mw)
```

**Paramètres :**
- `max_retries`: Nombre max de tentatives (3 → 4 essais totaux)
- `delay`: Délai initial avant première retry (0.5s)
- `backoff`: Multiplicateur de délai à chaque retry (2.0 → 0.5s, 1s, 2s)
- `jitter`: Ajoute variation aléatoire pour éviter "thundering herd"

---

### 2. Démo Tâche Flaky (capricieuse)

```python
import random

@broker.task
async def flaky_task(attempt: int = 0) -> str:
    """Échoue aléatoirement, puis réussit parfois."""
    attempt += 1
    if random.random() < 0.7 and attempt < 3:
        raise RuntimeError(f"Task failed on attempt {attempt}")
    return f"Success on attempt {attempt}"
```

```python
async def demo_retry_middleware():
    pipeline = Pipeline(broker).call_next(flaky_task)
    task = await pipeline.kiq(0)
    result = await task.wait_result(timeout=10)
    print(f"Pipeline succeeded! Result: {result.return_value}")
    print(f"Retry count: {retry_mw.retry_counts}")
```

Sortie :

```
Pipeline succeeded! Result: Success on attempt 2
Retry count: {'flaky_task': 1}
```

Le middleware retente automatiquement la tâche une fois avant succès.

---

### 3. Modes de Gestion d'Erreurs

```python
from taskiq_flow.errors import ErrorHandlingMode
from taskiq_flow.execution_engine import ExecutionEngine
from taskiq_flow.dataflow.registry import DataflowRegistry

registry = DataflowRegistry()
registry.register_task(flaky_task, output="flaky_output", inputs=[])
registry.register_task(process_result, output="final", inputs=["flaky_output"])
dag = registry.build_dag()
```

#### FAIL_FAST (défaut)

```python
engine = ExecutionEngine(broker, dag, error_mode=ErrorHandlingMode.FAIL_FAST)
# Arrêt immédiat à première erreur ; pipeline échoue
```

#### CONTINUE_ON_ERROR

```python
engine = ExecutionEngine(broker, dag, error_mode=ErrorHandlingMode.CONTINUE_ON_ERROR)
# Marque tâche échouée comme FAILED mais continue avec tâches en aval qui ne dépendent pas d'elle
```

#### SKIP_FAILED

```python
engine = ExecutionEngine(broker, dag, error_mode=ErrorHandlingMode.SKIP_FAILED)
# Tâches échouées sont skipées ; tâches en aval reçoivent valeurs par défaut (None) pour intrants échoués
```

#### DEAD_LETTER

```python
engine = ExecutionEngine(broker, dag, error_mode=ErrorHandlingMode.DEAD_LETTER)
# Tâches échouées sont mises en file "dead-letter" pour retry ultérieur
```

---

### 4. Agrégation d'Erreurs

```python
from taskiq_flow.errors import PipelineErrorAggregator

aggregator = PipelineErrorAggregator()

# During/after execution, errors are collected:
aggregator.add_error(task=failed_task, error=exc, context={...})
```

Utile pour générer rapports d'erreur et alertes.

---

## Sortie Attendue

Lancer `python examples/retry_demo.py` :

```
=== Demo 1: Retry Middleware ===

Executing flaky task with retry middleware...
(Task may fail 1-2 times before succeeding)

✅ Pipeline succeeded! Result: Success on attempt 2

Retry count stored in middleware: {'flaky_task': 1}


=== Demo 2: Error Handling Modes ===

--- Mode: FAIL_FAST ---
  Execution raised: RuntimeError: Task failed on attempt 3

--- Mode: CONTINUE_ON_ERROR ---
  Execution completed. Results: ['flaky_output']

--- Mode: SKIP_FAILED ---
  Execution completed. Results: ['flaky_output']

Note: ErrorHandlingMode.DEAD_LETTER would queue failures for later retry.


=== Demo 3: Error Aggregation ===

Total errors collected: 3
Failed tasks: ['task_a', 'task_b', 'task_c']

Error details:
  - task_a: RuntimeError: timeout
  - task_b: ValueError: invalid data
  - task_c: ConnectionError: network down

You can use PipelineErrorAggregator to analyze failures and affected branches.


=== All Retry & Error Handling Demos Complete ===
```

---

## Points Clés

### Quel Mode Choisir ?

| Mode | Idéal pour | Comportement |
|------|------------|--------------|
| `FAIL_FAST` | Pipelines critiques où tout échec invalide l'ensemble | Arrêt immédiat |
| `CONTINUE_ON_ERROR` | Analyses best-effort où résultats partiels ont de la valeur | Continue ; marque échecs |
| `SKIP_FAILED` | Traitement données où intrants manquants tolérés | Fournit None par défaut |
| `DEAD_LETTER` | Systèmes nécessitant intervention manuelle ou re-jeu | File d'attente pour retry ultérieur |

### Stratégies de Retry

- **Échecs transitoires** (timeouts réseau, épuisement temporaire ressources) → Utiliser `PipelineRetryMiddleware`
- **Échecs permanents** (données invalides, bugs code) → Utiliser `FAIL_FAST` ou `SKIP_FAILED` selon tolérance
- **Chargements mixtes** → Combiner retry middleware (pour transitoires) avec modes erreur (pour permanents)

### Monitoring des Retries

Suivez compteurs retry dans métriques ou logs :

```python
for task_name, count in retry_mw.retry_counts.items():
    logger.info(f"Task {task_name} retried {count} times")
```

Intégrez avec Prometheus :

```python
from taskiq_flow.metrics import MetricsMiddleware
broker.add_middlewares(MetricsMiddleware())
```

---

## Chemin d'Apprentissage

Après cet exemple :

1. **[Guide Retry]({{ '/fr/guides/retry/' | relative_url }})** — Documentation complète retry & gestion d'erreurs
2. **[Guide Exécution]({{ '/fr/guides/execution/' | relative_url }})** — Moteur d'exécution interne
3. **[Guide Monitoring]({{ '/fr/guides/tracking/' | relative_url }})** — Suivre tâches échouées et retries en production

---

*Cet exemple montre tous les patterns de retry. En production, ajustez paramètres retry (max_retries, backoff) selon caractéristiques tâches et exigences SLA.*
