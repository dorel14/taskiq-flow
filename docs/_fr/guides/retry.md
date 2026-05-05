---
title: Guide des Retentatives et de la Gestion d'Erreurs
nav_order: 26
---
# Guide des Retentatives et de la Gestion d'Erreurs

**Exécution de pipeline résiliente avec politiques de retry, backoff et files de lettres mortes**

> **Version** : 0.4.0 | **Lié** : [Guide d'Exécution]({{ '/fr/guides/execution/' | relative_url }}), [Guide d'Ordonnancement]({{ '/fr/guides/scheduling/' | relative_url }})

---

## Aperçu

Les pannes sont inévitables dans les systèmes distribués. Taskiq-Flow fournit des mécanismes complets de retry et de gestion d'erreurs pour garantir la robustesse des pipelines.

Ce guide couvre :

- Politiques de retry au niveau tâche et pipeline
- Stratégies d'exponential backoff
- Dead Letter Queues (DLQ) pour les échecs irrécupérables
- Logique de retry conditionnel
- Configuration des timeouts
- Surveillance des métriques de retry

---

## 1. Comprendre les Retentatives

Une **retry** (réessai) est la ré-exécution automatique d'une tâche échouée avec les mêmes entrées. Les politiques de retry définissent **quand** et **comment** réessayer.

### Quand Retenter

✅ **Bons candidats pour le retry** :

- Timeouts réseau (API externe indisponible)
- Erreurs de connexion base de données (transitoires)
- Limitation de débit (header retry-after)
- Épuisement temporaire des ressources

❌ **Ne PAS retenter** :

- Erreurs de validation (mauvaise entrée ne se corrigera pas)
- Erreurs de programmation (bug dans le code)
- Données manquantes (ne réapparaîtront pas)
- Échecs permanents (404 Not Found, 401 Unauthorized)

---

## 2. Retry au Niveau Tâche

Configurez le retry directement sur le décorateur de tâche :

```python
@broker.task(
    max_retries=3,      # Nombre maximum de tentatives (défaut : 0 = pas de retry)
    retry_delay=5.0,    # Secondes entre les retentatives
    retry_backoff=2.0,  # Multiplice le délai après chaque tentative
    retry_timeout=60    # Timeout global incluant les retentatives
)
async def flaky_api_call():
    response = await call_external_api()
    return response.json()
```

**Séquence de retry** :

| Tentative | Délai | Cumulé |
|-----------|-------|--------|
| 1 (initiale) | 0s | 0s |
| 2 (retry 1) | 5s | 5s |
| 3 (retry 2) | 10s (5 × 2) | 15s |
| 4 (retry 3) | 20s (10 × 2) | 35s |
| Échec final | — | 35s |

---

## 3. Retry au Niveau Pipeline

Appliquez une politique de retry cohérente à toutes les tâches d'un pipeline :

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

**Priorité** : Le niveau tâche écrase le niveau pipeline.

---

## 4. Politiques de Retry Personnalisées

Pour un contrôle fin, implémentez `RetryPolicy` :

```python
from taskiq_flow import RetryPolicy

class MyRetryPolicy(RetryPolicy):
    def should_retry(self, attempt: int, exception: Exception) -> bool:
        # Retente uniquement sur erreurs réseau, max 5 tentatives
        if attempt >= 5:
            return False
        return isinstance(exception, NetworkError)

    def get_delay(self, attempt: int) -> float:
        # Backoff personnalisé : 2^attempt + jitter aléatoire
        import random
        base = 2 ** attempt
        jitter = random.uniform(-0.1, 0.1) * base
        return max(0.5, base + jitter)

pipeline.with_retry(policy=MyRetryPolicy())
```

### 4.1. Retry Conditionnel (Sur Exceptions Spécifiques)

```python
@broker.task
async def task_with_selective_retry():
    try:
        result = await call_api()
        return result
    except NetworkTimeout:
        # Cette exception doit être retentée
        raise RetryException("Timeout, réessai autorisé")
    except InvalidResponse:
        # Erreur permanente ; pas de retry
        raise  # Échec immédiat
```

**Retry basé sur les exceptions** :

```python
from taskiq.exceptions import RetryException

@broker.task(retry_on=[NetworkError, TimeoutError])
async def task():
    # Retente automatiquement sur ces types d'exceptions
    pass
```

---

## 5. Exponential Backoff avec Jitter

Évitez le problème du "thundering herd" (tous les retentements en même temps) :

```python
import random

def exponential_backoff_with_jitter(
    attempt: int,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    backoff_factor: float = 2.0,
    jitter: bool = True
) -> float:
    """Calcule le délai de retry."""
    delay = min(max_delay, base_delay * (backoff_factor ** attempt))
    if jitter:
        # Ajoute ±10% de jitter aléatoire
        delay *= random.uniform(0.9, 1.1)
    return delay

# Utilisation dans une policy
class JitteredRetryPolicy(RetryPolicy):
    def get_delay(self, attempt: int) -> float:
        return exponential_backoff_with_jitter(attempt, base_delay=2.0)
```

**Pourquoi le jitter ?** Empêche les vagues synchronisées de retentements qui submergent les services.

---

## 6. Dead Letter Queues (DLQ)

Lorsque tous les retentatifs sont épuisés, les tâches échouées doivent être stockées quelque part.

### 6.1. Configuration DLQ

```python
from taskiq_flow.middlewares.retry import RetryMiddleware

broker.add_middlewares(
    RetryMiddleware(
        max_retries=3,
        dlq_queue="failed_tasks"  # Les tâches vont ici après épuisement des retentatives
    )
)
```

**Comportement** :

1. Tâche échoue → retry 1 (après délai)
2. Échoue à nouveau → retry 2 (délai plus long)
3. Échoue à nouveau → retry 3
4. Échoue tous les retentatives → déplacement vers la file `failed_tasks`

### 6.2. Inspection & Rejeu DLQ

```python
from taskiq_flow.middlewares.retry import DLQManager

dlq = DLQManager(broker)

# Lister les tâches échouées
failed_tasks = await dlq.list_failed()
for task_info in failed_tasks:
    print(f"Tâche {task_info.task_id} échouée : {task_info.error}")

# Rejouer une tâche échouée (remettre en file d'attente)
await dlq.retry_task(task_id)

# Supprimer définitivement une tâche échouée
await dlq.delete_task(task_id)

# Suppression en masse plus ancienne que N jours
await dlq.cleanup_older_than(days=7)
```

### 6.3. Alerting DLQ

Mettez en place des alertes lorsque des tâches vont en DLQ :

```python
class DLQAlertListener:
    async def on_task_to_dlq(self, task_id: str, error: str):
        send_slack_alert(f"Tâche {task_id} échouée après retentatives : {error}")
        create_incident_ticket(task_id, error)

dlq_manager = DLQManager(broker).with_listener(DLQAlertListener())
```

---

## 7. Timeouts

Évitez que les tâches ne s'exécutent indéfiniment.

### 7.1. Timeout au Niveau Tâche

```python
@broker.task(timeout=30)  # secondes
async def potentially_slow_task():
    await long_running_operation()
```

Si la tâche dépasse 30 secondes, une `asyncio.TimeoutError` est levée et la politique de retry s'applique.

### 7.2. Timeout au Niveau Pipeline

```python
pipeline = Pipeline(broker)
pipeline.with_timeout(seconds=300)  # 5 minutes pour l'ensemble du pipeline
```

Annule toutes les étapes en cours lorsque le timeout expire.

### 7.3. Timeout au Niveau Étape (Avancé)

```python
from taskiq_flow.steps import TimeoutStep

pipeline = Pipeline(broker)
pipeline.call_next(TimeoutStep(my_task, timeout=10.0))
```

---

## 8. Propagation des Erreurs

### 8.1. Échec Rapide (Par Défaut)

Le pipeline s'arrête à la première erreur :

```python
pipeline = Pipeline(broker)
# Par défaut : on_error="stop"

pipeline.call_next(task1)  # Échoue → le pipeline s'arrête, task2 ne s'exécute jamais
pipeline.call_next(task2)
```

### 8.2. Continuer en Cas d'Erreur

Continue d'exécuter les étapes restantes malgré les échecs :

```python
pipeline = Pipeline(broker)
pipeline.on_error("continue")

pipeline.call_next(task1)  # Échoue, mais task2 s'exécute quand même
pipeline.call_next(task2)
```

**Résultat** : Task2 reçoit `None` ou un résultat partiel ; vérifiez `result.is_failed`.

### 8.3. Compensation (Pattern Saga)

Exécute une tâche de nettoyage si une étape échoue :

```python
pipeline = Pipeline(broker)

pipeline.call_next(allocate_resource)
    .on_failure(compensate_allocation)  # Exécute la compensation si l'étape précédente a échoué
pipeline.call_next(process)
```

---

## 9. Surveillance des Retentatives

Suivez les métriques de retry :

```python
from taskiq_flow import PipelineTrackingManager

tracking = PipelineTrackingManager().with_auto_storage(broker)

# Métriques de retry exposées dans PipelineStatus:
status = await tracking.get_status(pipeline_id)
print(f"Étapes : {len(status.steps)}")
for step in status.steps:
    if step.retry_count > 0:
        print(f"  {step.name} : retenté {step.retry_count} fois")
        print(f"    Erreurs : {step.errors}")
```

**Métriques à surveiller** :

- **Taux de retry** (%) de tâches nécessitant un retry
- **Nombre moyen de retentatives** par tâche
- **Top des tâches échouantes** (plus de retentatives)
- **Taille de la DLQ** (tâches abandonnées)
- **Temps passé en retry** vs travail réel

### Intégration avec Prometheus

```python
from prometheus_client import Counter, Summary

RETRY_COUNT = Counter('task_retries_total', 'Total des tentatives de retry', ['task_name'])
TASK_FAILURES = Counter('task_failures_total', 'Tâches ayant échoué après retentatives', ['task_name'])
TASK_DURATION = Summary('task_duration_seconds', 'Temps d\'exécution des tâches', ['task_name'])

class MetricsMiddleware(PipelineMiddleware):
    async def on_step_complete(self, ctx, result):
        step_name = ctx.task_name
        RETRY_COUNT.labels(step_name).inc(ctx.retry_count)
        TASK_DURATION.labels(step_name).observe(ctx.duration_ms / 1000)
```

---

## 10. Bonnes Pratiques

### 10.1. Définir des Limites de Retry Raisonnables

```python
# Ne pas retenter indéfiniment
@broker.task(max_retries=3)  # Bon : borné
@broker.task(max_retries=None)  # Mauvais : retentatives infinies
```

### 10.2. Utiliser l'Exponential Backoff

Implémenté via `retry_backoff` :

```python
@broker.task(max_retries=5, retry_delay=2.0, retry_backoff=2.0)
# Délais : 2s, 4s, 8s, 16s, 32s
```

### 10.3. Ajouter du Jitter

Randomisez les délais pour éviter le "thundering herd" :

```python
retry_backoff=2.0, retry_jitter=True  # Ajoute ±10% de jitter
```

### 10.4. Fixer des Délais Max

```python
# Timeout global incluant les retentatives
@broker.task(retry_timeout=300)  # Abandon après 5 minutes totales
```

### 10.5. Logger Chaque Retry

```python
import logging
logger = logging.getLogger(__name__)

@broker.task(
    max_retries=3,
    on_retry=lambda attempt, exc: logger.warning(f"Retry {attempt} pour la tâche : {exc}")
)
```

### 10.6. Séparer Erreurs Transitoires vs Permanentes

```python
@broker.task
async def smart_task():
    try:
        return await call_api()
    except (Timeout, ConnectionError) as e:
        raise RetryException("Erreur transitoire") from e  # Sera retentée
    except NotFoundError:
        raise  # Pas de retry, échec permanent
```

### 10.7. DLQ pour Investigation

Ne jetez jamais les tâches échouées sans revue :

```python
dlq = DLQManager(broker)
# Examiner périodiquement la DLQ
failed = await dlq.list_failed(limit=100)
for task in failed:
    logger.error(f"Tâche DLQ {task.task_id} : {task.error}")
    # Penser à rejouer manuellement ou corriger les données
```

---

## 11. Pièges Courants

| Piège | Conséquence | Solution |
|-------|-------------|----------|
| Retentatives infinies (`max_retries=None`) | Système bloqué en boucle de retry | Fixer une limite explicite |
| Pas de backoff (delay=0) | Service submergé | Utiliser exponential backoff |
| Retenter sur erreurs de validation | Ressources gaspillées | Distinguer les types d'erreur |
| Pas de DLQ | Tâches échouées perdues | Configurer la DLQ |
| Timeout plus court que délai de retry | Timeout prématuré | S'assurer que timeout > somme des délais de retry |
| Multiples retentatives sur tâches non-idempotentes | Effets de bord en double | Rendre les tâches idempotentes ou limiter retry |

---

## 12. Résumé

| Fonctionnalité | Niveau Tâche | Niveau Pipeline |
|----------------|--------------|-----------------|
| **Limite de retry** | `@broker.task(max_retries=N)` | `pipeline.with_retry(max_attempts=N)` |
| **Délai** | `retry_delay` | `delay` |
| **Backoff** | `retry_backoff` | `backoff` |
| **Timeout** | `timeout` par tâche | `with_timeout(seconds)` global |
| **DLQ** | Via `RetryMiddleware` | Hérité des tâches |

**Pipeline résilient complet** :

```python
tracking = PipelineTrackingManager().with_auto_storage(broker)

pipeline = Pipeline(broker).with_tracking(tracking)
pipeline.with_retry(max_attempts=3, delay=2.0, backoff=2.0)
pipeline.with_timeout(seconds=300)
pipeline.on_error("continue")  # Ou utiliser des étapes de compensation

# Ajouter middleware de retry avec DLQ
from taskiq_flow.middlewares.retry import RetryMiddleware
broker.add_middlewares(RetryMiddleware(max_retries=3, dlq_queue="failed_tasks"))
```

---

## Prochaines Étapes

- **[Guide de Performance]({{ '/fr/guides/performance/' | relative_url }})** — Optimiser l'exécution et l'usage des ressources
- **[Guide d'Ordonnancement]({{ '/fr/guides/scheduling/' | relative_url }})** — Ordonnancement automatique des pipelines
- **[Guide de Suivi]({{ '/fr/guides/tracking/' | relative_url }})** — Surveiller les métriques de retry en production

---

*Les pannes arrivent. Réessayez intelligemment. Tout suivez.*
