---
title: Guide de Suivi et Monitoring des Pipelines
nav_order: 23
---
# Guide de Suivi et Monitoring des Pipelines

**Suivi en temps réel et historique des exécutions avec PipelineTrackingManager**

> **Version** : 0.3.2 | **Lié** : [Guide d'Exécution]({{ '/fr/guides/execution.md' | relative_url }}), [Guide WebSocket]({{ '/fr/guides/websocket.md' | relative_url }})

---

## Aperçu

Taskiq-Flow offre des capacités complètes de suivi pour monitorer les exécutions de pipeline en temps réel et historiquement. Ce guide couvre :

- `PipelineTrackingManager` — Coordonnateur central de suivi
- Backends de stockage (Mémoire, Redis)
- Requêtes de statut et historique
- Collecte de métriques
- Écoute d'événements au niveau étape

---

## 1. Démarrage Rapide

```python
from taskiq_flow import Pipeline, PipelineTrackingManager

# Initialiser le suivi avec sélection automatique du stockage
suivi = PipelineTrackingManager().with_auto_storage(broker)

# Attacher le suivi au pipeline
pipeline = Pipeline(broker).with_tracking(suivi)

# Exécuter
task = await pipeline.kiq(données)
result = await task.wait_result()

# Interroger le statut
statut = await suivi.get_status(pipeline.pipeline_id)
print(f"Statut: {statut.statut}")        # TERMINÉ
print(f"Étapes: {len(statut.étapes)}")     # Nombre d'étapes exécutées
print(f"Durée: {statut.durée_ms}ms")
```

C'est le pattern de base. Approfondissons.

---

## 2. PipelineTrackingManager

Le composant central pour enregistrer et récupérer les données d'exécution des pipelines.

### 2.1. Initialisation

```python
from taskiq_flow import PipelineTrackingManager, InMemoryPipelineStorage, RedisPipelineStorage

# Option 1: Sélection automatique selon le broker (recommandé)
suivi = PipelineTrackingManager().with_auto_storage(broker)
# Utilise Redis si le broker le supporte, sinon fallback Mémoire

# Option 2: Stockage mémoire explicite (développement uniquement)
suivi = PipelineTrackingManager().with_storage(InMemoryPipelineStorage())

# Option 3: Stockage Redis explicite (production)
suivi = PipelineTrackingManager().with_storage(
    RedisPipelineStorage(client_redis)
)

# Option 4: Backend de stockage personnalisé
suivi = PipelineTrackingManager().with_storage(MonStockagePersonnalisé())
```

### 2.2. Durée de Vie du Stockage

- **InMemoryPipelineStorage** : Vit dans le processus Python seulement ; perdu au redémarrage
- **RedisPipelineStorage** : Persistant entre processus ; survit aux redémarrages

Choisir selon le déploiement：
- Développement local → Mémoire
- Production mono-worker → Mémoire (si pas de redémarrage)
- Multi-workers / distribué → Redis (ou autre stockage partagé)

---

## 3. Modèle de Statut de Pipeline

Chaque pipeline suivi produit un objet `PipelineStatus`:

```python
from taskiq_flow.tracking.models import PipelineStatus

statut: PipelineStatus
```

**Champs**：

| Champ | Type | Description |
|-------|------|-------------|
| `pipeline_id` | `str` | Identifiant unique de l'instance de pipeline |
| `statut` | `str` | `EN_ATTENTE`, `EN_COURS`, `TERMINÉ`, `ÉCHOUÉ`, `ANNULÉ` |
| `pipeline_type` | `str` | `"sequential"` ou `"dataflow"` |
| `démarré_à` | `datetime` | Horodatage de début d'exécution |
| `terminé_à` | `datetime` | Horodatage de fin (si terminé) |
| `durée_ms` | `float` | Temps d'exécution total en millisecondes |
| `étapes` | `list[StepStatus]` | Détail par étape |
| `résultat` | `Any` | Valeur de retour finale (si terminé) |
| `erreur` | `str` | Message d'erreur (si échoué) |

**Champs StepStatus**:

| Champ | Type | Description |
|-------|------|-------------|
| `step_name` | `str` | Nom de la tâche |
| `statut` | `str` | `EN_ATTENTE`, `EN_COURS`, `TERMINÉ`, `ÉCHOUÉ` |
| `démarré_à` | `datetime` | Heure de début d'étape |
| `terminé_à` | `datetime` | Heure de fin d'étape |
| `durée_ms` | `float` | Temps d'exécution de l'étape |
| `résultat` | `Any` | Valeur de retour de l'étape |
| `erreur` | `str` | Message d'erreur si échec |

---

## 4. Interrogation des Statuts

### 4.1. Obtenir le Statut d'un Pipeline

```python
statut = await suivi.get_status(pipeline_id)

if statut.statut == "TERMINÉ":
    print(f"Pipeline terminé en {statut.durée_ms}ms")
    print(f"Résultat: {statut.résultat}")
elif statut.statut == "ÉCHOUÉ":
    print(f"Échec: {statut.erreur}")
```

### 4.2. Lister Tous les Pipelines

```python
tous_statuts = await suivi.list_pipelines()
for statut in tous_statuts:
    print(f"{statut.pipeline_id}: {statut.statut}")
```

### 4.3. Filtrer par Statut

```python
en_cours = await suivi.list_pipelines(filtre_statut="EN_COURS")
échoués = await suivi.list_pipelines(filtre_statut="ÉCHOUÉ")
terminés = await suivi.list_pipelines(filtre_statut="TERMINÉ")
```

### 4.4. Obtenir l'Historique

```python
# Obtenir les 10 derniers pipelines
historique = await suivi.get_historique(limit=10)

# Filtrer par plage de dates
from datetime import datetime, timedelta
il_y_a_semaine = datetime.now() - timedelta(days=7)
récents = await suivi.get_historique(depuis=il_y_a_semaine)
```

### 4.5. Supprimer les Anciens Enregistrements

```python
# Supprimer les enregistrements de plus de 30 jours
supprimés = await suivi.nettoyer_anciens(days=30)
print(f"Supprimé {supprimés} anciens enregistrements de pipeline")

# Supprimer un pipeline spécifique
await suivi.supprimer_pipeline(pipeline_id)
```

---

## 5. Backends de Stockage

### 5.1. InMemoryPipelineStorage

```python
from taskiq_flow.tracking import InMemoryPipelineStorage

stockage = InMemoryPipelineStorage()
suivi = PipelineTrackingManager().with_storage(stockage)

# Les données vivent uniquement dans le processus Python
# Au redémarrage, tout l'historique est perdu
# Adeéquat pour: développement, tests, scripts one-shot
```

**Avantages**：
- Zéro configuration
- Rapide (pas d'I/O réseau)
- Simple

**Inconvénients**：
- Non partageable entre workers
- Perdu au redémarrage
- Taille d'historique limitée

### 5.2. RedisPipelineStorage

```python
from taskiq_flow.tracking import RedisPipelineStorage
import redis.asyncio as redis

client_redis = redis.Redis(host="localhost", port=6379, decode_responses=True)
stockage = RedisPipelineStorage(client_redis)
suivi = PipelineTrackingManager().with_storage(stockage)
```

**Configuration**：

```python
# Avec préfixe de clé et TTL personnalisés
stockage = RedisPipelineStorage(
    client_redis,
    key_prefix="taskiq_flow:suivi:",
    ttl_secondes=604800  # rétention 7 jours
)
```

**Avantages**：
- Partagé entre multiples workers
- Persiste au redémarrage
- Évolutif
- Peut être en cluster pour haute disponibilité

**Inconvénients**：
- Requiert un serveur Redis
- Latence réseau
- Gestion TTL nécessaire (éviter croissance illimitée)

### 5.3. Stockage Personnalisé

Implémenter le protocole `TrackingStorage`:

```python
from taskiq_flow.tracking.storage import TrackingStorage
from taskiq_flow.tracking.models import PipelineStatus

class StockagePostgres(TrackingStorage):
    async def save_status(self, statut: PipelineStatus):
        # Insertion/mise à jour en PostgreSQL
        pass

    async def get_status(self, pipeline_id: str) -> PipelineStatus | None:
        # Récupération depuis la base
        pass

    async def list_pipelines(self, filtre_statut: str | None = None):
        # Requête avec filtre optionnel
        pass

    async def delete_pipeline(self, pipeline_id: str):
        # Suppression d'enregistrement
        pass

suivi = PipelineTrackingManager().with_storage(StockagePostgres())
```

---

## 6. Suivi en Temps Réel avec WebSocket

Pour des mises à jour de tableau de bord en direct, combiner `PipelineTrackingManager` avec `HookManager`:

```python
from taskiq_flow.hooks import HookManager, DiffuseurÉvénementsSuivi

gestionnaire_crochets = HookManager()
diffuseur = DiffuseurÉvénementsSuivi(suivi, gestionnaire_crochets)
suivi.ajouter_écouteur(diffuseur.on_mise_à_jour_statut)

pipeline = Pipeline(broker).with_hooks(gestionnaire_crochets).with_tracking(suivi)
```

Les événements de pipeline sont maintenant diffusés via WebSocket en temps réel.

Voir [Guide WebSocket]({{ '/fr/guides/websocket.md' | relative_url }}) pour la configuration complète。

---

## 7. Collecte de Métriques

Collecter des statistiques de performance au fil du temps:

```python
# Collecter les statistiques
stats = await suivi.get_métriques(jours=7)

print(f"Total exécutions: {stats.total_pipelines}")
print(f"Taux de succès: {stats.taux_réussite:.1%}")
print(f"Durée moyenne: {stats.durée_ms_moyenne:.0f}ms")
print(f"Raisons d'échec: {stats.raisons_échec}")
```

**Métriques courantes**：

- Débit (pipelines/minute)
- Ratio succès/échec
- Durée moyenne des étapes
- Étapes les plus longues
- Heures de pointe

Intégrer avec des systèmes de monitoring (Prometheus, Grafana):

```python
from prometheus_client import Counter, Histogram

COMPT_PIPELINES = Counter('pipelines_total', 'Total pipelines', ['statut'])
DURÉE_PIPELINE = Histogram('pipeline_duration_seconds', 'Durée d\'exécution')

class ExportateurPrometheus:
    async def on_pipeline_complete(self, statut: PipelineStatus):
        COMPT_PIPELINES.labels(statut=statut.statut).inc()
        DURÉE_PIPELINE.observe(statut.durée_ms / 1000)
```

---

## 8. Écouteurs d'Événements

Attacher des callbacks aux événements de suivi:

```python
class MonÉcouteur:
    async def on_pipeline_start(self, pipeline_id: str):
        print(f"Pipeline {pipeline_id} démarré")
        envoyer_notification_slack(f"Pipeline {pipeline_id} démarré")

    async def on_step_complete(self, pipeline_id: str, step_name: str, résultat: Any):
        journal_métrique_étape(step_name, résultat)

    async def on_pipeline_complete(self, pipeline_id: str, statut: PipelineStatus):
        if statut.statut == "ÉCHOUÉ":
            alerter_échec(pipeline_id)

écouteur = MonÉcouteur()
suivi.ajouter_écouteur(écouteur)
```

**Méthodes d'écouteur** (toutes optionnelles):

- `on_pipeline_start(pipeline_id: str)`
- `on_step_start(pipeline_id: str, step_name: str)`
- `on_step_complete(pipeline_id: str, step_name: str, résultat: Any)`
- `on_pipeline_complete(pipeline_id: str, statut: PipelineStatus)`
- `on_pipeline_error(pipeline_id: str, erreur: str)`

---

## 9. Visualisation des Données de Suivi

### 9.1. Sortie Console

```python
statut = await suivi.get_status(pipeline_id)
print(f"\n{'='*60}")
print(f"Pipeline: {statut.pipeline_id}")
print(f"Statut: {statut.statut}")
print(f"Durée: {statut.durée_ms:.0f}ms")
print(f"Étapes:")
for étape in statut.étapes:
    barre = "█" * int(étape.durée_ms / 10)
    print(f"  {étape.step_name:<30} {barre} {étape.durée_ms:.0f}ms")
```

### 9.2. Export JSON

```python
import json
statut_dict = statut.model_dump(mode="json", exclude={"résultat"})  # exclure grands résultats
print(json.dumps(statut_dict, indent=2, default=str))
```

### 9.3. Intégration avec Tableaux de Bord

Utiliser les endpoints API REST (voir [Guide API]({{ '/fr/guides/api.md' | relative_url }})) pour construire des tableaux de bord personnalisés:

```javascript
// Frontend fetch
fetch('/api/pipelines/{pipeline_id}/status')
  .then(res => res.json())
  .then(statut => {
    // Rendre graphique temporel des durées d'étapes
    // Afficher badges succès/échec
  });
```

---

## 10. Meilleures Pratiques de Production

### 10.1. Utiliser Redis en Production

Toujours utiliser `RedisPipelineStorage` en production:

```python
# config.py
URL_REDIS = os.getenv("URL_REDIS", "redis://localhost:6379")

# app.py
from redis.asyncio import Redis
client_redis = Redis.from_url(URL_REDIS)
suivi = PipelineTrackingManager().with_storage(
    RedisPipelineStorage(client_redis, ttl_secondes=2592000)  # 30 jours
)
```

### 10.2. Configurer des Politiques de Rétention

```python
# Job de nettoyage périodique (quotidien)
async def nettoyer_anciens_suivis():
    supprimés = await suivi.nettoyer_anciens(jours=7)
    print(f"Nettoyé {supprimés} anciens enregistrements de pipeline")

# Utiliser APScheduler pour exécuter quotidiennement
from taskiq_flow import PipelineScheduler
planificateur = PipelineScheduler(broker)
planificateur.schedule_at(nettoyer_anciens_suivis, run_at="0 3 * * *")  # 3h daily
```

### 10.3. Surveiller la Santé du Tracker

```python
# Health check pour systèmes de monitoring
async def santé_suivi():
    try:
        test_pipeline = Pipeline(broker).with_tracking(suivi)
        await test_pipeline.kiq("health_check")
        return {"statut": "sain"}
    except Exception as e:
        return {"statut": "non_sain", "erreur": str(e)}
```

### 10.4. Limiter la Taille de l'Historique

```python
# Garder seulement les N derniers pipelines par motif de pipeline_id
import fnmatch

motifs = ["batch_job_*", "etl_*"]
for motif in motifs:
    anciens = await suivi.list_pipelines()
    correspondants = [p for p in anciens if fnmatch.fnmatch(p.pipeline_id, motif)]
    if len(correspondants) > 100:
        for ancien_pipeline in correspondants[-100:]:
            await suivi.supprimer_pipeline(ancien_pipeline.pipeline_id)
```

---

## 11. Dépannage

### Erreur "Aucun stockage configuré"

**Symptôme** : `RuntimeError: No tracking storage configured`

**Solution** : Ajouter le stockage avant d'utiliser le suivi:

```python
suivi = PipelineTrackingManager().with_auto_storage(broker)
# ou
suivi = PipelineTrackingManager().with_storage(InMemoryPipelineStorage())
```

### Données de Suivi Manquantes

**Symptôme** : `get_status()` retourne `None` alors que le pipeline a tourné

**Causes & corrections**:

1. **Suivi non attaché**:
   ```python
   pipeline = Pipeline(broker).with_tracking(suivi)  # Doit appeler with_tracking()
   ```

2. **Brokers différents** — S'assurer du même instance `broker` entre tâche et pipeline.

3. **Durée de vie du stockage** — Le stockage mémoire est perdu au redémarrage；passer à Redis.

4. **Décalage d'ID de Pipeline** — Confirmer que `pipeline.pipeline_id` correspond à la requête.

### Dégradation des Performance avec Redis

**Symptôme** : Le suivi ralentit l'exécution du pipeline

**Correctifs**：
- Utiliser le pooling de connexions Redis
- Mettre à jour les statuts en batch (regrouper plusieurs étapes)
- Écritures batch asynchrones (comportement par défaut)
- Augmenter `maxmemory` Redis et utiliser politique d'éviction appropriée

---

## 12. Résumé

| Fonctionnalité | Mémoire | Redis |
|----------------|---------|-------|
| **Multi-processus** | ❌ Non | ✅ Oui |
| **Persistant** | ❌ Non | ✅ Oui |
| **État partagé** | ❌ Non | ✅ Oui |
| **Vitesse** | ⚡ Plus rapide | ⚡ Rapide (réseau) |
| **Configuration requise** | Aucune | Serveur Redis |

**Recette basique**:
```python
suivi = PipelineTrackingManager().with_auto_storage(broker)
pipeline = Pipeline(broker).with_tracking(suivi)
```

**Recette production**:
```python
suivi = PipelineTrackingManager().with_storage(
    RedisPipelineStorage(client_redis, ttl_secondes=604800)
)
pipeline = Pipeline(broker).with_tracking(suivi)
```

---

## Prochaines Étapes

- **[Streaming WebSocket]({{ '/fr/guides/websocket.md' | relative_url }})** — Livraison d'événements en direct pour tableaux de bord
- **[Planification]({{ '/fr/guides/scheduling.md' | relative_url }})** — Exécution périodique automatique de pipelines
- **[Performance]({{ '/fr/guides/performance.md' | relative_url }})** — Optimiser la surcharge de suivi

---

*Tout suivre. Visualiser avec [WebSocket]({{ '/fr/guides/websocket.md' | relative_url }}).*
