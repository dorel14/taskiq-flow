---
title: Guide des Pipelines
nav_order: 20
---
# Guide des Pipelines

**Motifs de pipelines séquentiels et dataflow, configurations et bonnes pratiques**

> **Version** : 0.3.2 | **Lié** : [Guide d'Exécution]({{ '/fr/guides/execution.md' | relative_url }}), [Guide des Tâches]({{ '/fr/guides/tasks.md' | relative_url }})

---

## Aperçu

Taskiq-Flow propose deux types principaux de pipelines pour orchestrer des workflows de tâches：

1. **SequentialPipeline** — Enchaînement manuel des étapes pour des workflows linéaires
2. **DataflowPipeline** — Construction automatique de DAG depuis les dépendances entre tâches

Ce guide explore les deux types, leurs cas d'usage, et comment choisir entre eux.

---

## 1. Pipeline Séquentiel

Le modèle classique où vous enchaînez explicitement les étapes dans l'ordre.

### 1.1. Structure de Base

```python
from taskiq_flow import Pipeline

pipeline = (
    Pipeline(broker)
    .call_next(task1)
    .call_next(task2)
    .call_next(task3)
)
```

**Exécution** : `task1 → task2 → task3` (synchroniquement)

### 1.2. Opérations Disponibles

#### `.call_next(task, *args, **kwargs)`

Exécute une tâche, passant le résultat précédent comme premier argument：

```python
pipeline.call_next(traiter_données).call_next(sauvegarder_résultat)
# traiter_données reçoit la sortie de l'étape précédente
# sauvegarder_résultat reçoit la sortie de traiter_données
```

**Liaison de paramètres**：
- Par position : le résultat devient le premier argument
- Par nom : `pipeline.call_next(tache, nom_param=résultat_précédent)`

Exemple：
```python
@broker.task
def multiplieur(valeur: int, facteur: int) -> int:
    return valeur * facteur

pipeline.call_next(additionner).call_next(multiplieur, facteur=3)
# sortie de additionner → multiplieur(valeur=...), facteur=3
```

#### `.call_after(task, *args, **kwargs)`

Exécute une tâche **sans** consommer le résultat précédent (fire-and-forget dans le pipeline)：

```python
pipeline.call_next(processus).call_after(journaliser)
# journaliser s'exécute après processus mais ne reçoit pas sa sortie
```

Utile pour les effets de bord (logs, notifications) qui ne devraient pas transformer le flux de données。

#### `.map(task, max_parallel=None)`

Applique une tâche à chaque élément d'un résultat itérable en parallèle：

```python
# L'étape précédente a retourné : [1, 2, 3, 4]
pipeline.map(processer_élément)
# Exécute processer_élément(1), processer_élément(2), ... concurremment
# Collecte les résultats: [traité1, traité2, ...]
```

**Options**：
- `max_parallel=10` — limiter les exécutions concurrentes
- `output_name="résultats"` — clé de sortie personnalisée (défaut : nom de sortie de tâche)

#### `.filter(task)`

Conserve les éléments où la tâche renvoie une valeur vraie：

```python
# L'étape précédente a retourné : [1, 2, 3, 4]
pipeline.filter(est_pair)
# Garde les éléments où est_pair(élément) renvoie True
# Résultat: [2, 4]
```

#### `.group(tâches, param_names=None)`

Exécute plusieurs tâches indépendantes en parallèle, à partir de la même entrée：

```python
pipeline.group(
    [tache_a, tache_b, tache_c],
    param_names=["x", "y", "z"]  # lier l'entrée à ces paramètres
)
# Toutes les trois tâches reçoivent le même résultat précédent
# Retourne : [résultat_a, résultat_b, résutlat_c]
```

---

## 2. Pipeline Dataflow

Construction automatique de DAG via annotations `@pipeline_task(output=...)`.

### 2.1. Déclaration des Sorties de Tâche

```python
from taskiq_flow import pipeline_task, DataflowPipeline

@broker.task
@pipeline_task(output="features")
def extraire_features(données: list[str]) -> dict:
    return {"count": len(données)}

@broker.task
@pipeline_task(output="stats")
def calculer_stats(features: dict) -> dict:
    return {"entries": features["count"] * 2}

@broker.task
@pipeline_task(output="report")
def générer_rapport(stats: dict) -> str:
    return f"Stats: {stats}"
```

**Clé** : Le paramètre `output` déclare ce que cette tâche produit. Les tâches en aval déclarent des noms de paramètres correspondants pour consommer ces sorties.

### 2.2. Construction du Pipeline

```python
pipeline = DataflowPipeline.from_tasks(
    broker,
    [extraire_features, calculer_stats, générer_rapport]
)
```

**Résolution automatique des dépendances**：

1. `extraire_features` produit `features` — aucune dépendance
2. `calculer_stats` a besoin de `features` — dépend de `extraire_features`
3. `générer_rapport` a besoin de `stats` — dépend de `calculer_stats`

**DAG résultant**：
```
extraire_features → calculer_stats → générer_rapport
```

### 2.3. Multiple Consommateurs

Plusieurs tâches peuvent consommer la même sortie ; elles attendront toutes le producteur：

```python
@broker.task
@pipeline_task(output="features")
def extraire(données): ...

@broker.task
@pipeline_task(output="tags")
def tagger(features: dict): ...   # consommateur 1 de features

@broker.task
@pipeline_task(output="embedding")
def embeder(features: dict): ...  # consommateur 2 de features

# tagger et embeder s'exécutent en parallèle après completion de extraire
```

### 2.4. Paramètres d'Entrée

Les pipelines dataflow acceptent des entrées externes via `kiq_dataflow(**kwargs)`：

```python
résultats = await pipeline.kiq_dataflow(data=["fichier1.mp3", "fichier2.mp3"])
# Le paramètre `data` est apparié à toute tâche en ayant besoin
# Doit correspondre à un nom de paramètre d'une tâche sans producteur (entrée externe)
```

---

## 3. Configuration du Pipeline

### 3.1. Ajout du Suivi

```python
from taskiq_flow import PipelineTrackingManager

suivi = PipelineTrackingManager().with_auto_storage(broker)
pipeline = Pipeline(broker).with_tracking(suivi)
```

Voir [Guide de Suivi]({{ '/fr/guides/tracking.md' | relative_url }}) pour plus de détails.

### 3.2. Définition d'un ID de Pipeline Personnalisé

```python
pipeline.pipeline_id = "mon_workflow_001"
# Si non défini, un UUID est généré automatiquement
```

Important pour le suivi et les abonnements WebSocket.

### 3.3. Attachement des Hooks (WebSocket)

```python
from taskiq_flow.hooks import HookManager

crochets = HookManager()
pipeline = Pipeline(broker).with_hooks(crochets)
```

Voir [Guide WebSocket]({{ '/fr/guides/websocket.md' | relative_url }}).

### 3.4. Retry & Politiques d'Erreur

```python
pipeline.with_retry(
    max_attempts=3,
    delay=1.0,
    backoff=2.0
)
pipeline.on_error("continue")  # ou "stop"
```

Voir [Guide de Retry]({{ '/fr/guides/retry.md' | relative_url }}).

### 3.5. Timeouts

```python
pipeline.with_timeout(seconds=60)
```

---

## 4. Cycle de Vie du Pipeline

### 4.1. Création → Exécution → Completion

```
1.  pipeline = Pipeline(broker)           # Créer l'objet pipeline
2.  pipeline.call_next(...)               # Enchaîner les étapes
3.  task = await pipeline.kiq(entrée)      # Lancer
4.  résultat = await task.wait_result()   # Attendre & récupérer
```

### 4.2. Réutilisabilité

Les objets Pipeline sont **à usage unique**. Pour des exécutions répétées, créez un nouveau pipeline ou utilisez `PipelineScheduler`：

```python
# Correct: Créer un pipeline frais à chaque fois
async def exécuter_workflow(données):
    pipeline = Pipeline(broker).call_next(étape1).call_next(étape2)
    return await pipeline.kiq(données)

# Pour des schedules récurrents, utiliser PipelineScheduler
from taskiq_flow import PipelineScheduler
planificateur = PipelineScheduler(broker)
await planificateur.schedule(pipeline, cron="* * * * *")
```

---

## 5. Visualisation des Pipelines

### 5.1. DAG ASCII (Console)

```python
pipeline.print_dag()
```

Exemple de sortie：
```
Ordre d'Exécution DAG:
  Niveau 0: tache_a
  Niveau 1: tache_b, tache_c
  Niveau 2: tache_d
```

### 5.2. JSON pour Interfaces Web

```python
viz = pipeline.visualize()  # retourne un dict
print(viz)
```

Structure：
```json
{
  "nodes": [
    {"id": "tache_a", "outputs": ["x", "y"]},
    {"id": "tache_b", "inputs": ["x"]}
  ],
  "edges": [{"from": "tache_a", "to": "tache_b"}]
}
```

### 5.3. Format DOT (Graphviz)

```python
dot = pipeline.visualize_dot()
with open("pipeline.dot", "w") as f:
    f.write(dot)
# Rendre: dot -Tpng pipeline.dot -o pipeline.png
```

Le diagramme résultant montre les nœuds, liens et ordre d'exécution.

---

## 6. Inspection du Pipeline (DataflowRegistry)

Pour des cas avancés, construire et inspecter manuellement le graphe de dataflow:

```python
from taskiq_flow import DataflowRegistry

registry = DataflowRegistry()

# Enregistrer les tâches avec E/S explicites
registry.register_task(
    task=charger_données,
    output="brut",
    inputs=["source"]  # entrée externe
)
registry.register_task(
    task=nettoyer,
    output="propre",
    inputs=["brut"]
)
registry.register_task(
    task=sauvegarder,
    output="sauvé",
    inputs=["propre"]
)

# Inspecter la structure
print("Tâches:", [t.nom_tâche for t in registry.get_tasks()])
print("Sorties:", registry.get_sorties())           # ["brut", "propre", "sauvé"]
print("Entrées externes:", registry.get_entrées_externes())  # ["source"]

# Trouver les dépendances
producteur = registry.get_producer("propre")   # retourne TaskNode pour 'propre'
consommateurs = registry.get_consumers("brut") # liste des tâches nécessitant 'brut'

# Construire le DAG
dag = registry.build_dag()
dag.print()
ordre = dag.topological_sort()  # liste des tâches dans l'ordre d'exécution
niveaux = dag.niveaux              # liste de listes (groupes parallèles)
```

Voir `examples/registry_discovery_example.py` pour une utilisation complète.

---

## 7. Choix entre Types de Pipeline

| Critère | SequentialPipeline | DataflowPipeline |
|---------|-------------------|------------------|
| **Forme du workflow** | Linéaire, avec embranchements occasionnels | DAG complexe avec nombreuses branches |
| **Dépendances des tâches** | Implicites (ordre d'enchaînement) | Explicites (`@pipeline_task`) |
| **Parallélisme** | Manuel (`.group()`) | Automatique (tâches indépendantes) |
| **Flexibilité** | Contrôle total de l'ordre | Déclaratif ; la bibliothèque optimise |
| **Workflows dynamiques** | Difficile (fixé au moment de la construction) | Facile (peut ajouter des tâches flexiblement) |
| **Idéal pour** | ETL étapes linéaires, batch simple | Traitement audio/vidéo, pipelines ML |

**Règle empirique**：
- **SequentialPipeline** pour des workflows simples à ordre fixe
- **DataflowPipeline** pour des workflows complexes, ramifiés ou réutilisables

---

## 8. Bonnes Pratiques

### 8.1. Nommage des Tâches et Sorties

Utiliser des noms de sortie clairs et uniques：

```python
@pipeline_task(output="user_features")  # clair
@pipeline_task(output="features_2")     # ambigu (si plusieurs features existent)
```

### 8.2. Éviter les Dépendances Circulaires

DataflowPipeline détecte les cycles et lève `CycleError` pendant `build_dag()`. Concevoir avec un flux de données avant uniquement.

### 8.3. Minimiser l'État Partagé

Chaque tâche doit être pure (la sortie dépend uniquement des entrées) pour la sécurité en parallèle.

### 8.4. Versionner les IDs de Pipeline

Inclure la version dans les IDs de pipeline pour le suivi：

```python
pipeline.pipeline_id = f"analyse_audio_v1_{int(time.time())}"
```

### 8.5. Utiliser `.call_after()` pour les Effets Secondaires

Ne pas corrompre le flux de données avec logs/métriques：

```python
pipeline.call_next(processus).call_after(journaliser_résultat)  # correct
pipeline.call_next(processus_et_journaliser)                      # anti-pattern
```

### 8.6. Limiter le Parallélisme pour les Tâches Ressource-Intensives

```python
# Transcodage intensif en CPU
pipeline.map(transcoder, fichiers, max_parallel=2)
```

### 8.7. Valider le DAG Avant Exécution

```python
pipeline.print_dag()  # Toujours inspecter les pipelines complexes
input("Appuyer sur Entrée pour exécuter...")
```

---

## 9. Pièges Courants

| Symptôme | Cause probable | Correction |
|----------|----------------|------------|
| Tâche exécutée deux fois | `.call_next()` et tâche dépendante tous deux déclarés | Supprimer l'appel redondant; Dataflow gère les dépendances |
| Sortie manquante | `@pipeline_task(output=...)` ne correspond pas au paramètre en aval | Aligner le nom de sortie avec le nom du paramètre |
| Toutes les tâches séquentielles | Utilisation de Pipeline au lieu de DataflowPipeline | Passer à DataflowPipeline pour le parallélisme automatique |
| Résultats None | Oubli de `broker.add_middlewares(PipelineMiddleware())` | Ajouter le middleware avant de créer des pipelines |
| Pipeline stale réutilisé | Tentative d'appeler `kiq()` deux fois sur le même objet pipeline | Créer un pipeline frais par exécution |

---

## 10. Motifs Avancés

### 10.1. Hybride Séquentiel + Dataflow

Combiner les deux types pour un contrôle maximal：

```python
# Coquille séquentielle
séquentiel = Pipeline(broker)

# À l'intérieur d'une étape, lancer un sous-pipeline dataflow
@broker.task
async def traiter_lot(données: list) -> dict:
    sous_pipeline = DataflowPipeline.from_tasks(
        broker,
        [sous_tache1, sous_tache2, sous_tache3]
    )
    return await sous_pipeline.kiq_dataflow(data=données)

séquentiel.call_next(traiter_lot).call_next(finaliser)
```

### 10.2. Construction de Pipeline Dynamique

Construire des pipelines à l'exécution selon la configuration：

```python
def construire_pipeline(config: dict) -> Pipeline:
    étapes = []
    if config.get("preprocess"):
        étapes.append(tâche_prétraitement)
    if config.get("analyser"):
        étapes.append(tâche_analyse)
    # ...
    pipeline = Pipeline(broker)
    for étape in étapes:
        pipeline.call_next(étape)
    return pipeline
```

### 10.3. Branchement Conditionnel

Utiliser `.filter()` et les étapes de condition：

```python
haute_valeur = pipeline.filter(est_haute_valeur)
haute_valeur.call_next(traitement_premium)
basse_valeur = pipeline.filter(est_basse_valeur)
basse_valeur.call_next(traitement_standard)

# Fusion
fusionné = haute_valeur.group([traitement_premium, traitement_standard])
```

Voir [steps/condition.py](https://github.com/SoniqueBay/taskiq-flow/blob/main/taskiq_flow/steps/condition.py) pour `IfStep`.

---

## 11. Checklist de Vérification

Avant d'exécuter un pipeline, vérifier :

- [ ] Type de pipeline choisi correctement (Séquentiel vs Dataflow)
- [ ] Toutes les fonctions décorées avec `@broker.task`
- [ ] Dataflow: toutes les tâches concernées décorées avec `@pipeline_task(output=…)`
- [ ] Les noms de sortie correspondent exactement aux noms de paramètres en aval
- [ ] `PipelineMiddleware` ajouté au broker
- [ ] `pipeline_id` défini si suivi/WebSocket nécessaire
- [ ] DAG inspecté avec `print_dag()` pour les pipelines complexes
- [ ] Limites de parallélisme (`max_parallel`) définies appropriément
- [ ] Timeouts configurés pour les tâches longues
- [ ] Exécution d'exemple réussie avant utilisation en production

---

## Lectures Complémentaires

- **[Guide d'Exécution]({{ '/fr/guides/execution.md' | relative_url }})** — Comment les pipelines s'exécutent, gestion d'erreurs, timeouts
- **[Guide des Tâches]({{ '/fr/guides/tasks.md' | relative_url }})** — Écriture des fonctions de tâche et décorateurs
- **[Exemples]({{ '/fr/examples/' | relative_url }})** — Démonstrations complètes de pipelines

---

*Maîtriser les pipelines pour orchestrer n'importe quel workflow. Ensuite, apprendre sur la [Définition des Tâches]({{ '/fr/guides/tasks.md' | relative_url }}).*
