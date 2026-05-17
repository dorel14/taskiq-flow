---
title: Guide d'Installation
nav_order: 11
---
# Guide d'Installation

**Installer Taskiq-Flow et configurer votre environnement**

---

## Prérequis

- **Python** ≥3.10
- **pip** (gestionnaire de paquets Python)
- Optionnel : **Redis** (pour le suivi/stockage distribué)

---

## Installation de base

Installez Taskiq-Flow depuis PyPI :

```bash
pip install taskiq taskiq-flow
```

C'est tout ! Vous êtes prêt à créer des pipelines.

---

## Dépendances optionnelles

Taskiq-Flow propose des fonctionnalités supplémentaires via des extras :

```bash
# Tout
pip install "taskiq-flow[all]"

# Brokers (Kafka, RabbitMQ, Redis)
pip install "taskiq-flow[brokers]"

# Planification (APScheduler + SQLAlchemy)
pip install "taskiq-flow[scheduler]"

# Types scientifiques (numpy, xarray, zarr)
pip install "taskiq-flow[scientific]"
```

| Extra | Installe |
|-------|----------|
| `all` | Toutes les fonctionnalités |
| `brokers` | `taskiq-aio-kafka`, `taskiq-aio-pika`, `taskiq-redis` |
| `scheduler` | `apscheduler[sqlalchemy]` (persistance BDD) |
| `scientific` | `numpy`, `xarray`, `zarr` |

> **Note** : `fastapi` et `uvicorn` sont inclus dans l'installation de base.

---

## Vérifier l'installation

Créez un fichier de test `test_install.py` :

```python
from taskiq import InMemoryBroker
from taskiq_flow import Pipeline, PipelineMiddleware

broker = InMemoryBroker()
broker.add_middlewares(PipelineMiddleware())

@broker.task
def hello(name: str) -> str:
    return f"Bonjour, {name}!"

async def main():
    pipeline = Pipeline(broker).call_next(hello)
    task = await pipeline.kiq("Monde")
    result = await task.wait_result()
    print(result.return_value)  # Devrait afficher : Bonjour, Monde !

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
```

Exécutez :

```bash
python test_install.py
```

Sortie attendue :
```
Bonjour, Monde !
```

---

## Prochaines étapes

Une fois installé, continuez avec :

- **[Guide de Démarrage Rapide]({{ '/fr/quickstart/' | relative_url }})** — Créez votre premier pipeline
- **[Guide des Concepts]({{ '/fr/guides/core-concepts/' | relative_url }})** — Comprenez les idées fondamentales

---

*Installation réussie ? Passez au [Guide de Démarrage Rapide]({{ '/fr/quickstart/' | relative_url }}).*
