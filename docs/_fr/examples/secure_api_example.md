---
permalink: /fr/examples/secure-api-example/
title: Exemple: secure_api_example.py
nav_order: 46
color_scheme: dark
---
# Exemple: secure_api_example.py

**Sécurité API avec authentification, limitation de débit et audit logging**

> **Version** : {VERSION} | **Fichier** : `examples/secure_api_example.py`

---

## Aperçu

Cet exemple montre comment sécuriser votre intégration FastAPI Taskiq-Flow avec les fonctionnalités de sécurité intégrées introduites en v0.4.5. Il couvre :

- Configuration de `TaskiqFlowConfig` avec paramètres de sécurité
- Authentification par clé API avec contrôle d'accès basé sur les rôles
- Activation de la limitation de débit sur les endpoints API
- Ajout d'un audit log pour la conformité
- Création d'une application FastAPI sécurisée avec support JWT

---

## Ce Que Cet Exemple Montre

- Création d'une `TaskiqFlowConfig` avec `security_enabled=True`
- Définition de clés API avec rôles et ACLs de pipelines
- Intégration de `create_visualization_api()` avec la configuration de sécurité
- Ajout d'endpoints d'audit personnalisés
- Lancement d'un serveur API sécurisé

---

## Parcours Du Code

### 1. Broker et Tâches

```python
from taskiq import InMemoryBroker
from taskiq_flow import DataflowPipeline, pipeline_task

broker = InMemoryBroker(await_inplace=True)

@broker.task
@pipeline_task(output="result")
async def process_data(data: str) -> dict:
    return {"processed": data.upper(), "status": "ok"}

@broker.task
@pipeline_task(output="validated")
async def validate_result(result: dict) -> dict:
    if result.get("status") != "ok":
        raise ValueError("Invalid result")
    return {**result, "validated": True}

pipeline = DataflowPipeline.from_tasks(broker, [process_data, validate_result])
pipeline.pipeline_id = "secure_demo_pipeline"
```

---

### 2. Configuration de Sécurité

```python
from taskiq_flow import TaskiqFlowConfig

config = TaskiqFlowConfig(
    security_enabled=True,
    auth_provider="api_key",
    api_keys={
        "sk_admin_full": {
            "role": "admin",
            "pipelines": ["*"],  # Accès à tous les pipelines
        },
        "sk_viewer_reports": {
            "role": "viewer",
            "pipelines": ["report_*"],  # Seulement les pipelines commençant par 'report_'
        },
    },
    require_https=False,  # Mettre à True en production
    rate_limit_enabled=True,
    rate_limit_default="60/minute",
)
```

**Fonctionnalités de sécurité :**

- **Authentification** : Clés API (ou JWT en alternative)
- **Autorisation** : ACLs au niveau pipeline avec motifs wildcards
- **Limitation de débit** : Limites par endpoint via slowapi
- **Forçage HTTPS** : Configurable

---

### 3. Création de l'API Sécurisée

```python
from fastapi import FastAPI
from taskiq_flow import create_visualization_api

app = FastAPI(title="Secure Taskiq-Flow API", version="{VERSION}")
viz_api = create_visualization_api(broker, app, config=config)
viz_api.add_pipeline("secure_demo_pipeline", pipeline)
```

Quand `config.security_enabled=True`, `create_visualization_api` applique automatiquement le middleware de sécurité. Tous les endpoints requièrent une authentification et respectent les limites de débit.

---

### 4. Audit Logging Personnalisé

```python
from taskiq_flow.security.audit import AuditLogger

audit_logger = AuditLogger()

@app.post("/execute-with-audit")
async def execute_with_audit(data: str, user: str = "demo_user"):
    await audit_logger.log_access(
        user={"sub": user},
        action="execute_pipeline",
        pipeline_id=pipeline.pipeline_id,
        success=True,
        details={"input_length": len(data)},
    )
    result = await pipeline.kiq_dataflow(data=data)
    return {"task_id": result.task_id, "status": "started"}
```

L'audit logging enregistre toutes les requêtes authentifiées pour conformité et monitoring.

---

### 5. Lancement du Serveur

```python
if __name__ == "__main__":
    import uvicorn
    print("API Key for admin: sk_admin_full")
    print("API Key for viewer: sk_viewer_reports")
    uvicorn.run(app, host="0.0.0.0", port=8000)
```

Testez avec :

```bash
curl -H "X-API-Key: sk_admin_full" http://localhost:8000/pipelines
```

Docs Swagger : http://localhost:8000/docs (sécurité activée ici aussi).

---

## Fonctionnalités de Sécurité Détailées

### Fournisseurs d'Authentification

| Fournisseur | Fonctionnement |
|-------------|----------------|
| `api_key` | Clés API simples via header `X-API-Key: <key>` |
| `jwt` | Authentification Bearer avec validation JWT |

Changez via `auth_provider` dans la config.

### Autorisation (ACLs)

Les ACLs de pipeline contrôlent quels rôles accèdent à quels pipelines :

```python
pipeline_acls = {
    "*": {"read": ["admin", "viewer"]},  # Tous les pipelines
    "report_*": {"write": ["admin"]},    # Seul admin peut modifier les pipelines report
}
```

Les wildcards (`*`) sont supportés dans les IDs de pipeline.

### Limitation de Débit

Utilise `slowapi` sous le capot. Configurez par endpoint ou par défaut :

```python
rate_limit_enabled=True
rate_limit_default="100/minute"
```

Limites personnalisées par route :

```python
@viz_api.router.post("/pipelines/{pipeline_id}/execute", rate_limit="10/minute")
async def execute_pipeline(...):
    ...
```

### Audit Logging

Toutes les requêtes authentifiées sont logged automatiquement. Événements d'audit personnalisés :

```python
await audit_logger.log_access(
    user=user_dict,
    action="pipeline_execute",
    pipeline_id="my_pipeline",
    success=True,
    details={"param": "value"},
)
```

---

## Sortie Attendue

Au démarrage du serveur :

```
Starting secure API server...
API Key for admin: sk_admin_full
API Key for viewer: sk_viewer_reports

Test with:
  curl -H "X-API-Key: sk_admin_full" http://localhost:8000/pipelines

Docs at: http://localhost:8000/docs
```

Sans clé API :

```json
{
  "detail": "Not authenticated"
}
```

Avec clé valide :

```json
{
  "pipelines": ["secure_demo_pipeline"]
}
```

---

## Points Clés

### Checklist Production

- [ ] Mettre `require_https=True` en production
- [ ] Utiliser des clés API fortes et aléatoires
- [ ] Stocker les clés dans variables d'environnement ou vault
- [ ] Activer l'audit logging vers fichier/base de données
- [ ] Configurer des ACLs fines par pipeline
- [ ] Définir des limites de débit appropriées par endpoint
- [ ] Utiliser l'auth JWT pour intégration OAuth2
- [ ] Périodiquement rotation des clés API

### Basculer vers JWT

```python
config = TaskiqFlowConfig(
    auth_provider="jwt",
    #pragma: allowlist nextline secret
    jwt_secret="votre-secret-super-securise-très-fort",
    jwt_algorithm="HS256",
)
```

Authentification :

```bash
curl -H "Authorization: Bearer <jwt-token>" http://localhost:8000/pipelines
```

---

## Chemin d'Apprentissage

Après cet exemple :

1. **[Guide Sécurité]({{ '/fr/guides/api/#securite-observabilite' | relative_url }})** — Fonctionnalités complètes sécurité & observabilité
2. **[Guide API]({{ '/fr/guides/api/' | relative_url }})** — Patterns d'intégration FastAPI
3. **[Sécurité WebSocket]({{ '/fr/guides/websocket/#securite-websocket' | relative_url }})** — Sécuriser les connexions temps réel

---

*Cet exemple montre des patterns de sécurité prêts pour la production. Adaptez les ACLs et limites de débit à votre cas d'usage.*
