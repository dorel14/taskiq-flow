---
title: Guide de sécurité
nav_order: 29
color_scheme: dark
---
# Guide de sécurité

Ce guide explique comment sécuriser votre installation TaskIQ-Flow en utilisant les fonctionnalités de sécurité intégrées : authentification, autorisation, limitation de débit et journalisation d'audit.

## Vue d'ensemble

TaskIQ-Flow fournit un système de sécurité flexible pouvant être activé via la configuration. Lorsque la sécurité est activée (`security.enabled = true`), les fonctionnalités suivantes sont actives :

- **Authentification** : Vérifie les identités des clients à l'aide de clés API ou de jetons JWT.
- **Autorisation** : Applique des listes de contrôle d'accès (ACL) aux pipelines et aux tâches.
- **Limitation de débit** : Limite le nombre de requêtes par client ou adresse IP.
- **Journalisation d'audit** : Journalise les événements liés à la sécurité pour la surveillance et la conformité.

## Configuration

Les fonctionnalités de sécurité sont configurées dans l'objet `TaskiqFlowConfig` ou via des variables d'environnement. Les principaux paramètres de sécurité sont :

```python
from taskiq_flow import TaskiqFlowConfig

config = TaskiqFlowConfig(
    security=SecurityConfig(
        enabled=True,
        api_keys={
            "admin-key": {
                "role": "admin",
                "permissions": ["read", "write", "execute"],
                "pipeline_whitelist": ["*"],
            },
            "viewer-key": {
                "role": "viewer",
                "permissions": ["read"],
                "pipeline_whitelist": ["pipeline1", "pipeline2"],
            },
        },
        jwt_secret_key="your-secret-key",
        rate_limit_per_minute=60,
        audit_log_path="audit.log",
    )
)
```

Vous pouvez également définir des variables d'environnement :

- `TASKIQ_FLOW_SECURITY_ENABLED=true`
- `TASKIQ_FLOW_SECURITY_API_KEYS` (chaîne JSON)
- `TASKIQ_FLOW_SECURITY_JWT_SECRET_KEY`
- `TASKIQ_FLOW_SECURITY_RATE_LIMIT_PER_MINUTE`
- `TASKIQ_FLOW_SECURITY_AUDIT_LOG_PATH`

## Authentification

TaskIQ-Flow prend en charge deux méthodes d'authentification :

### Authentification par clé API

Les clients doivent inclure leur clé API dans l'en-tête `X-API-Key` pour les requêtes HTTP ou dans le champ `auth` des messages de connexion WebSocket.

Exemple de requête HTTP :
```http
GET /api/pipelines
X-API-Key: admin-key
```

### Authentification JWT

Si une clé secrète JWT est configurée, les clients peuvent s'authentifier à l'aide d'un jeton Web Token (JWT) dans l'en-tête `Authorization` :

```
Authorization: Bearer <jwt-token>
```

Le JWT doit contenir un champ `sub` (sujet) identifiant l'utilisateur et éventuellement un champ `role`.

## Autorisation

Une fois authentifiés, les utilisateurs se voient attribuer un rôle et des permissions. Le système vérifie ces permissions par rapport à l'action demandée et à la ressource.

### Permissions

- `read` : Visualiser les métadonnées, l'état et les résultats des pipelines.
- `write` : Créer, mettre à jour ou supprimer des pipelines.
- `execute` : Déclencher l'exécution des pipelines.
- `admin` : Accès complet à toutes les fonctionnalités, y compris la configuration de sécurité.

### Liste blanche de pipelines

Chaque clé API peut éventuellement spécifier une liste de pipelines auxquels l'utilisateur est autorisé à accéder. Si la liste blanche est vide ou contient `"*"`, l'utilisateur peut accéder à tous les pipelines.

## Limitation de débit

Pour prévenir les abus, TaskIQ-Flow limite le nombre de requêtes par client (identifié par une clé API ou une adresse IP) par minute. La limite est configurable via `rate_limit_per_minute` (par défaut : 60).

Lorsque la limite est dépassée, le serveur répond avec le code HTTP 429 (Trop de requêtes) ou ferme la connexion WebSocket avec une violation de politique.

## Journalisation d'audit

Les événements liés à la sécurité sont enregistrés dans le journal d'audit (s'il est configuré) pour une analyse ultérieure. Les événements comprennent :

- Les succès et échecs d'authentification
- Les refus d'autorisation
- Le throttling de limitation de débit
- Les modifications de la configuration de sécurité

Le journal d'audit est un fichier texte simple contenant un objet JSON par ligne, ce qui le rend facile à analyser avec des outils comme `jq` ou à ingérer dans un système SIEM.

## Sécurité WebSocket

Les connexions WebSocket suivent le même modèle de sécurité que HTTP :

1. Lors de la négociation de mise à niveau WebSocket, le client doit s'authentifier via l'en-tête `X-API-Key` ou un JWT dans l'en-tête `Authorization`.
2. Après authentification, chaque message WebSocket (par exemple, s'abonner/se désabonner) est vérifié pour les permissions.
3. Les tentatives non autorisées entraînent un message d'erreur et la terminaison de la connexion.

## Exemple : Sécurisation d'un pipeline

Voici un exemple complet de sécurisation d'un pipeline qui traite des données sensibles :

```python
from taskiq import Taskiq, InMemoryBroker
from taskiq_flow import TaskiqFlowConfig, SecurityConfig
from taskiq_flow.api import create_api
from taskiq_flow.events import HookManager

# Configurer la sécurité
security_config = SecurityConfig(
    enabled=True,
    api_keys={
        "processor-key": {
            "role": "processor",
            "permissions": ["read", "execute"],
            "pipeline_whitelist": ["data-pipeline"],
        },
        "admin-key": {
            "role": "admin",
            "permissions": ["read", "write", "execute", "admin"],
            "pipeline_whitelist": ["*"],
        },
    },
    jwt_secret_key="super-secret",
    rate_limit_per_minute=30,
    audit_log_path="security-audit.log",
)

config = TaskiqFlowConfig(security=security_config)

# Initialiser le courtier et l'application
broker = InMemoryBroker()
taskiq = Taskiq(broker)
taskiq_app = create_api(taskiq, config=config)

# Définir un pipeline sécurisé
@taskiq.task
def process_sensitive_data(data: str) -> str:
    # Simuler le traitement
    return data.upper()

# Enregistrer le pipeline avec le courtier (optionnel, pour un déclenchement manuel)
```

Lancez l'application avec `uvicorn taskiq_app:app --host 0.0.0.0 --port 8000`. Tous les endpoints nécessiteront désormais une authentification.

## Test de la sécurité

Pour vérifier votre configuration de sécurité, essayez d'accéder à un point de terminaison sans identifiants :

```bash
curl -i http://localhost:8000/api/pipelines
# Doit retourner 401 Non autorisé

curl -i -H "X-API-Key: invalid-key" http://localhost:8000/api/pipelines
# Doit retourner 401 Non autorisé

curl -i -H "X-API-Key: processor-key" http://localhost:8000/api/pipelines
# Doit retourner 200 OK avec la liste des pipelines (si processor-key a la permission de lecture)
```

Pour tester WebSocket, utilisez une bibliothèque client WebSocket et incluez l'en-tête `X-API-Key` lors de la requête de mise à niveau.

## Conclusion

En suivant ce guide, vous pouvez sécuriser votre instance TaskIQ-Flow pour protéger les données sensibles et garantir que seuls les utilisateurs autorisés puissent effectuer des actions spécifiques. Pour plus de détails, consultez la documentation de référence de l'API.

---