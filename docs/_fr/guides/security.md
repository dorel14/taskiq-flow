---
title: Guide de sécurité
nav_order: 29
color_scheme: dark
---
# Guide de sécurité

Ce guide explique comment sécuriser votre installation TaskIQ-Flow en utilisant les fonctionnalités de sécurité intégrées : authentification, autorisation, enforcement HTTPS, limitation de débit et journalisation d'audit.

## Vue d'ensemble

TaskIQ-Flow fournit un système de sécurité flexible configuré directement sur
l'objet :class:`~taskiq_flow.config.TaskiqFlowConfig`. Lorsque la sécurité
est activée (``security_enabled = True``), les fonctionnalités suivantes sont
actives :

- **Authentification** : Vérifie les identités des clients à l'aide de clés API ou de jetons JWT.
- **Autorisation** : Applique des listes de contrôle d'accès (ACL) aux pipelines via ``pipeline_acls``.
- **Enforcement HTTPS** : Bloque les requêtes HTTP non chiffrées lorsque ``require_https`` est ``True``.
- **Limitation de débit** : Limite le nombre de requêtes par client ou adresse IP, par endpoint.
- **Journalisation d'audit** : Journalise les événements liés à la sécurité pour la surveillance et la conformité.

## Configuration

Les fonctionnalités de sécurité sont configurées dans
l'objet :class:`~taskiq_flow.config.TaskiqFlowConfig` ou via des variables
d'environnement. Les principaux paramètres sont :

```python
from taskiq_flow import TaskiqFlowConfig

config = TaskiqFlowConfig(
    # ---- Activation globale ----
    security_enabled=True,
    # ---- Authentification ----
    auth_provider="api_key",           # "api_key" ou "jwt"
    api_keys={
        "admin-key": {
            "role": "admin",
            "pipelines": ["*"],
            "permissions": ["read", "write", "execute"],
        },
        "viewer-key": {
            "role": "viewer",
            "pipelines": ["pipeline1", "pipeline2"],
            "permissions": ["read"],
        },
    },
    jwt_secret="votre-cle-secrete",  # pragma: allowlist secret
    # ---- HTTPS ----
    require_https=True,
    # ---- Autorisation (ACLs) ----
    pipeline_acls={
        "pipeline1": {
            "read": ["admin", "viewer"],
            "execute": ["admin"],
        },
    },
    # ---- Rate limiting ----
    rate_limit_enabled=True,
    rate_limit_default="100/minute",
    # ---- WebSocket ----
    websocket_require_auth=True,
    websocket_max_connections=1000,
)
```

La journalisation d'audit est gérée par
:class:`~taskiq_flow.security.audit.AuditLogger`, instanciée automatiquement
par l'API (aucun champ de configuration requis).

Vous pouvez également définir des variables d'environnement :

| Variable d'environnement | Champ config | Description |
|---|---|---|
| `TASKIQ_FLOW_SECURITY_ENABLED` | `security_enabled` | Activer/désactiver la sécurité |
| `TASKIQ_FLOW_AUTH_PROVIDER` | `auth_provider` | Fournisseur : `api_key` ou `jwt` |
| `TASKIQ_FLOW_API_KEYS` | `api_keys` | Clés API au format JSON |
| `TASKIQ_FLOW_JWT_SECRET` | `jwt_secret` | Clé secrète de signature JWT |
| `TASKIQ_FLOW_REQUIRE_HTTPS` | `require_https` | Forcer HTTPS |
| `TASKIQ_FLOW_PIPELINE_ACLS` | `pipeline_acls` | ACLs des pipelines au format JSON |
| `TASKIQ_FLOW_RATE_LIMIT_ENABLED` | `rate_limit_enabled` | Activer la limitation de débit |
| `TASKIQ_FLOW_RATE_LIMIT_DEFAULT` | `rate_limit_default` | Limite par défaut (ex. `"100/minute"`) |
| `TASKIQ_FLOW_WEBSOCKET_REQUIRE_AUTH` | `websocket_require_auth` | Exiger l'authentification WebSocket |

## Authentification

TaskIQ-Flow prend en charge deux méthodes d'authentification :

### Authentification par clé API

Les clients doivent inclure leur clé API dans l'en-tête ``X-API-Key`` pour les requêtes HTTP ou dans le champ ``auth`` des messages de connexion WebSocket.

Exemple de requête HTTP :
```http
GET /api/pipelines
X-API-Key: admin-key #pragma: allowlist secret
```

### Authentification JWT

Si une clé secrète JWT est configurée (``jwt_secret``), les clients peuvent s'authentifier à l'aide d'un jeton Web Token (JWT) dans l'en-tête ``Authorization`` :

```
Authorization: Bearer <jwt-token>
```

Le JWT doit contenir un champ ``sub`` (sujet) identifiant l'utilisateur et une liste ``roles``.

## Autorisation

Une fois authentifiés, les utilisateurs se voient attribuer un rôle et des permissions. Le système vérifie ces permissions par rapport à l'action demandée et à la ressource, en s'appuyant sur :

- **ACLs par pipeline** (``pipeline_acls``) : contrôle d'accès granulaire par pipeline et par permission.
- **Liste blanche de pipelines** (``pipelines`` dans chaque clé API) : liste simple des pipelines autorisés ; ``"*"`` signifie tous les pipelines.

### Permissions

- `read` : Visualiser les métadonnées, l'état et les résultats des pipelines.
- `execute` : Déclencher l'exécution des pipelines.
- `admin` : Accès complet à toutes les fonctionnalités, y compris la configuration de sécurité.

### Liste blanche de pipelines

Chaque clé API peut éventuellement spécifier la clé ``pipelines`` contenant la liste des pipelines auxquels l'utilisateur est autorisé à accéder. Si la liste est vide ou contient ``"*"``, l'utilisateur peut accéder à tous les pipelines.

## HTTPS

Lorsque ``require_https`` est ``True`` (valeur par défaut), le
:class:`~taskiq_flow.security.https.HTTPSEnforcementMiddleware` rejette toutes
les requêtes HTTP non chiffrées avec le code 403.

Le middleware respecte l'en-tête ``X-Forwarded-Proto`` afin de fonctionner
correctement derrière un reverse proxy ou un load-balancer qui termine TLS.

## Limitation de débit

Pour prévenir les abus, TaskIQ-Flow limite le nombre de requêtes par
endpoint et par adresse IP. Les limites par défaut sont :

| Endpoint | Limite par défaut |
|---|---|
| Liste des pipelines | 60/minute |
| Récupération DAG | 120/minute |
| Chemin critique | 120/minute |
| Groupes parallèles | 120/minute |
| Exécution de pipeline | 10/minute |
| Statut | 30/minute |
| Connexion WebSocket | 5/minute |

La limite par défaut pour les endpoints non listés est ``rate_limit_default``
(par défaut : ``"100/minute"``).

Lorsque la limite est dépassée, le serveur répond avec le code HTTP 429
(Trop de requêtes).

## Journalisation d'audit

Les événements liés à la sécurité sont enregistrés par
:class:`~taskiq_flow.security.audit.AuditLogger` sous forme d'enregistrements
de logging structurés. Le nom du logger dédié est ``taskiq_flow.audit``.

Les événements comprennent :

- Les succès et échecs d'authentification
- Les refus d'autorisation
- Les événements de limitation de débit
- Les actions sur les pipelines (lecture, exécution)
- Les modifications de la configuration de sécurité

Chaque entrée contient un horodatage UTC et les champs ``extra`` structurés,
ce qui permet de l'ingérer facilement dans un système SIEM ou de l'analyser
avec des outils comme ``jq``.

## Sécurité WebSocket

Les connexions WebSocket suivent le même modèle de sécurité que HTTP :

1. Lors de la négociation de mise à niveau WebSocket, le client doit s'authentifier via l'en-tête ``X-API-Key`` ou un jeton JWT dans l'en-tête ``Authorization``.
2. Après authentification, chaque message WebSocket (abonnement, désabonnement) est vérifié par rapport aux ACLs du pipeline.
3. Les tentatives non autorisées entraînent un message d'erreur et la terminaison de la connexion.

## Exemple : Sécurisation d'une API

Voici un exemple complet utilisant l'**API actuelle** (``create_visualization_api``, champs plats sur ``TaskiqFlowConfig``) :

```python
from taskiq import Taskiq, InMemoryBroker
from taskiq_flow import TaskiqFlowConfig, create_visualization_api
from taskiq_flow.security import AuditLogger

# ── 1. Configuration de la sécurité ───────────────────────────────
config = TaskiqFlowConfig(
    security_enabled=True,
    auth_provider="api_key",
    api_keys={
        "processor-key": {
            "role": "processor",
            "pipelines": ["data-pipeline"],
            "permissions": ["read", "execute"],
        },
        "admin-key": {
            "role": "admin",
            "pipelines": ["*"],
            "permissions": ["read", "execute", "admin"],
        },
    },
    jwt_secret="super-secret",  # pragma: allowlist secret
    require_https=True,
    pipeline_acls={
        "data-pipeline": {
            "read": ["processor", "admin"],
            "execute": ["processor", "admin"],
        },
    },
    rate_limit_enabled=True,
    rate_limit_default="30/minute",
    websocket_require_auth=True,
)

# ── 2. Initialisation du courtier et de l'API ─────────────────────
broker = InMemoryBroker()
taskiq = Taskiq(broker)
app = create_visualization_api(broker)

# ── 3. Journaliseur d'audit personnalisé (optionnel) ──────────────
audit_logger = AuditLogger()
```

Lancez l'application avec ``uvicorn app:app --host 0.0.0.0 --port 8000``. Tous les endpoints nécessiteront désormais une authentification.

## Tests de sécurité

```bash
# Sans identifiants → 401 Unauthorized
curl -i http://localhost:8000/pipelines

# Clé invalide → 403 Forbidden
curl -i -H "X-API-Key: invalid-key" http://localhost:8000/pipelines

# Clé viewer valide → 200 OK
curl -i -H "X-API-Key: viewer-key" http://localhost:8000/pipelines
```

Pour tester WebSocket, utilisez une bibliothèque client WebSocket et incluez l'en-tête ``X-API-Key`` lors de la requête de mise à niveau.

## Conclusion

En suivant ce guide, vous pouvez sécuriser votre instance TaskIQ-Flow pour
protéger les données sensibles et garantir que seuls les utilisateurs autorisés
peuvent effectuer des actions spécifiques. Pour plus de détails, consultez la
documentation de référence de l'API.

---
