"""
Métriques pour Taskiq-Flow.

Ce module fournit des métriques Prometheus pour le monitoring
des pipelines, tâches et performances.

Auteur: SoniqueBay Team
Version: 1.0.2
"""

from prometheus_client import Counter, Gauge, Histogram

# Métriques des pipelines
PIPELINE_EXECUTIONS_TOTAL = Counter(
    "taskiq_flow_pipeline_executions_total",
    "Nombre total d'exécutions de pipeline",
    ["pipeline_id", "status"],
)

PIPELINE_DURATION_SECONDS = Histogram(
    "taskiq_flow_pipeline_duration_seconds",
    "Durée d'exécution des pipelines",
    ["pipeline_id"],
    buckets=(0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0),
)

PIPELINE_STEPS_TOTAL = Counter(
    "taskiq_flow_pipeline_steps_total",
    "Nombre total d'étapes de pipeline exécutées",
    ["pipeline_id", "step_type"],
)

ACTIVE_PIPELINES = Gauge(
    "taskiq_flow_active_pipelines",
    "Nombre de pipelines actifs",
    ["pipeline_id"],
)

# Métriques des tâches
TASK_EXECUTIONS_TOTAL = Counter(
    "taskiq_flow_task_executions_total",
    "Nombre total d'exécutions de tâche",
    ["task_name", "status", "queue"],
)

TASK_DURATION_SECONDS = Histogram(
    "taskiq_flow_task_duration_seconds",
    "Durée d'exécution des tâches",
    ["task_name", "task_type"],
    buckets=(0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
)

TASK_RETRY_ATTEMPTS = Counter(
    "taskiq_flow_task_retry_attempts_total",
    "Nombre total de tentatives de réessai",
    ["task_name", "exception_type"],
)

TASK_RETRY_BACKOFF_SECONDS = Histogram(
    "taskiq_flow_task_retry_backoff_seconds",
    "Durée d'attente entre les réessais",
    ["task_name"],
    buckets=(1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0),
)

# Métriques de file d'attente
DLQ_QUEUE_SIZE = Gauge(
    "taskiq_flow_dlq_queue_size",
    "Taille de la file d'attente des lettres mortes",
    ["queue_name"],
)

RETRY_POLICY_VIOLATIONS = Counter(
    "taskiq_flow_retry_policy_violations_total",
    "Nombre de violations de politique de réessai",
    ["task_name", "violation_type"],
)

# Métriques de planification
SCHEDULED_JOBS_TOTAL = Counter(
    "taskiq_flow_scheduled_jobs_total",
    "Nombre total de tâches planifiées",
    ["job_name", "status"],
)

SCHEDULED_JOB_MISSES = Counter(
    "taskiq_flow_scheduled_job_misses_total",
    "Nombre de manquements de tâches planifiées",
    ["job_name"],
)

# Métriques WebSocket
WEBSOCKET_CONNECTIONS_ACTIVE = Gauge(
    "taskiq_flow_websocket_connections_active",
    "Nombre de connexions WebSocket actives",
    ["pipeline_id"],
)

WEBSOCKET_MESSAGES_TOTAL = Counter(
    "taskiq_flow_websocket_messages_total",
    "Nombre total de messages WebSocket",
    ["pipeline_id", "direction", "type"],
)

# Métriques système
WORKER_CPU_USAGE = Gauge(
    "taskiq_flow_worker_cpu_usage_percent",
    "Utilisation CPU des workers",
    ["worker_id"],
)

WORKER_MEMORY_USAGE_BYTES = Gauge(
    "taskiq_flow_worker_memory_usage_bytes",
    "Utilisation mémoire des workers",
    ["worker_id"],
)


__all__ = [
    "ACTIVE_PIPELINES",
    "DLQ_QUEUE_SIZE",
    "PIPELINE_DURATION_SECONDS",
    "PIPELINE_EXECUTIONS_TOTAL",
    "PIPELINE_STEPS_TOTAL",
    "RETRY_POLICY_VIOLATIONS",
    "SCHEDULED_JOBS_TOTAL",
    "SCHEDULED_JOB_MISSES",
    "TASK_DURATION_SECONDS",
    "TASK_EXECUTIONS_TOTAL",
    "TASK_RETRY_ATTEMPTS",
    "TASK_RETRY_BACKOFF_SECONDS",
    "WEBSOCKET_CONNECTIONS_ACTIVE",
    "WEBSOCKET_MESSAGES_TOTAL",
    "WORKER_CPU_USAGE",
    "WORKER_MEMORY_USAGE_BYTES",
]
