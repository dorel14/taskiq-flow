"""Système de hooks et d'événements pour les pipelines.

Ce module fournit un système d'événements publishes-subscribe
permettant de réagir aux différents stades d'exécution
d'un pipeline (début, fin, erreur, etc.) via des callbacks.
Inclut également un bridge WebSocket pour la diffusion en temps réel.

Auteur: SoniqueBay Team
Version: 0.3.1
"""

from taskiq_flow.hooks.bridge_picows import WebSocketHookBridge, setup_websocket_bridge
from taskiq_flow.hooks.events import (
    PipelineCompleteEvent,
    PipelineErrorEvent,
    PipelineEvent,
    PipelineStartEvent,
    StepCompleteEvent,
    StepErrorEvent,
    StepStartEvent,
)

from .manager import HookManager

__all__ = [
    "HookManager",
    "PipelineCompleteEvent",
    "PipelineErrorEvent",
    "PipelineEvent",
    "PipelineStartEvent",
    "StepCompleteEvent",
    "StepErrorEvent",
    "StepStartEvent",
    "WebSocketHookBridge",
    "setup_websocket_bridge",
]
