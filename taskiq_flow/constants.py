"""
Constantes utilisées dans tout le projet taskiq-flow.

Ce module centralise toutes les constantes de clés de labels
TaskIQ et autres valeurs fixes utilisées par le middleware
et les steps du pipeline.

Auteur: SoniqueBay Team
Version: 1.0.2
"""

from typing import Literal

# Existing constants
CURRENT_STEP = "_pipe_current_step"
PIPELINE_DATA = "_pipe_data"
EMPTY_PARAM_NAME: Literal[-1] = -1

# New constants for enhanced features
PIPELINE_ID = "_pipe_id"
STEP_RETRIES = "_step_retries"
STEP_TIMEOUT = "_step_timeout"
STEP_RETRY_DELAY = "_step_retry_delay"
