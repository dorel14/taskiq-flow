"""
Resource-aware optimization for pipeline execution.

Author: SoniqueBay Team
Version: 1.0.2
"""

from taskiq_flow.optimization.parallel import (
    ResourceAwareExecutor,
    TaskResourceProfile,
    get_default_executor,
)

__all__ = [
    "ResourceAwareExecutor",
    "TaskResourceProfile",
    "get_default_executor",
]
