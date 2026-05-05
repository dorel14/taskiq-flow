"""Resource-aware optimization for pipeline execution.

Author: SoniqueBay Team
Version: 0.4.0
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
