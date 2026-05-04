"""Configuration de logging structuré pour TaskIQ-Flow.

Fournit des utilitaires pour configurer le logging avec format JSON
et contexte enrichi.

Auteur: SoniqueBay Team
Version: 0.3.2
"""

import json
import logging
import sys
from dataclasses import dataclass
from typing import Any, Literal


@dataclass
class LogConfig:
    """Configuration for structured logging."""

    level: int = logging.INFO
    format_type: str = "text"  # "text" or "json"
    include_timestamp: bool = True
    include_pipeline_context: bool = True


class StructuredFormatter(logging.Formatter):
    """Custom formatter for structured logs with pipeline context."""

    def __init__(
        self,
        fmt: str | None = None,
        datefmt: str | None = None,
        style: Literal["%", "{", "$"] = "%",
        config: LogConfig | None = None,
    ) -> None:
        """Initialize the structured formatter.

        Args:
            fmt: Format string (ignored for JSON format)
            datefmt: Date format string
            style: Format style
            config: Logging configuration
        """
        super().__init__(fmt=fmt, datefmt=datefmt, style=style)
        self.config = config or LogConfig()

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record.

        Args:
            record: Log record to format

        Returns:
            Formatted log message
        """
        # Extract pipeline context from record
        pipeline_id = getattr(record, "pipeline_id", None)
        task_name = getattr(record, "task_name", None)
        step_index = getattr(record, "step_index", None)
        duration = getattr(record, "duration", None)

        # Build base log entry
        log_entry: dict[str, Any] = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add timestamp if configured
        if self.config.include_timestamp:
            log_entry["timestamp"] = self.formatTime(record, self.datefmt)

        # Add pipeline context if configured and available
        if self.config.include_pipeline_context:
            if pipeline_id:
                log_entry["pipeline_id"] = pipeline_id
            if task_name:
                log_entry["task_name"] = task_name
            if step_index is not None:
                log_entry["step_index"] = step_index
            if duration is not None:
                log_entry["duration_ms"] = round(duration * 1000, 2)

        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        # Format based on configuration
        if self.config.format_type == "json":
            return json.dumps(log_entry)

        # Text format with pipeline context prefix
        prefix_parts = []
        if pipeline_id:
            prefix_parts.append(f"PipelineID={pipeline_id}")
        if task_name:
            prefix_parts.append(f"Task={task_name}")
        if step_index is not None:
            prefix_parts.append(f"Step={step_index}")

        prefix = "[" + "] [".join(prefix_parts) + "]" if prefix_parts else ""

        # Build text message
        time_str = (
            self.formatTime(record, self.datefmt)
            if self.config.include_timestamp
            else ""
        )
        time_prefix = f"{time_str} " if time_str else ""

        duration_str = f" ({duration * 1000:.2f}ms)" if duration else ""

        level_str = f"{record.levelname:8s}"

        message = record.getMessage()
        if record.exc_info:
            message += "\n" + self.formatException(record.exc_info)

        return f"{time_prefix}{level_str} {prefix} {message}{duration_str}"


def setup_logging(
    level: int = logging.INFO,
    format_type: str = "text",
    handlers: list[logging.Handler] | None = None,
) -> None:
    """Set up structured logging for TaskIQ-Flow.

    Args:
        level: Logging level
        format_type: "text" or "json"
        handlers: Custom handlers (if None, uses StreamHandler)
    """
    config = LogConfig(level=level, format_type=format_type)
    formatter = StructuredFormatter(config=config)

    if handlers is None:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        handlers = [handler]
    else:
        for h in handlers:
            h.setFormatter(formatter)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remove existing handlers to avoid duplicates
    for existing_handler in root_logger.handlers[:]:
        root_logger.removeHandler(existing_handler)

    # Add our handlers
    for h in handlers:
        root_logger.addHandler(h)

    # Set specific loggers
    loggers = [
        "taskiq_flow",
        "taskiq_flow.execution_engine",
        "taskiq_flow.pipeline",
        "taskiq_flow.decorators",
        "taskiq_flow.tracking",
        "taskiq_flow.map_reduce",
    ]

    for logger_name in loggers:
        logger = logging.getLogger(logger_name)
        logger.setLevel(level)
        # Remove existing handlers
        for existing_handler in logger.handlers[:]:
            logger.removeHandler(existing_handler)
        # Add our handlers
        for h in handlers:
            logger.addHandler(h)


def get_logger(name: str) -> logging.Logger:
    """Get a logger with the given name.

    Args:
        name: Logger name

    Returns:
        Configured logger instance
    """
    return logging.getLogger(f"taskiq_flow.{name}")


# Module-level logger
default_logger = get_logger(__name__)
