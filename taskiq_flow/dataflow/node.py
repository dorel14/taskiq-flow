"""DataNode class for representing data artifacts in the pipeline."""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class DataNode:
    """
    Represents a data artifact produced/consumed by tasks.

    Attributes:
        name: Logical name of the data artifact
        producer_task: Task that produces this data
        consumers: Tasks that consume this data
        is_external: Whether this is an input to the pipeline
    """

    name: str
    producer_task: Any = None
    consumers: list[Any] = field(default_factory=list)
    is_external: bool = False

    def add_consumer(self, task: Any) -> None:
        """Add a consumer task for this data."""
        if task not in self.consumers:
            self.consumers.append(task)

    def set_producer(self, task: Any) -> None:
        """Set the producer task for this data."""
        self.producer_task = task
