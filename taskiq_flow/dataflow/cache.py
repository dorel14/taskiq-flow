"""Cache de données pour stocker et récupérer les résultats de tâches.

Implémente un système de cache avec injection de dépendances,
permettant aux tâches de consommer les sorties des tâches
précédentes via leurs noms de flux.

Auteur: SoniqueBay Team
Version: 0.3.2
"""

from typing import Any


class DataCache:
    """
    Cache for storing and retrieving task results.

    Provides automatic dependency injection by mapping output names
    to their values, allowing tasks to receive their dependencies
    as function arguments automatically.
    """

    def __init__(self) -> None:
        """Initialize an empty cache."""
        self._cache: dict[str, Any] = {}

    def set(self, key: str, value: Any) -> None:
        """
        Store a value in the cache.

        Args:
            key: The output name / cache key
            value: The value to store
        """
        self._cache[key] = value

    def get(self, key: str) -> Any:
        """
        Retrieve a value from the cache.

        Args:
            key: The output name / cache key

        Returns:
            The cached value

        Raises:
            KeyError: If the key is not found in the cache
        """
        if key not in self._cache:
            raise KeyError(f"No cached value found for key: {key}")
        return self._cache[key]

    def has(self, key: str) -> bool:
        """
        Check if a key exists in the cache.

        Args:
            key: The output name / cache key

        Returns:
            True if the key exists, False otherwise
        """
        return key in self._cache

    def inject(self, dependencies: list[str]) -> dict[str, Any]:
        """
        Inject dependencies by retrieving all required values.

        Args:
            dependencies: List of output names to retrieve

        Returns:
            Dictionary mapping dependency names to their values

        Raises:
            KeyError: If any dependency is not found in the cache
        """
        return {dep: self.get(dep) for dep in dependencies}

    def update(self, items: dict[str, Any]) -> None:
        """
        Update the cache with multiple items.

        Args:
            items: Dictionary of key-value pairs to store
        """
        self._cache.update(items)

    def clear(self) -> None:
        """Clear all cached values."""
        self._cache.clear()

    def __len__(self) -> int:
        """Return the number of cached items."""
        return len(self._cache)

    @property
    def size(self) -> int:
        """Return the number of cached items."""
        return len(self._cache)

    @property
    def keys(self) -> list[str]:
        """Return all cache keys."""
        return list(self._cache.keys())

    def to_dict(self) -> dict[str, Any]:
        """
        Return the cache contents as a dictionary.

        Returns:
            Dictionary of all cached key-value pairs
        """
        return dict(self._cache)

