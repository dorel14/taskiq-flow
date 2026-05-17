"""
Cache de données pour stocker et récupérer les résultats de tâches.

Implémente un système de cache avec injection de dépendances,
permettant aux tâches de consommer les sorties des tâches
précédentes via leurs noms de flux.

Auteur: SoniqueBay Team
Version: 1.0.2
"""

from typing import Any


class DataCache:
    """
    Cache de données pour stocker et récupérer les résultats de tâches.

    Implémente un système de cache avec injection de dépendances,
    permettant aux tâches de consommer les sorties des tâches
    précédentes via leurs noms de flux.

    Le DataCache est utilisé pendant l'exécution du pipeline pour
    conserver les résultats intermédiaires et les injecter automatiquement
    dans les tâches qui en ont besoin.

    Attributes:
        _cache: Dictionnaire interne {clé -> valeur}

    Example:
        Utilisation basique du cache:

        >>> cache = DataCache()
        >>> cache.set("features", {"tempo": 120})
        >>> cache.set("tags", ["rock", "pop"])
        >>>
        >>> # Récupération des valeurs
        >>> features = cache.get("features")
        >>> print(features)
        {'tempo': 120}
        >>>
        >>> # Vérification d'existence
        >>> if cache.has("tags"):
        ...     print("Tags disponibles")
        >>>
        >>> # Injection automatique de dépendances
        >>> deps = cache.inject(["features", "tags"])
        >>> deps.keys()
        dict_keys(['features', 'tags'])

    """

    def __init__(self) -> None:
        """Initialise un cache vide."""
        self._cache: dict[str, Any] = {}

    def set(self, key: str, value: Any) -> None:
        """
        Stocke une valeur dans le cache.

        Args:
            key: Nom de la sortie / clé de cache
            value: Valeur à stocker

        Example:
            >>> cache = DataCache()
            >>> cache.set("audio_features", {"duration": 180.0})
            >>> "audio_features" in cache.keys()
            True

        """
        self._cache[key] = value

    def get(self, key: str) -> Any:
        """
        Récupère une valeur depuis le cache.

        Args:
            key: Nom de la sortie / clé de cache

        Returns:
            La valeur en cache correspondante

        Raises:
            KeyError: Si la clé n'existe pas dans le cache

        Example:
            >>> cache = DataCache()
            >>> cache.set("result", 42)
            >>> value = cache.get("result")
            >>> print(value)
            42

        """
        if key not in self._cache:
            raise KeyError(f"No cached value found for key: {key}")
        return self._cache[key]

    def has(self, key: str) -> bool:
        """
        Vérifie si une clé existe dans le cache.

        Args:
            key: Nom de la sortie / clé de cache

        Returns:
            True si la clé existe, False sinon

        Example:
            >>> cache = DataCache()
            >>> cache.set("model", trained_model)
            >>> cache.has("model")
            True

        """
        return key in self._cache

    def inject(self, dependencies: list[str]) -> dict[str, Any]:
        """
        Injecte les dépendances en récupérant toutes les valeurs requises.

        Cette méthode est utilisée pour l'injection automatique de
        dépendances dans les tâches : elle retourne un dictionnaire
        mapping chaque nom de dépendance vers sa valeur en cache.

        Args:
            dependencies: Liste des noms de sorties à récupérer

        Returns:
            Dictionnaire {nom_dépendance: valeur}

        Raises:
            KeyError: Si l'une des dépendances n'est pas trouvée

        Example:
            >>> cache = DataCache()
            >>> cache.set("features", [1, 2, 3])
            >>> cache.set("labels", [0, 1, 0])
            >>>
            >>> # Injection pour une tâche qui attend features et labels
            >>> args = cache.inject(["features", "labels"])
            >>> args == {"features": [1,2,3], "labels": [0,1,0]}
            True

        """
        return {dep: self.get(dep) for dep in dependencies}

    def update(self, items: dict[str, Any]) -> None:
        """
        Met à jour le cache avec plusieurs éléments.

        Écrase les valeurs existantes pour les clés fournies.

        Args:
            items: Dictionnaire de paires clé-valeur à stocker

        Example:
            >>> cache = DataCache()
            >>> cache.update({"a": 1, "b": 2, "c": 3})
            >>> cache.get("b")
            2

        """
        self._cache.update(items)

    def clear(self) -> None:
        """
        Vide entièrement le cache.

        Supprime toutes les entrées. Utile pour réinitialiser
        l'état entre plusieurs exécutions.

        Example:
            >>> cache = DataCache()
            >>> cache.set("x", 100)
            >>> cache.clear()
            >>> len(cache)
            0

        """
        self._cache.clear()

    def __len__(self) -> int:
        """
        Retourne le nombre d'éléments en cache.

        Returns:
            Nombre d'entrées dans le cache

        Example:
            >>> cache = DataCache()
            >>> len(cache)
            0
            >>> cache.set("key", "value")
            >>> len(cache)
            1

        """
        return len(self._cache)

    @property
    def size(self) -> int:
        """
        Nombre d'éléments en cache (propriété alternative).

        Returns:
            Nombre d'entrées dans le cache

        Example:
            >>> cache = DataCache()
            >>> cache.size
            0

        """
        return len(self._cache)

    @property
    def keys(self) -> list[str]:
        """
        Liste des clés présentes dans le cache.

        Returns:
            Copie de la liste des clés

        Example:
            >>> cache = DataCache()
            >>> cache.set("a", 1)
            >>> cache.set("b", 2)
            >>> sorted(cache.keys)
            ['a', 'b']

        """
        return list(self._cache.keys())

    def to_dict(self) -> dict[str, Any]:
        """
        Retourne le contenu du cache comme dictionnaire.

        Returns:
            Copie du dictionnaire interne du cache

        Example:
            >>> cache = DataCache()
            >>> cache.set("x", 1)
            >>> cache.set("y", 2)
            >>> d = cache.to_dict()
            >>> d == {"x": 1, "y": 2}
            True

        """
        return dict(self._cache)
