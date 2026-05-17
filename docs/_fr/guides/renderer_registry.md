---
title: Guide du registre de rendu
nav_order: 28
color_scheme: dark
---
# Guide du registre de rendu

Ce guide explique comment utiliser le registre de rendu dans TaskIQ-Flow pour gérer et étendre les capacités de rendu des résultats des tâches et des visualisations de pipelines.

## Vue d'ensemble

Le registre de rendu est un registre central qui associe des types de données à des fonctions de rendu. Il permet de personnaliser la façon dont différents types de données sont affichés dans le tableau de bord TaskIQ-Flow, les réponses API et les visualisations.

## Utilisation du registre de rendu

### Enregistrement d'un rendu personnalisé

Pour enregistrer un rendu personnalisé pour un type de données spécifique, utilisez la fonction `register_renderer` du module `taskiq_flow.registry` :

```python
from taskiq_flow.registry import register_renderer
from typing import Any

def render_my_data(data: Any) -> str:
    """Rendu personnalisé pour les objets MyData."""
    return f"<div class='my-data'>My data : {data}</div>"

register_renderer(MyData, render_my_data)
```

### Rendus intégrés

TaskIQ-Flow est livré avec plusieurs rendus intégrés pour les types de données courants :

- `str` : Rendu sous forme de texte brut
- `int`, `float` : Rendu sous forme de nombres
- `list`, `tuple` : Rendu sous forme de tableaux JSON
- `dict` : Rendu sous forme d'objets JSON
- `bytes` : Rendu sous forme de chaîne codée en base64
- `None` : Rendu sous forme de chaîne vide

Vous pouvez remplacer l'un de ces rendus en enregistrant votre propre rendu pour le même type.

### Accès au registre de rendu

Le registre de rendu est accessible via le module `taskiq_flow.renderer_registry` :

```python
from taskiq_flow import renderer_registry

# Obtenir un rendu pour un type spécifique
renderer = renderer_registry.get(MyData)

# Vérifier si un rendu est enregistré pour un type
if renderer_registry.has(MyData):
    print("Rendu trouvé")
```

## Extension du registre

Vous pouvez étendre le registre en créant une instance de registre personnalisée et en l'enregistrant avec l'application :

```python
from taskiq_flow.registry import RendererRegistry
from taskiq_flow import set_renderer_registry

custom_registry = RendererRegistry()
custom_registry.register(MyData, lambda data: f"Personnalisé : {data}")

set_renderer_registry(custom_registry)
```

Ceci est utile lorsque vous souhaitez isoler les enregistrements de rendu pour différentes applications ou environnements de test.

## Exemple : Rendu des DataFrames Pandas

Voici un exemple d'enregistrement d'un rendu pour les DataFrames Pandas afin de les afficher sous forme de tableaux HTML :

```python
import pandas as pd
from taskiq_flow.registry import register_renderer

def render_dataframe(df: pd.DataFrame) -> str:
    """Rendu d'un DataFrame Pandas sous forme de tableau HTML."""
    return df.to_html(index=False, table_id="dataframe-table")

register_renderer(pd.DataFrame, render_dataframe)
```

Désormais, lorsqu'une tâche renvoie un DataFrame Pandas, il sera affiché sous la forme d'un tableau HTML interactif dans le tableau de bord.

## Référence API

Pour plus de détails, consultez l'[API du registre de rendu](../api/renderer_registry.md).

---