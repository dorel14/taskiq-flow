---
title: Renderer Registry Guide
nav_order: 28
color_scheme: dark
---
# Renderer Registry Guide

This guide explains how to use the Renderer Registry in TaskIQ-Flow to manage and extend rendering capabilities for task results and pipeline visualizations.

## Overview

The Renderer Registry is a central registry that maps data types to rendering functions. It allows you to customize how different types of data are displayed in the TaskIQ-Flow dashboard, API responses, and visualizations.

## Using the Renderer Registry

### Registering a Custom Renderer

To register a custom renderer for a specific data type, use the `register_renderer` function from the `taskiq_flow.registry` module:

```python
from taskiq_flow.registry import register_renderer
from typing import Any

def render_my_data(data: Any) -> str:
    """Custom renderer for MyData objects."""
    return f"<div class='my-data'>My data: {data}</div>"

register_renderer(MyData, render_my_data)
```

### Built-in Renderers

TaskIQ-Flow comes with several built-in renderers for common data types:

- `str`: Rendered as plain text
- `int`, `float`: Rendered as numbers
- `list`, `tuple`: Rendered as JSON arrays
- `dict`: Rendered as JSON objects
- `bytes`: Rendered as base64-encoded string
- `None`: Rendered as empty string

You can override any of these by registering your own renderer for the same type.

### Accessing the Renderer Registry

The renderer registry is accessible via the `taskiq_flow.renderer_registry` module:

```python
from taskiq_flow import renderer_registry

# Get a renderer for a specific type
renderer = renderer_registry.get(MyData)

# Check if a renderer is registered for a type
if renderer_registry.has(MyData):
    print("Renderer found")
```

## Extending the Registry

You can extend the registry by creating a custom registry instance and registering it with the application:

```python
from taskiq_flow.registry import RendererRegistry
from taskiq_flow import set_renderer_registry

custom_registry = RendererRegistry()
custom_registry.register(MyData, lambda data: f"Custom: {data}")

set_renderer_registry(custom_registry)
```

This is useful when you want to isolate renderer registrations for different applications or testing environments.

## Example: Rendering Pandas DataFrames

Here's an example of registering a renderer for Pandas DataFrames to display them as HTML tables:

```python
import pandas as pd
from taskiq_flow.registry import register_renderer

def render_dataframe(df: pd.DataFrame) -> str:
    """Render a Pandas DataFrame as an HTML table."""
    return df.to_html(index=False, table_id="dataframe-table")

register_renderer(pd.DataFrame, render_dataframe)
```

Now, when a task returns a Pandas DataFrame, it will be displayed as an interactive HTML table in the dashboard.

## API Reference

For more details, see the [Renderer Registry API](../api/renderer_registry.md).

---