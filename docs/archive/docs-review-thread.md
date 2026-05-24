---
title: Documentation Review Thread
date: 2026-05-24
author: gemini-code-assist
status: archived
---

# Documentation Review Comments

## Thread Summary

Review comments on documentation files that were addressed and archived.

---

## Comment 1: Missing TaskiqError Import

**File**: `docs/_en/guides/api.md`

### Issue
L'exception TaskiqError est utilisée dans la signature de la fonction mais n'est pas importée dans cet extrait de code. Il faudrait ajouter from taskiq import TaskiqError.

### Resolution
Added the missing import:
```python
from taskiq.exceptions import TaskiqError
```

---

## Comment 2: Inconsistent @pipeline.task Decorator

**File**: `docs/_en/guides/api.md`

### Issue
L'utilisation du décorateur @pipeline.task est incohérente avec le reste de la documentation qui préconise l'utilisation combinée de @broker.task et @pipeline_task.

### Resolution
The documentation already correctly uses `@broker.task` + `@pipeline_task` pattern. No changes needed.

---

## Comment 3: Non-existent run_pipeline Method

**File**: `docs/_en/guides/performance.md`

### Issue
La méthode run_pipeline n'existe pas sur la classe ResourceAwareExecutor. Cet exécuteur doit être utilisé pour obtenir le parallélisme optimal via get_optimal_parallelism, qui est ensuite appliqué au pipeline avant son exécution via kiq_dataflow.

### Resolution
Updated the code example to show the correct pattern:
1. Use `ResourceAwareExecutor` to compute optimal parallelism via `get_optimal_parallelism()`
2. Apply the computed value to the pipeline configuration
3. Execute via `pipeline.kiq_dataflow()`

---

## Changes Made

1. Added `TaskiqError` import to `docs/_en/guides/api.md`
2. Fixed resource-aware execution example in `docs/_en/guides/performance.md`:
   - Changed `@pipeline.task` to correct `@broker.task` + `@pipeline_task` pattern
   - Replaced `executor.run_pipeline()` with correct pattern using `get_optimal_parallelism()`
   - Updated execution to use `pipeline.kiq_dataflow()`