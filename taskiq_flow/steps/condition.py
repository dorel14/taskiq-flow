"""Step conditionnel pour exécution sous condition.

Exécute une tâche si une condition (expression ou callable) est vraie,
sinon exécute une tâche alternative si fournie.

Auteur: SoniqueBay Team
Version: 0.3.1
"""

import ast
from collections.abc import Callable
from typing import Any

import pydantic
from taskiq import AsyncBroker, TaskiqResult

from taskiq_flow.abc import AbstractStep


class ConditionStep(pydantic.BaseModel, AbstractStep, step_name="condition"):
    """
    Step d'exécution conditionnelle.

    Exécute une tâche (ou une alternative) en fonction d'une condition
    évaluée sur le résultat de l'étape précédente.

    Attributs:
        condition: Condition sous forme de string (expression) ou callable
        task: Tâche à exécuter si condition vraie
        else_task: Tâche alternative à exécuter si condition fausse (optionnel)
    """

    condition: str | Callable[[Any], bool]
    task: Any  # SequentialStep
    else_task: Any | None = None  # SequentialStep | None

    async def act(
        self,
        broker: AsyncBroker,
        step_number: int,
        parent_task_id: str,
        task_id: str,
        pipe_data: str,
        result: TaskiqResult[Any],
    ) -> None:
        """
        Évalue la condition et exécute la tâche correspondante.

        Args:
            broker: Broker TaskIQ
            step_number: Numéro d'étape
            parent_task_id: ID tâche parente
            task_id: ID pour cette étape
            pipe_data: Pipeline sérialisé
            result: Résultat de l'étape précédente

        Note:
            Si condition est une chaîne, elle est évaluée comme
            une expression Python safe avec variable 'value'.
            Si callable, elle est appelée avec le résultat.
        """
        # Evaluate condition
        if isinstance(self.condition, str):
            # Simple expression evaluation (safe support)
            condition_met = self._eval_condition(self.condition, result.return_value)
        elif callable(self.condition):
            # Check if it's a coroutine function
            import asyncio  # noqa: PLC0415

            if asyncio.iscoroutinefunction(self.condition):
                condition_met = await self.condition(result.return_value)
            else:
                condition_met = self.condition(result.return_value)
        else:
            condition_met = bool(self.condition)

        if condition_met:
            await self.task.act(
                broker,
                step_number,
                parent_task_id,
                task_id,
                pipe_data,
                result,
            )
        elif self.else_task:
            await self.else_task.act(
                broker,
                step_number,
                parent_task_id,
                task_id,
                pipe_data,
                result,
            )
        # If no else and condition not met, skip this step

    def _eval_condition(self, expression: str, value: Any) -> bool:
        """Safe expression evaluation using AST."""
        # Basic support for simple expressions with value variable
        try:
            # Parse the expression
            tree = ast.parse(expression, mode="eval")

            # Define allowed nodes for safety - more permissive for common operations
            allowed_nodes = {
                ast.Expression,
                ast.UnaryOp,
                ast.BinOp,
                ast.Compare,
                ast.Name,
                ast.Constant,
                ast.Call,
                ast.Subscript,
                ast.Index,
                ast.Attribute,
                ast.Load,
                ast.Eq,
                ast.NotEq,
                ast.Lt,
                ast.LtE,
                ast.Gt,
                ast.GtE,
                ast.Is,
                ast.IsNot,
                ast.And,
                ast.Or,
                ast.Not,
                ast.BoolOp,
                ast.List,
                ast.Dict,
                ast.Tuple,
            }

            # Check all nodes in the tree
            for node in ast.walk(tree):
                if type(node) not in allowed_nodes:
                    return False

                # Additional restrictions
                if isinstance(node, ast.Call):
                    # Only allow specific safe function calls and method calls
                    if isinstance(node.func, ast.Name):
                        # Direct function calls
                        if node.func.id not in ["len", "str", "int", "float", "bool"]:
                            return False
                    elif isinstance(node.func, ast.Attribute):
                        # Method calls - allow safe methods on basic types
                        if hasattr(node.func, "attr"):
                            safe_methods = ["get", "keys", "values", "items"]
                            if node.func.attr not in safe_methods:
                                return False
                    else:
                        return False

                if isinstance(node, ast.Name) and node.id not in [
                    "value",
                    "len",
                    "str",
                    "int",
                    "float",
                    "bool",
                    "True",
                    "False",
                    "None",
                ]:
                    return False

                # Prevent dangerous attribute access
                if isinstance(node, ast.Attribute) and (
                    not hasattr(node, "attr") or node.attr.startswith("_")
                ):
                    return False

            # Compile and evaluate with restricted globals
            compiled = compile(tree, "<string>", "eval")
            safe_globals = {
                "__builtins__": {
                    "len": len,
                    "str": str,
                    "int": int,
                    "float": float,
                    "bool": bool,
                    "True": True,
                    "False": False,
                    "None": None,
                },
            }
            result = eval(compiled, safe_globals, {"value": value})  # noqa: S307
            return bool(result)
        except Exception:
            return False
