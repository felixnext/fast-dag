"""Function introspection utilities."""

import inspect
from collections.abc import Callable
from typing import get_type_hints


def get_function_name(func: Callable) -> str:
    """Extract the name of a function."""
    return func.__name__


def get_function_description(func: Callable) -> str | None:
    """Extract the docstring description from a function."""
    doc = inspect.getdoc(func)
    if doc:
        # Return first line of docstring as description
        return doc.split("\n")[0].strip()
    return None


def get_function_inputs(func: Callable) -> list[str]:
    """Extract input parameter names from a function.

    Excludes 'self', 'cls', and 'context' parameters.
    """
    sig = inspect.signature(func)
    inputs = []

    for name, param in sig.parameters.items():
        # Skip special parameters
        if name in ("self", "cls", "context"):
            continue
        # Skip *args and **kwargs
        if param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD):
            continue
        inputs.append(name)

    return inputs


def get_function_outputs(func: Callable) -> list[str]:
    """Extract output names from a function.

    By default returns ['result'] unless the function returns a tuple
    with annotations.
    """
    sig = inspect.signature(func)

    # Check if we have a return annotation
    if sig.return_annotation is not inspect.Parameter.empty:
        # Try to get type hints which handles forward references better
        try:
            hints = get_type_hints(func)
            return_type = hints.get("return", sig.return_annotation)
        except (NameError, AttributeError):
            return_type = sig.return_annotation

        # Check if it's a tuple type
        if (
            hasattr(return_type, "__origin__")
            and return_type.__origin__ is tuple
            and hasattr(return_type, "__args__")
        ):
            # If we have tuple element types, use their count
            return [f"output_{i}" for i in range(len(return_type.__args__))]

    # Default single output
    return ["result"]


def has_context_parameter(func: Callable) -> bool:
    """Check if a function has a 'context' parameter."""
    sig = inspect.signature(func)
    return "context" in sig.parameters


def is_async_function(func: Callable) -> bool:
    """Check if a function is async."""
    return inspect.iscoroutinefunction(func)
