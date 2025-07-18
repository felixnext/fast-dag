"""Function registry for serialization support."""

import importlib
import inspect
from collections.abc import Callable


class FunctionRegistry:
    """Registry for functions that can be serialized/deserialized."""

    _functions: dict[str, Callable] = {}
    _modules: set[str] = set()

    @classmethod
    def register(cls, func: Callable, name: str | None = None) -> None:
        """Register a function for serialization."""
        if name is None:
            module = func.__module__
            qualname = func.__qualname__
            name = f"{module}.{qualname}"
        cls._functions[name] = func

    @classmethod
    def get(cls, name: str) -> Callable:
        """Get a function by its registered name."""
        # First check if already registered
        if name in cls._functions:
            return cls._functions[name]

        # Try to import and find the function
        try:
            # Split module and function name
            parts = name.split(".")
            for i in range(len(parts) - 1, 0, -1):
                module_name = ".".join(parts[:i])
                func_path = ".".join(parts[i:])

                try:
                    module = importlib.import_module(module_name)
                    obj = module

                    # Navigate to the function
                    for part in func_path.split("."):
                        obj = getattr(obj, part)

                    if callable(obj):
                        cls._functions[name] = obj
                        return obj
                except (ImportError, AttributeError):
                    continue

            raise ValueError(f"Function not found: {name}")
        except Exception as e:
            raise ValueError(f"Cannot load function '{name}': {e}") from e

    @classmethod
    def register_module(cls, module_name: str, auto_discover: bool = True) -> None:
        """Register all functions from a module."""
        if module_name in cls._modules:
            return

        cls._modules.add(module_name)

        if auto_discover:
            try:
                module = importlib.import_module(module_name)
                for name, obj in inspect.getmembers(module):
                    if inspect.isfunction(obj) and obj.__module__ == module_name:
                        func_name = f"{module_name}.{name}"
                        cls._functions[func_name] = obj
            except ImportError:
                pass

    @classmethod
    def clear(cls) -> None:
        """Clear the registry."""
        cls._functions.clear()
        cls._modules.clear()

    @classmethod
    def list_functions(cls) -> list[str]:
        """List all registered function names."""
        return list(cls._functions.keys())


def get_function_path(func: Callable) -> str:
    """Get the full path to a function for serialization."""
    module = func.__module__
    qualname = func.__qualname__

    # Check for lambda functions which cannot be serialized
    if "<lambda>" in qualname:
        raise ValueError(f"Lambda functions cannot be serialized: {func}")

    return f"{module}.{qualname}"
