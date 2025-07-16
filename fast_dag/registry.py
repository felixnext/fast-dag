"""Function registry for dynamic workflow creation."""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any


@dataclass
class FunctionRegistry:
    """Registry for storing and retrieving functions by name.

    Useful for dynamic workflow creation and function reuse.
    """

    functions: dict[str, Callable] = field(default_factory=dict)
    metadata: dict[str, dict[str, Any]] = field(default_factory=dict)

    def register(
        self,
        func: Callable | None = None,
        name: str | None = None,
        **metadata: Any,
    ) -> Callable:
        """Register a function in the registry.

        Can be used as decorator:

        @registry.register
        def my_func():
            pass

        Or with custom name:

        @registry.register(name="custom_name")
        def my_func():
            pass
        """

        def decorator(f: Callable) -> Callable:
            func_name = name or f.__name__
            self.functions[func_name] = f
            if metadata:
                self.metadata[func_name] = metadata
            return f

        if func is None:
            return decorator
        else:
            return decorator(func)

    def get(self, name: str) -> Callable | None:
        """Get a function by name."""
        return self.functions.get(name)

    def __getitem__(self, name: str) -> Callable:
        """Dict-like access to functions."""
        if name not in self.functions:
            raise KeyError(f"Function '{name}' not found in registry")
        return self.functions[name]

    def __contains__(self, name: str) -> bool:
        """Check if a function is registered."""
        return name in self.functions

    def list_functions(self) -> list[str]:
        """List all registered function names."""
        return list(self.functions.keys())

    def clear(self) -> None:
        """Clear all registered functions."""
        self.functions.clear()
        self.metadata.clear()
