"""Core Node class and basic functionality."""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from ..connections import ConditionalOutputProxy, InputCollection, OutputCollection
from ..introspection import (
    get_function_description,
    get_function_inputs,
    get_function_name,
    get_function_outputs,
    has_context_parameter,
    is_async_function,
)
from ..types import NodeType


@dataclass
class CoreNode:
    """Core Node class with basic functionality.

    This class provides the foundational node functionality including
    function wrapping, metadata management, and basic properties.
    """

    func: Callable[..., Any]
    name: str | None = None
    inputs: list[str] | None = None
    outputs: list[str] | None = None
    description: str | None = None
    node_type: NodeType = NodeType.STANDARD
    metadata: dict[str, Any] = field(default_factory=dict)
    retry: int | None = None
    retry_delay: float | None = None
    timeout: float | None = None

    # Connection tracking
    # Maps input name to (source_node, output_name) for regular nodes
    # For ANY/ALL nodes, this stores the last connection for compatibility
    input_connections: dict[str, tuple[Any, str]] = field(default_factory=dict)
    # Maps output name to list of (target_node, input_name)
    output_connections: dict[str, list[tuple[Any, str]]] = field(default_factory=dict)
    # For ANY/ALL nodes: Maps input name to list of (source_node, output_name)
    multi_input_connections: dict[str, list[tuple[Any, str]]] = field(
        default_factory=dict
    )

    # Runtime properties
    _has_context: bool | None = None
    _is_async: bool | None = None

    # Lifecycle hooks
    pre_execute: Callable[[Any, dict[str, Any]], None] | None = None
    post_execute: Callable[[Any, dict[str, Any], Any], Any] | None = None
    on_error: Callable[[Any, dict[str, Any], Exception], None] | None = None

    # Caching configuration
    cached: bool = False
    cache_backend: str = "memory"
    cache_ttl: float | None = None

    def __post_init__(self):
        """Initialize node properties from function introspection."""
        # Auto-detect name if not provided
        if self.name is None:
            self.name = get_function_name(self.func)

        # Auto-detect inputs if not provided
        if self.inputs is None:
            self.inputs = get_function_inputs(self.func)
        else:
            # Validate explicit inputs against function signature
            # Only validate the count - allowing renamed parameters for flexibility
            import inspect

            sig = inspect.signature(self.func)
            all_params = []
            for param_name, _param in sig.parameters.items():
                if param_name not in ("self", "context"):
                    all_params.append(param_name)

            # Check if the number of inputs matches the total parameters (required + optional)
            # But allow fewer inputs (for optional parameters)
            if len(self.inputs) > len(all_params):
                # Too many inputs provided
                raise ValueError(
                    f"Node '{self.name}': Input signature mismatch. "
                    f"Function has {len(all_params)} parameters but {len(self.inputs)} inputs provided"
                )

        # Auto-detect outputs if not provided
        if self.outputs is None:
            self.outputs = get_function_outputs(self.func)

            # Special handling for conditional nodes
            # Check if the function returns ConditionalReturn
            try:
                import inspect
                from typing import get_type_hints

                sig = inspect.signature(self.func)
                if sig.return_annotation is not inspect.Parameter.empty:
                    try:
                        hints = get_type_hints(self.func)
                        return_type = hints.get("return", sig.return_annotation)
                        # Check if return type is ConditionalReturn
                        if (
                            hasattr(return_type, "__name__")
                            and return_type.__name__ == "ConditionalReturn"
                        ):
                            self.outputs = ["on_true", "on_false"]
                            if self.node_type == NodeType.STANDARD:
                                self.node_type = NodeType.CONDITIONAL
                    except Exception:
                        pass  # Ignore errors in type hint resolution
            except Exception:
                pass  # Ignore introspection errors

        # Auto-detect description if not provided
        if self.description is None:
            self.description = get_function_description(self.func)

        # Cache introspection results
        self._has_context = has_context_parameter(self.func)
        self._is_async = is_async_function(self.func)

    @property
    def has_context(self) -> bool:
        """Check if the node's function accepts a context parameter."""
        if self._has_context is None:
            self._has_context = has_context_parameter(self.func)
        return self._has_context

    @property
    def is_async(self) -> bool:
        """Check if the node's function is async."""
        if self._is_async is None:
            self._is_async = is_async_function(self.func)
        return self._is_async

    @property
    def input_ports(self) -> InputCollection:
        """Get input ports for the node."""
        return InputCollection(self)

    @property
    def output_ports(self) -> OutputCollection:
        """Get output ports for the node."""
        return OutputCollection(self)

    @property
    def on_true(self) -> ConditionalOutputProxy:
        """Get proxy for true branch of conditional node."""
        return ConditionalOutputProxy(self, "true")

    @property
    def on_false(self) -> ConditionalOutputProxy:
        """Get proxy for false branch of conditional node."""
        return ConditionalOutputProxy(self, "false")

    def __repr__(self) -> str:
        """String representation of the node."""
        return f"Node(name='{self.name}', func={self.func.__name__})"
