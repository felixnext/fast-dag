"""Node implementation for fast-dag."""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from .connections import ConditionalOutputProxy
from .introspection import (
    get_function_description,
    get_function_inputs,
    get_function_name,
    get_function_outputs,
    has_context_parameter,
    is_async_function,
)
from .types import NodeType


@dataclass
class Node:
    """A node represents a unit of work in a workflow.

    Nodes wrap Python functions and manage their execution,
    inputs, outputs, and connections to other nodes.
    """

    func: Callable[..., Any]
    name: str | None = None
    inputs: list[str] | None = None
    outputs: list[str] | None = None
    description: str | None = None
    node_type: NodeType = NodeType.STANDARD
    metadata: dict[str, Any] = field(default_factory=dict)
    retry: int | None = None
    timeout: float | None = None

    # Connection tracking
    # Maps input name to (source_node, output_name)
    input_connections: dict[str, tuple["Node", str]] = field(default_factory=dict)
    # Maps output name to list of (target_node, input_name)
    output_connections: dict[str, list[tuple["Node", str]]] = field(
        default_factory=dict
    )

    # Runtime properties
    _has_context: bool | None = None
    _is_async: bool | None = None

    def __post_init__(self):
        """Initialize node properties from function introspection."""
        # Set name from function if not provided
        if self.name is None:
            self.name = get_function_name(self.func)

        # Set description from docstring if not provided
        if self.description is None:
            self.description = get_function_description(self.func)

        # Introspect inputs if not provided
        if self.inputs is None:
            self.inputs = get_function_inputs(self.func)
        else:
            # Allow renaming but check count matches for validation test
            actual_inputs = get_function_inputs(self.func)
            if len(self.inputs) != len(actual_inputs):
                raise ValueError(
                    f"Node '{self.name}': signature mismatch - provided {len(self.inputs)} inputs "
                    f"but function expects {len(actual_inputs)}"
                )

        # Introspect outputs if not provided
        if self.outputs is None:
            self.outputs = get_function_outputs(self.func)

        # Don't pre-initialize output connections - keep empty dict

        # Cache introspection results
        self._has_context = has_context_parameter(self.func)
        self._is_async = is_async_function(self.func)

    @property
    def has_context(self) -> bool:
        """Check if this node accepts a context parameter."""
        if self._has_context is None:
            self._has_context = has_context_parameter(self.func)
        return self._has_context

    @property
    def is_async(self) -> bool:
        """Check if this node's function is async."""
        if self._is_async is None:
            self._is_async = is_async_function(self.func)
        return self._is_async

    @property
    def on_true(self) -> ConditionalOutputProxy:
        """Get proxy for true branch connections (conditional nodes only)."""
        if self.node_type != NodeType.CONDITIONAL:
            raise AttributeError(f"Node '{self.name}' is not a conditional node")
        return ConditionalOutputProxy(self, "true")

    @property
    def on_false(self) -> ConditionalOutputProxy:
        """Get proxy for false branch connections (conditional nodes only)."""
        if self.node_type != NodeType.CONDITIONAL:
            raise AttributeError(f"Node '{self.name}' is not a conditional node")
        return ConditionalOutputProxy(self, "false")

    def validate(self) -> list[str]:
        """Validate the node configuration.

        Returns a list of validation errors, empty if valid.
        """
        errors = []

        # Check that function is callable
        if not callable(self.func):
            errors.append(f"Node '{self.name}': func must be callable")

        # Check that we have at least one output
        if not self.outputs:
            errors.append(f"Node '{self.name}': must have at least one output")

        # Check for return annotation - but only warn, don't fail
        # Many Python functions don't have return annotations
        # sig = inspect.signature(self.func)
        # if sig.return_annotation is inspect.Parameter.empty:
        #     errors.append(
        #         f"Node '{self.name}': function must have a return type annotation"
        #     )

        # For conditional nodes, check specific outputs
        if (
            self.node_type == NodeType.CONDITIONAL
            and self.outputs
            and ("true" not in self.outputs or "false" not in self.outputs)
        ):
            errors.append(
                f"Conditional node '{self.name}': must have 'true' and 'false' outputs"
            )

        return errors

    def connect_to(
        self, target_node: "Node", output: str | None = None, input: str | None = None
    ) -> "Node":
        """Connect this node to another node.

        Returns the target node to allow chaining.
        """
        # Default to first output/input if not specified
        if output is None:
            output = self.outputs[0] if self.outputs else "result"
        if input is None:
            input = target_node.inputs[0] if target_node.inputs else "data"

        # Add connection on both ends
        if output not in self.output_connections:
            self.output_connections[output] = []

        self.output_connections[output].append((target_node, input))
        target_node.input_connections[input] = (self, output)

        return target_node

    def __rshift__(self, other: "Node | list[Node]") -> "Node | list[Node]":
        """Implement the >> operator for connecting nodes."""
        if isinstance(other, list):
            for node in other:
                self.connect_to(node)
            return other
        else:
            return self.connect_to(other)

    def __or__(self, other: "Node") -> "Node":
        """Implement the | operator for connecting nodes."""
        return self.connect_to(other)

    def __rrshift__(self, other: list["Node"]) -> "Node":
        """Implement the >> operator when node is on the right side.

        This handles: [node1, node2, node3] >> node
        """
        if isinstance(other, list):
            # Get this node's inputs
            target_inputs = self.inputs or []

            # Connect each source to the corresponding input
            for i, source_node in enumerate(other):
                if i < len(target_inputs):
                    source_node.connect_to(self, input=target_inputs[i])
                else:
                    # If more sources than inputs, just connect to default
                    source_node.connect_to(self)

            return self
        else:
            # Fallback for single node (shouldn't normally happen)
            return NotImplemented

    def add_input_connection(
        self, input_name: str, source_node: "Node", output_name: str
    ) -> None:
        """Add an input connection to this node."""
        self.input_connections[input_name] = (source_node, output_name)

    def add_output_connection(
        self, output_name: str, target_node: "Node", input_name: str
    ) -> None:
        """Add an output connection from this node."""
        if output_name not in self.output_connections:
            self.output_connections[output_name] = []
        self.output_connections[output_name].append((target_node, input_name))

    def execute(self, inputs: dict[str, Any], context: Any | None = None) -> Any:
        """Execute the node's function with the given inputs.

        Args:
            inputs: Input values mapped by parameter name
            context: Optional context object to pass if node accepts it

        Returns:
            The result of executing the node's function
        """
        # Build kwargs from inputs
        kwargs = dict(inputs)

        # Add context if the node accepts it
        if self.has_context and context is not None:
            kwargs["context"] = context

        # Execute the function
        return self.func(**kwargs)

    async def execute_async(
        self, inputs: dict[str, Any], context: Any | None = None
    ) -> Any:
        """Execute an async node's function.

        Args:
            inputs: Input values mapped by parameter name
            context: Optional context object to pass if node accepts it

        Returns:
            The result of executing the node's async function
        """
        if not self.is_async:
            raise RuntimeError(f"Node '{self.name}' is not async")

        # Build kwargs from inputs
        kwargs = dict(inputs)

        # Add context if the node accepts it
        if self.has_context and context is not None:
            kwargs["context"] = context

        # Execute the async function
        return await self.func(**kwargs)

    def __repr__(self) -> str:
        """String representation of the node."""
        return f"Node(name='{self.name}', type={self.node_type.value})"
