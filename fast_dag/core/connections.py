"""Connection utilities and special connection types."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .node import Node


class ConditionalOutputProxy:
    """Proxy object for conditional node outputs (on_true, on_false)."""

    def __init__(self, node: "Node", output_name: str):
        self.node = node
        self.output_name = output_name

    def __rshift__(self, other: "Node | list[Node]") -> "Node | list[Node]":
        """Implement >> operator for conditional outputs."""
        if isinstance(other, list):
            for target in other:
                self.node.connect_to(target, output=self.output_name)
            return other
        else:
            return self.node.connect_to(other, output=self.output_name)

    def __or__(self, other: "Node") -> "Node":
        """Implement | operator for conditional outputs."""
        return self.node.connect_to(other, output=self.output_name)

    def connect_to(self, target: "Node", input: str | None = None) -> "Node":
        """Connect this conditional output to a target node."""
        return self.node.connect_to(target, output=self.output_name, input=input)


class OutputProxy:
    """Proxy object for specific node outputs."""

    def __init__(self, node: "Node", output_name: str):
        self.node = node
        self.output_name = output_name

    def connect_to(
        self, target: "Node | InputProxy", input: str | None = None
    ) -> "Node":
        """Connect this output to a target node or input."""
        if isinstance(target, InputProxy):
            # Connect to specific input
            return self.node.connect_to(
                target.node, output=self.output_name, input=target.input_name
            )
        else:
            # Connect to node
            return self.node.connect_to(target, output=self.output_name, input=input)

    def __rshift__(self, other: "Node | InputProxy") -> "Node":
        """Implement >> operator for specific outputs."""
        return self.connect_to(other)

    def __or__(self, other: "Node | InputProxy") -> "Node":
        """Implement | operator for specific outputs."""
        return self.connect_to(other)


class InputProxy:
    """Proxy object for specific node inputs."""

    def __init__(self, node: "Node", input_name: str):
        self.node = node
        self.input_name = input_name


class OutputCollection:
    """Collection of output proxies for a node."""

    def __init__(self, node: "Node"):
        self.node = node

    def __getitem__(self, output_name: str) -> OutputProxy:
        """Get a specific output proxy."""
        if self.node.outputs and output_name not in self.node.outputs:
            raise KeyError(
                f"Output '{output_name}' not found in node '{self.node.name}'. Available outputs: {self.node.outputs}"
            )
        return OutputProxy(self.node, output_name)


class InputCollection:
    """Collection of input proxies for a node."""

    def __init__(self, node: "Node"):
        self.node = node

    def __getitem__(self, input_name: str) -> InputProxy:
        """Get a specific input proxy."""
        if self.node.inputs and input_name not in self.node.inputs:
            raise KeyError(
                f"Input '{input_name}' not found in node '{self.node.name}'. Available inputs: {self.node.inputs}"
            )
        return InputProxy(self.node, input_name)


class NodeList(list):
    """A list of nodes that supports connection operators."""

    def __rshift__(self, other: "Node") -> "Node":
        """Connect all nodes in this list to the target node.

        Maps outputs to inputs in order based on the target node's input names.
        """
        from .types import NodeType

        # Check if multiple source connections are allowed
        # Only ANY and ALL nodes can have multiple source connections to the same input
        # Multiple sources to different inputs are allowed for all node types
        if len(self) > 1 and other.node_type not in (NodeType.ANY, NodeType.ALL):
            target_inputs = other.inputs or []

            # If we have fewer target inputs than sources, some inputs will get multiple connections
            if len(self) > len(target_inputs):
                raise ValueError(
                    f"Cannot connect {len(self)} source nodes to node '{other.name}' "
                    f"with only {len(target_inputs)} inputs. "
                    "Use @dag.any() or @dag.all() decorators for multi-input convergence."
                )

        # Get the target node's inputs
        target_inputs = other.inputs or []

        # Connect each source to the corresponding input
        for i, source_node in enumerate(self):
            if i < len(target_inputs):
                source_node.connect_to(other, input=target_inputs[i])
            else:
                # If more sources than inputs, just connect to default
                source_node.connect_to(other)

        return other

    def __or__(self, other: "Node") -> "Node":
        """Connect using pipe operator."""
        return self.__rshift__(other)
