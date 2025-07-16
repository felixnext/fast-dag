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


class NodeList(list):
    """A list of nodes that supports connection operators."""

    def __rshift__(self, other: "Node") -> "Node":
        """Connect all nodes in this list to the target node.

        Maps outputs to inputs in order based on the target node's input names.
        """
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
