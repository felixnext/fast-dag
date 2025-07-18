"""Node connection management functionality."""

from typing import TYPE_CHECKING, Any

from ..types import NodeType
from .validation import NodeValidator

if TYPE_CHECKING:
    # Avoid circular imports
    Node = Any


class NodeConnections(NodeValidator):
    """Node connection management functionality."""

    def connect_to(
        self, target_node: "Node", output: str | None = None, input: str | None = None
    ) -> "Node":
        """Connect this node to another node.

        Args:
            target_node: The node to connect to
            output: Output name from this node (defaults to first output)
            input: Input name on target node (defaults to first input)

        Returns:
            The target node for chaining
        """
        # Default to first output/input if not specified
        if output is None:
            output = self.outputs[0] if self.outputs else "result"
        if input is None:
            input = target_node.inputs[0] if target_node.inputs else "input"

        # Add connections on both nodes
        self.add_output_connection(output, target_node, input)
        target_node.add_input_connection(input, self, output)

        return target_node

    def add_input_connection(
        self, input_name: str, source_node: "Node", output_name: str
    ) -> None:
        """Add an input connection to this node."""
        # For ANY/ALL nodes, store multiple connections
        if self.node_type in (NodeType.ANY, NodeType.ALL):
            if input_name not in self.multi_input_connections:
                self.multi_input_connections[input_name] = []
            self.multi_input_connections[input_name].append((source_node, output_name))

        # Always store in regular input_connections for compatibility
        self.input_connections[input_name] = (source_node, output_name)

    def add_output_connection(
        self, output_name: str, target_node: "Node", input_name: str
    ) -> None:
        """Add an output connection from this node."""
        if output_name not in self.output_connections:
            self.output_connections[output_name] = []
        self.output_connections[output_name].append((target_node, input_name))
