"""Node operator overloads functionality."""

from typing import TYPE_CHECKING

from ..types import NodeType
from .connections import NodeConnections

if TYPE_CHECKING:
    from .node import Node  # type: ignore[import-untyped]


class NodeOperators(NodeConnections):
    """Node operator overloads functionality."""

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
            # Check if multiple source connections are allowed
            # Only ANY and ALL nodes can have multiple source connections to the same input
            # Multiple sources to different inputs are allowed for all node types
            if len(other) > 1 and self.node_type not in (NodeType.ANY, NodeType.ALL):
                target_inputs = self.inputs or []

                # If we have fewer target inputs than sources, some inputs will get multiple connections
                if len(other) > len(target_inputs):
                    raise ValueError(
                        f"Cannot connect {len(other)} source nodes to node '{self.name}' "
                        f"with only {len(target_inputs)} inputs. "
                        "Use @dag.any() or @dag.all() decorators for multi-input convergence."
                    )

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
