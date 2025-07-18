"""Node module combining all functionality."""

from .execution import NodeExecution


class Node(NodeExecution):
    """Complete Node implementation with all functionality.

    This class combines all node mixins to provide the full node functionality:
    - Core properties and metadata (CoreNode)
    - Validation (NodeValidator)
    - Connection management (NodeConnections)
    - Operator overloads (NodeOperators)
    - Execution logic (NodeExecution)
    """

    pass


__all__ = ["Node"]
