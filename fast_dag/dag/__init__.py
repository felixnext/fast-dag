"""DAG module - refactored into smaller components."""

from .builder import DAGBuilder
from .core import DAG as CoreDAG
from .execution import DAGExecutor
from .nested import NestedWorkflows
from .serialization import DAGSerialization
from .visualization import DAGVisualization


class DAG(
    CoreDAG,
    DAGBuilder,
    DAGExecutor,
    NestedWorkflows,
    DAGSerialization,
    DAGVisualization,
):
    """Complete DAG implementation with all functionality."""

    pass


__all__ = ["DAG"]
