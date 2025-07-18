"""DAG runner module combining all functionality."""

from .core import ExecutionMetrics, ExecutionMode
from .execution import DAGExecutor


class DAGRunner(DAGExecutor):
    """Complete DAG runner implementation with all functionality.

    This class combines all runner mixins to provide the full runner functionality:
    - Core configuration and metrics (CoreDAGRunner)
    - Utility functions (RunnerUtils)
    - Node execution (NodeExecutor)
    - Main execution logic (DAGExecutor)
    """

    pass


__all__ = ["DAGRunner", "ExecutionMode", "ExecutionMetrics"]
