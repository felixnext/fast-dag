"""Core runner classes and enums."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..dag import DAG


class ExecutionMode(Enum):
    """Execution modes for DAG runner."""

    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    ASYNC = "async"


@dataclass
class ExecutionMetrics:
    """Metrics collected during DAG execution."""

    total_duration: float = 0.0
    nodes_executed: int = 0
    node_times: dict[str, float] = field(default_factory=dict)
    parallel_groups: list[list[str]] = field(default_factory=list)
    errors: dict[str, str] = field(default_factory=dict)


@dataclass
class CoreDAGRunner:
    """Core DAG runner with basic configuration.

    Provides the foundational runner functionality including
    configuration, metrics collection, and basic runner state.
    """

    dag: "DAG"
    mode: ExecutionMode = ExecutionMode.SEQUENTIAL
    max_workers: int = 4
    timeout: float | None = None
    error_strategy: str = "stop"  # stop, continue, retry

    # Runtime state
    _metrics: ExecutionMetrics = field(default_factory=ExecutionMetrics)
    _executor: ThreadPoolExecutor | None = None

    def configure(
        self,
        mode: ExecutionMode | None = None,
        max_workers: int | None = None,
        timeout: float | None = None,
        error_strategy: str | None = None,
    ) -> None:
        """Configure runner settings."""
        if mode is not None:
            self.mode = mode
        if max_workers is not None:
            self.max_workers = max_workers
        if timeout is not None:
            self.timeout = timeout
        if error_strategy is not None:
            self.error_strategy = error_strategy

    def get_metrics(self) -> dict[str, Any]:
        """Get execution metrics."""
        return {
            "total_duration": self._metrics.total_duration,
            "nodes_executed": self._metrics.nodes_executed,
            "node_times": self._metrics.node_times,
            "parallel_groups": self._metrics.parallel_groups,
            "errors": self._metrics.errors,
            "avg_node_time": (
                self._metrics.total_duration / self._metrics.nodes_executed
                if self._metrics.nodes_executed > 0
                else 0.0
            ),
        }
