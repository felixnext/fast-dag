"""Context classes for carrying data through workflow execution."""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class Context:
    """Execution context flowing through workflow.

    Stores results, metadata, and metrics during workflow execution.
    Provides dict-like access to results for convenience.
    """

    results: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    metrics: dict[str, Any] = field(default_factory=dict)

    def set_result(self, key: str, value: Any) -> None:
        """Set a result in the context."""
        self.results[key] = value

    def get_result(self, key: str) -> Any:
        """Get a result from the context."""
        return self.results[key]

    def get(self, key: str, default: Any = None) -> Any:
        """Get a result with a default value if not found."""
        return self.results.get(key, default)

    def __getitem__(self, key: str) -> Any:
        """Dict-like access to results."""
        return self.results[key]

    def __contains__(self, key: str) -> bool:
        """Check if a result exists."""
        return key in self.results


@dataclass
class FSMContext(Context):
    """Extended context for state machines.

    Adds state history tracking and cycle result management
    for finite state machine workflows.
    """

    state_history: list[str] = field(default_factory=list)
    cycle_count: int = 0
    cycle_results: dict[str, list[Any]] = field(default_factory=dict)

    def add_cycle_result(self, node_name: str, value: Any) -> None:
        """Add a result for a node in the current cycle."""
        if node_name not in self.cycle_results:
            self.cycle_results[node_name] = []
        self.cycle_results[node_name].append(value)

    def get_latest(self, node_name: str) -> Any | None:
        """Get the most recent result for a node.

        Returns the latest cycle result if available,
        otherwise falls back to regular results.
        """
        if node_name in self.cycle_results and self.cycle_results[node_name]:
            return self.cycle_results[node_name][-1]
        return self.results.get(node_name)

    def get_cycle(self, node_name: str, cycle: int) -> Any | None:
        """Get result from a specific cycle.

        Returns None if the node hasn't been executed that many times
        or if the cycle index is out of bounds.
        """
        if node_name in self.cycle_results and 0 <= cycle < len(
            self.cycle_results[node_name]
        ):
            return self.cycle_results[node_name][cycle]
        return None
