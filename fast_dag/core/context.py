"""Context classes for carrying data through workflow execution."""

from dataclasses import dataclass, field
from typing import Any


class CycleResults:
    """Dual-interface container for cycle results that supports both dict and list access."""

    def __init__(self):
        self._values = []  # For list-like access
        self._by_state = {}  # For dict-like access

    def append(self, value: Any) -> None:
        """Add a value to the flat list (for backward compatibility)."""
        self._values.append(value)

    def add_for_state(self, state_name: str, value: Any) -> None:
        """Add a value for a specific state."""
        if state_name not in self._by_state:
            self._by_state[state_name] = []
        self._by_state[state_name].append(value)

    def __getitem__(self, key):
        """Support both dict and list access."""
        if isinstance(key, int):
            return self._values[key]
        elif isinstance(key, str):
            return self._by_state[key]
        else:
            raise TypeError(f"Key must be int or str, not {type(key)}")

    def __len__(self):
        """Return length of the flat list."""
        return len(self._values)

    def __iter__(self):
        """Iterate over flat list."""
        return iter(self._values)

    def __eq__(self, other):
        """Compare with list or dict."""
        if isinstance(other, list):
            return self._values == other
        elif isinstance(other, dict):
            return self._by_state == other
        return False

    def __contains__(self, key):
        """Support 'in' operator for dict-like keys."""
        if isinstance(key, str):
            return key in self._by_state
        return False

    def __repr__(self):
        return f"CycleResults(values={self._values}, by_state={self._by_state})"


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

    def has_result(self, key: str) -> bool:
        """Check if a result exists for the given key."""
        return key in self.results

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
    cycle_results: CycleResults = field(default_factory=CycleResults)
    state_cycle_results: dict[str, list[Any]] = field(default_factory=dict)

    def add_cycle_result(self, node_name: str, value: Any) -> None:
        """Add a result for a node in the current cycle."""
        # Add to both flat list and per-state dictionary
        self.cycle_results.append(value)
        self.cycle_results.add_for_state(node_name, value)
        # Also track per-state results
        if node_name not in self.state_cycle_results:
            self.state_cycle_results[node_name] = []
        self.state_cycle_results[node_name].append(value)

    def get_latest(self, node_name: str) -> Any | None:
        """Get the most recent result for a node.

        Returns the latest cycle result if available,
        otherwise falls back to regular results.
        """
        if (
            node_name in self.state_cycle_results
            and self.state_cycle_results[node_name]
        ):
            return self.state_cycle_results[node_name][-1]
        return self.results.get(node_name)

    def get_cycle(self, node_name: str, cycle: int) -> Any | None:
        """Get result from a specific cycle.

        Returns None if the node hasn't been executed that many times
        or if the cycle index is out of bounds.
        """
        if node_name in self.state_cycle_results and 0 <= cycle < len(
            self.state_cycle_results[node_name]
        ):
            return self.state_cycle_results[node_name][cycle]
        return None
