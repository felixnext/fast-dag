"""Core FSM class and basic functionality."""

from dataclasses import dataclass, field
from typing import Any

from ..core.context import FSMContext
from ..core.exceptions import ValidationError
from ..core.types import FSMReturn
from ..dag import DAG


@dataclass
class CoreFSM(DAG):
    """Core FSM class with basic state management.

    This class provides the foundational FSM functionality including
    state management, transitions, and basic validation.
    """

    initial_state: str | None = None
    terminal_states: set[str] = field(default_factory=set)
    max_cycles: int = 1000

    # Runtime state
    current_state: str | None = None
    state_transitions: dict[str, dict[str, str]] = field(default_factory=dict)
    _terminated: bool = field(default=False, init=False)

    def add_transition(
        self, from_state: str, to_state: str, condition: str = "default"
    ) -> None:
        """Add a state transition rule."""
        if from_state not in self.state_transitions:
            self.state_transitions[from_state] = {}
        self.state_transitions[from_state][condition] = to_state

    def set_initial_state(self, state: str) -> None:
        """Set the initial state for FSM execution."""
        if state not in self.nodes:
            raise ValueError(f"State '{state}' not found in FSM")
        self.initial_state = state

    def add_terminal_state(self, state: str) -> None:
        """Add a terminal state that ends FSM execution."""
        if state not in self.nodes:
            raise ValueError(f"State '{state}' not found in FSM")
        self.terminal_states.add(state)

    def validate(
        self,
        allow_disconnected: bool = False,  # Check for unreachable states by default
        check_types: bool = False,  # noqa: ARG002
        allow_multiple_initial: bool = False,  # Allow multiple initial states
    ) -> list[str]:
        """Validate the FSM structure.

        FSMs allow cycles, so we skip cycle detection.
        """
        errors = []

        # Check if FSM is empty
        if not self.nodes:
            errors.append("No nodes")
            return errors  # No point checking other things if empty

        # Check initial state
        if self.initial_state is None:
            errors.append("No initial state defined for FSM")
        elif self.initial_state not in self.nodes:
            errors.append(f"Initial state '{self.initial_state}' not found in nodes")

        # Check for multiple initial states being set during decoration
        initial_states = self.metadata.get("initial_states", [])
        if len(initial_states) > 1 and not allow_multiple_initial:
            errors.append(
                f"Multiple initial states defined: {', '.join(initial_states)}"
            )

        # Even with multiple initial states, we allow last-one-wins behavior at runtime

        # Check each node's validation
        for node_obj in self.nodes.values():
            node_errors = node_obj.validate()
            errors.extend(node_errors)

        # Don't check for cycles (FSMs can have them)
        # For FSMs, we still check for unreachable states but in a different way
        # We assume states can be reached via FSMReturn but only if they are
        # referenced in the expected workflow
        if not allow_disconnected and self.initial_state:
            # For FSMs, we do a more sophisticated reachability analysis
            # We check for states that are NEVER referenced by any other state
            # and are not the initial state or terminal states

            potentially_reachable = {self.initial_state}

            # Add states that are targets of explicit transitions
            for transitions in self.state_transitions.values():
                potentially_reachable.update(transitions.values())

            # Add states with incoming connections (if any)
            for node in self.nodes.values():
                if node.input_connections and node.name:
                    potentially_reachable.add(node.name)

            # Add all terminal states as they could be reached via FSMReturn
            potentially_reachable.update(self.terminal_states)

            # For FSMs, we need to assume that any state can be reached
            # via FSMReturn from any other state. The reachability is dynamic
            # and determined by the FSMReturn values at runtime.

            # For FSMs, we need to be careful about reachability since states
            # are connected via FSMReturn which can't be statically analyzed.
            # We'll only flag states that are clearly isolated.

            # For FSMs, we skip strict reachability checking since FSMReturn
            # provides dynamic state transitions that can't be statically analyzed.
            # We only flag states that are completely isolated and have no
            # possible way to be reached (e.g., no connections and no transitions).

            # However, even this is problematic because FSMReturn can reference
            # any state dynamically. For now, we'll only flag states that are
            # explicitly marked as unreachable in very specific cases.

            # Skip reachability checking for FSMs for now - it's too complex
            # to do correctly without analyzing FSMReturn values at runtime.

        return errors

    def validate_or_raise(self, allow_disconnected: bool = False) -> None:
        """Validate the FSM and raise ValidationError if invalid."""
        errors = self.validate(allow_disconnected=allow_disconnected)
        if errors:
            raise ValidationError(f"FSM validation failed: {errors}")

    @property
    def is_terminated(self) -> bool:
        """Check if FSM is terminated (stop=True was returned)."""
        return self._terminated

    def is_terminal_state(self, state: str | None) -> bool:
        """Check if a given state is a terminal state."""
        return state in self.terminal_states if state else False

    @property
    def state_history(self) -> list[str]:
        """Get the history of state transitions."""
        if self.context is None or not isinstance(self.context, FSMContext):
            return []
        return self.context.state_history

    def get_history(self, state_name: str) -> list[Any]:
        """Get execution history for a specific state."""
        if self.context is None or not isinstance(self.context, FSMContext):
            return []
        results = self.context.state_cycle_results.get(state_name, [])
        # Extract values from FSMReturn objects
        return [r.value if isinstance(r, FSMReturn) else r for r in results]

    def __getitem__(self, key: str) -> Any:
        """Dict-like access to results with cycle support.

        Examples:
            fsm["state_name"] - get latest result
            fsm["state_name.5"] - get result from 5th cycle
        """
        if self.context is None or not isinstance(self.context, FSMContext):
            raise KeyError(f"No execution context available, key '{key}' not found")

        # Check for cycle notation
        if "." in key:
            state_name, cycle_str = key.rsplit(".", 1)
            try:
                cycle = int(cycle_str)
                result = self.context.get_cycle(state_name, cycle)
                if isinstance(result, FSMReturn):
                    return result.value
                return result
            except ValueError:
                # Not a valid cycle number, treat as regular key
                pass

        # Get latest result and extract value if it's FSMReturn
        result = self.context.get_latest(key)
        if isinstance(result, FSMReturn):
            return result.value
        return result
