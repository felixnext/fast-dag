"""FSM (Finite State Machine) implementation."""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from .core.context import Context, FSMContext
from .core.exceptions import ExecutionError, InvalidNodeError, ValidationError
from .core.node import Node
from .core.types import FSMReturn, NodeType
from .dag import DAG


@dataclass
class FSM(DAG):
    """Finite State Machine workflow.

    FSM extends DAG to support cycles and state-based execution.
    Unlike DAGs, FSMs can have cycles and execute nodes multiple times.
    """

    initial_state: str | None = None
    terminal_states: set[str] = field(default_factory=set)
    max_cycles: int = 1000

    # Runtime state
    current_state: str | None = None
    state_transitions: dict[str, dict[str, str]] = field(default_factory=dict)

    def state(
        self,
        func: Callable | None = None,
        *,
        name: str | None = None,
        inputs: list[str] | None = None,
        outputs: list[str] | None = None,
        description: str | None = None,
        initial: bool = False,
        terminal: bool = False,
    ) -> Callable:
        """Decorator to add a function as a state node in the FSM.

        State nodes are similar to regular nodes but designed for FSM workflows.

        Args:
            func: The function to wrap as a state
            name: Override the state name (defaults to function name)
            inputs: Override input parameter names
            outputs: Override output names
            description: State description
            initial: Mark this as the initial state
            terminal: Mark this as a terminal state
        """

        def decorator(f: Callable) -> Node:
            node = Node(
                func=f,
                name=name,
                inputs=inputs,
                outputs=outputs,
                description=description,
                node_type=NodeType.FSM_STATE,
            )
            self.add_node(node)

            # Set initial state if marked
            if initial:
                self.initial_state = node.name
                # Store in metadata for validation later
                if "initial_states" not in self.metadata:
                    self.metadata["initial_states"] = []
                self.metadata["initial_states"].append(node.name)

            # Add to terminal states if marked
            if terminal and node.name is not None:
                self.terminal_states.add(node.name)

            return node

        if func is None:
            return decorator  # type: ignore
        else:
            return decorator(func)  # type: ignore

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
    ) -> list[str]:
        """Validate the FSM structure.

        FSMs allow cycles, so we skip cycle detection.
        """
        errors = []

        # Check initial state
        if self.initial_state is None:
            errors.append("FSM must have an initial state")
        elif self.initial_state not in self.nodes:
            errors.append(f"Initial state '{self.initial_state}' not found in nodes")

        # Check for multiple initial states
        if "initial_states" in self.metadata:
            initial_states = self.metadata["initial_states"]
            if len(initial_states) > 1:
                errors.append(
                    f"FSM has multiple initial states: {', '.join(initial_states)}. "
                    "Only one initial state is allowed."
                )

        # Check each node's validation
        for node_obj in self.nodes.values():
            node_errors = node_obj.validate()
            errors.extend(node_errors)

        # Don't check for cycles (FSMs can have them)
        # But do check for unreachable states unless explicitly allowed
        if not allow_disconnected and self.initial_state:
            # For FSMs, we can't determine reachability statically since
            # transitions are defined dynamically via FSMReturn.
            # However, we can check if states have ANY way to be reached.
            # A state is potentially reachable if:
            # 1. It's the initial state
            # 2. It's mentioned in state_transitions
            # 3. It has incoming connections (though FSMs typically don't use these)

            potentially_reachable = {self.initial_state}

            # Add states that are targets of transitions
            for transitions in self.state_transitions.values():
                potentially_reachable.update(transitions.values())

            # Add states with incoming connections (if any)
            for node in self.nodes.values():
                if node.input_connections and node.name:
                    potentially_reachable.add(node.name)

            # Check for unreachable states
            for state_name in self.nodes:
                if state_name not in potentially_reachable:
                    errors.append(
                        f"State '{state_name}' is unreachable from initial state"
                    )

        return errors

    def run(
        self,
        context: Context | None = None,
        mode: str = "sequential",  # noqa: ARG002
        error_strategy: str = "stop",
        **kwargs: Any,
    ) -> Any:
        """Execute the FSM.

        Args:
            context: FSM execution context (created if not provided)
            mode: Execution mode (only sequential supported for FSM)
            error_strategy: How to handle errors (stop, continue)
            **kwargs: Input values for the initial state

        Returns:
            The final result from FSM execution
        """
        # Initialize context - convert to FSMContext if needed
        if context is None:
            self.context = FSMContext()
        elif isinstance(context, FSMContext):
            self.context = context
        else:
            # Convert regular Context to FSMContext
            fsm_context = FSMContext()
            fsm_context.results = context.results
            fsm_context.metadata = context.metadata
            fsm_context.metrics = context.metrics
            self.context = fsm_context

        # Validate FSM before execution
        errors = self.validate()
        if errors:
            raise ValidationError(f"Cannot execute invalid FSM: {errors}")

        if self.initial_state is None:
            raise ExecutionError("FSM has no initial state")

        # Initialize state
        self.current_state = self.initial_state
        last_result = None

        # Execute FSM
        while self.context.cycle_count < self.max_cycles:
            # Terminal states should still be executed
            # The stop signal will break the loop after execution

            # Record state in history
            self.context.state_history.append(self.current_state)

            # Get current node
            node = self.nodes[self.current_state]

            # Prepare inputs
            node_inputs = {}

            if (
                self.context.cycle_count == 0
                and self.current_state == self.initial_state
            ):
                # First execution of initial state - use kwargs
                for input_name in node.inputs or []:
                    if input_name in kwargs:
                        node_inputs[input_name] = kwargs[input_name]
                    elif input_name == "context":
                        continue  # Context is handled separately
                    else:
                        if node.inputs:
                            raise ValueError(
                                f"Initial state '{self.current_state}' missing required input: '{input_name}'"
                            )
            else:
                # Get inputs from connections
                for input_name, (
                    source_node,
                    output_name,
                ) in node.input_connections.items():
                    source_name = source_node.name
                    if source_name is None:
                        raise ExecutionError("Source node has no name")

                    # Try to get latest result first
                    result = self.context.get_latest(source_name)
                    if result is None and source_name not in self.context:
                        raise ExecutionError(
                            f"State '{self.current_state}' requires result from '{source_name}' which hasn't executed"
                        )

                    # Handle output selection
                    if isinstance(result, dict) and output_name in result:
                        node_inputs[input_name] = result[output_name]
                    else:
                        node_inputs[input_name] = result

            # Execute the node
            try:
                result = node.execute(node_inputs, context=self.context)

                # Store result in context
                self.context.set_result(self.current_state, result)

                # Store in cycle results
                if self.current_state not in self.context.cycle_results:
                    self.context.cycle_results[self.current_state] = []
                self.context.cycle_results[self.current_state].append(result)

                last_result = result

                # Increment cycle count before processing transitions
                self.context.cycle_count += 1

                # Extract value and determine next state
                if isinstance(result, FSMReturn):
                    last_result = result.value  # Extract the value
                    if result.stop:
                        break

                    if result.next_state:
                        # Validate that the next state exists
                        if result.next_state not in self.nodes:
                            raise InvalidNodeError(
                                f"State '{self.current_state}' attempted to transition to "
                                f"non-existent state '{result.next_state}'"
                            )
                        self.current_state = result.next_state
                    else:
                        # Stay in current state
                        pass
                else:
                    # Use transition table if available
                    if self.current_state in self.state_transitions:
                        # Default transition
                        if "default" in self.state_transitions[self.current_state]:
                            self.current_state = self.state_transitions[
                                self.current_state
                            ]["default"]
                        else:
                            # No transition defined, stay in current state
                            pass

            except Exception as e:
                if error_strategy == "stop":
                    # Re-raise InvalidNodeError as-is
                    if isinstance(e, InvalidNodeError):
                        raise
                    raise ExecutionError(
                        f"Error executing state '{self.current_state}': {e}"
                    ) from e
                elif error_strategy in ("continue", "continue_none", "continue_skip"):
                    # Log error and try to continue
                    self.context.metadata[f"{self.current_state}_error"] = str(e)

                    # For continue_none, store None as the result
                    if error_strategy == "continue_none":
                        self.context.set_result(self.current_state, None)
                        if self.current_state not in self.context.cycle_results:
                            self.context.cycle_results[self.current_state] = []
                        self.context.cycle_results[self.current_state].append(None)

                    # Move to error state if defined
                    if (
                        self.current_state in self.state_transitions
                        and "error" in self.state_transitions[self.current_state]
                    ):
                        self.current_state = self.state_transitions[self.current_state][
                            "error"
                        ]
                    else:
                        break
                else:
                    raise ValueError(f"Unknown error strategy: {error_strategy}") from e

        # Check if we hit max cycles
        if self.context.cycle_count >= self.max_cycles:
            self.context.metadata["max_cycles_reached"] = True

        return last_result

    async def run_async(
        self,
        context: Context | None = None,
        mode: str = "sequential",  # noqa: ARG002
        error_strategy: str = "stop",  # noqa: ARG002
        **kwargs: Any,
    ) -> Any:
        """Execute the FSM asynchronously.

        Args:
            context: FSM execution context (created if not provided)
            mode: Execution mode (only sequential supported for FSM)
            error_strategy: How to handle errors (stop, continue)
            **kwargs: Input values for the initial state

        Returns:
            The final result from FSM execution
        """
        # Initialize context - convert to FSMContext if needed
        if context is None:
            self.context = FSMContext()
        elif isinstance(context, FSMContext):
            self.context = context
        else:
            # Convert regular Context to FSMContext
            fsm_context = FSMContext()
            fsm_context.results = context.results
            fsm_context.metadata = context.metadata
            fsm_context.metrics = context.metrics
            self.context = fsm_context

        # Validate FSM before execution
        errors = self.validate()
        if errors:
            raise ValidationError(f"Cannot execute invalid FSM: {errors}")

        if self.initial_state is None:
            raise ExecutionError("FSM has no initial state")

        # Initialize state
        self.current_state = self.initial_state
        last_result = None

        # Execute FSM
        while self.context.cycle_count < self.max_cycles:
            # Terminal states should still be executed
            # The stop signal will break the loop after execution

            # Record state in history
            self.context.state_history.append(self.current_state)

            # Get current node
            node = self.nodes[self.current_state]

            # Prepare inputs
            node_inputs = {}

            if (
                self.context.cycle_count == 0
                and self.current_state == self.initial_state
            ):
                # First execution of initial state - use kwargs
                for input_name in node.inputs or []:
                    if input_name in kwargs:
                        node_inputs[input_name] = kwargs[input_name]
                    elif input_name == "context":
                        continue
                    else:
                        if node.inputs:
                            raise ValueError(
                                f"Initial state '{self.current_state}' missing required input: '{input_name}'"
                            )
            else:
                # Get inputs from connections
                for input_name, (
                    source_node,
                    output_name,
                ) in node.input_connections.items():
                    source_name = source_node.name
                    if source_name is None:
                        raise ExecutionError("Source node has no name")

                    # Try to get latest result first
                    result = self.context.get_latest(source_name)
                    if result is None and source_name not in self.context:
                        raise ExecutionError(
                            f"State '{self.current_state}' requires result from '{source_name}' which hasn't executed"
                        )

                    # Handle output selection
                    if isinstance(result, dict) and output_name in result:
                        node_inputs[input_name] = result[output_name]
                    else:
                        node_inputs[input_name] = result

            # Execute the node
            if node.is_async:
                result = await node.execute_async(node_inputs, context=self.context)
            else:
                result = node.execute(node_inputs, context=self.context)

            # Store result in context
            self.context.set_result(self.current_state, result)

            # Store in cycle results
            if self.current_state not in self.context.cycle_results:
                self.context.cycle_results[self.current_state] = []
            self.context.cycle_results[self.current_state].append(result)

            last_result = result

            # Increment cycle count before processing transitions
            self.context.cycle_count += 1

            # Extract value and determine next state
            if isinstance(result, FSMReturn):
                last_result = result.value  # Extract the value
                if result.stop:
                    break

                if result.next_state:
                    self.current_state = result.next_state
            else:
                # Use transition table if available
                if (
                    self.current_state in self.state_transitions
                    and "default" in self.state_transitions[self.current_state]
                ):
                    self.current_state = self.state_transitions[self.current_state][
                        "default"
                    ]

        # Check if we hit max cycles
        if self.context.cycle_count >= self.max_cycles:
            self.context.metadata["max_cycles_reached"] = True

        return last_result

    def get_history(self, state_name: str) -> list[Any]:
        """Get execution history for a specific state."""
        if self.context is None or not isinstance(self.context, FSMContext):
            return []
        results = self.context.cycle_results.get(state_name, [])
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

    @property
    def is_terminated(self) -> bool:
        """Check if FSM has reached a terminal state."""
        return (
            self.current_state in self.terminal_states if self.current_state else False
        )

    def is_terminal_state(self, state: str | None) -> bool:
        """Check if a given state is a terminal state."""
        return state in self.terminal_states if state else False

    @property
    def state_history(self) -> list[str]:
        """Get the history of state transitions."""
        if self.context is None or not isinstance(self.context, FSMContext):
            return []
        return self.context.state_history

    def step(
        self, context: Context | None = None, **kwargs: Any
    ) -> tuple[Context, Any]:
        """Execute a single step of the FSM.

        Args:
            context: Current FSM context (created if not provided)
            **kwargs: Input values for the initial state (only used on first step)

        Returns:
            Tuple of (updated_context, step_result)
        """
        # Initialize or use provided context - convert to FSMContext if needed
        if context is None:
            fsm_context = FSMContext()
        elif isinstance(context, FSMContext):
            fsm_context = context
        else:
            # Convert regular Context to FSMContext
            fsm_context = FSMContext()
            fsm_context.results = context.results
            fsm_context.metadata = context.metadata
            fsm_context.metrics = context.metrics
        self.context = fsm_context

        # Validate FSM before execution
        errors = self.validate()
        if errors:
            raise ValidationError(f"Cannot execute invalid FSM: {errors}")

        if self.initial_state is None:
            raise ExecutionError("FSM has no initial state")

        # Determine current state
        if self.current_state is None:
            # First step - initialize
            self.current_state = self.initial_state
            is_first_step = True
        else:
            is_first_step = False

        # Check if we're trying to execute a state that already returned stop=True
        if self.current_state in fsm_context.results:
            last_result = fsm_context.results[self.current_state]
            if isinstance(last_result, FSMReturn) and last_result.stop:
                return fsm_context, None

        # Check cycle limit
        if fsm_context.cycle_count >= self.max_cycles:
            raise ExecutionError(f"Maximum cycles ({self.max_cycles}) exceeded")

        # Record state in history
        fsm_context.state_history.append(self.current_state)

        # Get current node
        node = self.nodes[self.current_state]

        # Prepare inputs
        node_inputs = {}

        if is_first_step:
            # First execution - use kwargs
            for input_name in node.inputs or []:
                if input_name in kwargs:
                    node_inputs[input_name] = kwargs[input_name]
                elif input_name == "context":
                    continue  # Context is handled separately
                else:
                    if node.inputs:
                        raise ValueError(
                            f"Initial state '{self.current_state}' missing required input: '{input_name}'"
                        )
        else:
            # Get inputs from connections
            for input_name, (
                source_node,
                output_name,
            ) in node.input_connections.items():
                source_name = source_node.name
                if source_name is None:
                    raise ExecutionError("Source node has no name")

                # Try to get latest result first
                result = fsm_context.get_latest(source_name)
                if result is None and source_name not in fsm_context:
                    raise ExecutionError(
                        f"State '{self.current_state}' requires result from '{source_name}' which hasn't executed"
                    )

                # Handle output selection
                if isinstance(result, dict) and output_name in result:
                    node_inputs[input_name] = result[output_name]
                else:
                    node_inputs[input_name] = result

        # Execute the node
        result = node.execute(node_inputs, context=fsm_context)

        # Store result in context
        fsm_context.set_result(self.current_state, result)

        # Store in cycle results
        if self.current_state not in fsm_context.cycle_results:
            fsm_context.cycle_results[self.current_state] = []
        fsm_context.cycle_results[self.current_state].append(result)

        # Determine next state and extract value
        value_to_return = result
        if isinstance(result, FSMReturn):
            value_to_return = result.value
            if result.stop:
                # Mark as terminated - but after returning the value
                # We'll check terminal state next time
                pass
            elif result.next_state:
                self.current_state = result.next_state
            # else stay in current state
        else:
            # Use transition table if available
            if (
                self.current_state in self.state_transitions
                and "default" in self.state_transitions[self.current_state]
            ):
                self.current_state = self.state_transitions[self.current_state][
                    "default"
                ]
                # else stay in current state

        # Increment cycle count
        fsm_context.cycle_count += 1

        # Don't change current_state after stop=True
        # This allows the test to inspect which state was terminal

        return fsm_context, value_to_return
