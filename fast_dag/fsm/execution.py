"""FSM execution functionality."""

from typing import Any

from ..core.context import Context, FSMContext
from ..core.exceptions import ExecutionError, ValidationError
from ..core.types import FSMReturn
from .builder import FSMBuilder


class FSMExecutor(FSMBuilder):
    """FSM execution functionality."""

    def run(
        self,
        *,
        inputs: dict[str, Any] | None = None,
        context: Context | None = None,
        mode: str = "sequential",  # noqa: ARG002
        max_workers: int | None = None,  # noqa: ARG002
        error_strategy: str | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute the FSM.

        Args:
            inputs: Input values for the initial state
            context: FSM execution context (created if not provided)
            mode: Execution mode (only sequential supported for FSM)
            max_workers: Not used (for API compatibility)
            error_strategy: How to handle errors (stop, continue)
            **kwargs: Additional input values for the initial state

        Returns:
            The final result from the last executed state
        """
        # Merge inputs with kwargs
        all_inputs = {}
        if inputs:
            all_inputs.update(inputs)
        all_inputs.update(kwargs)

        # Use error_strategy if provided
        if error_strategy is None:
            error_strategy = "stop"

        # Validate FSM structure (allow multiple initial states by default for runtime)
        errors = self.validate(allow_multiple_initial=True)
        if errors:
            raise ValidationError(f"FSM validation failed: {errors}")

        # Initialize or convert context to FSMContext
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

        # Set initial state
        if self.initial_state is None:
            raise ExecutionError("FSM must have an initial state")

        self.current_state = self.initial_state

        # Track initial inputs
        for key, value in all_inputs.items():
            self.context.results[key] = value

        last_result = None

        # Execute FSM cycles
        while self.current_state:
            # Check if we've exceeded max cycles before executing
            if self.context.cycle_count >= self.max_cycles:
                # Don't increment when we've hit the limit
                break

            # Get current state node
            if self.current_state not in self.nodes:
                raise ExecutionError(f"State '{self.current_state}' not found in FSM")

            node = self.nodes[self.current_state]

            # Record state transition
            self.context.state_history.append(self.current_state)

            try:
                # Prepare node inputs
                node_inputs = {}

                # Get inputs from node connections or initial inputs
                if self.context.cycle_count == 0:
                    # For the first cycle, use initial inputs
                    for input_name in node.inputs or []:
                        if input_name in self.context.results:
                            node_inputs[input_name] = self.context.results[input_name]
                        elif input_name == "context":
                            continue  # Context is handled separately
                        else:
                            if node.inputs:
                                raise ExecutionError(
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

                # Execute node with current context
                if node.is_async:
                    # Handle async nodes in sync execution
                    import asyncio

                    try:
                        # Try to get existing event loop
                        asyncio.get_running_loop()
                        # Can't use run_until_complete in a running loop
                        raise ExecutionError(
                            f"Cannot execute async state '{self.current_state}' in sync mode while event loop is running. Use run_async() instead."
                        )
                    except RuntimeError:
                        # No event loop, create one
                        result = asyncio.run(
                            node.execute_async(node_inputs, context=self.context)
                        )
                else:
                    result = node.execute(node_inputs, context=self.context)

                # Store result in context
                self.context.set_result(self.current_state, result)

                # Store result
                last_result = result.value if isinstance(result, FSMReturn) else result

                # Store in cycle results using the proper method
                if isinstance(result, FSMReturn):
                    self.context.add_cycle_result(self.current_state, result.value)
                else:
                    self.context.add_cycle_result(self.current_state, result)

                # Update main results with latest value
                if node.name:
                    if isinstance(result, FSMReturn):
                        self.context.results[node.name] = result.value
                    else:
                        self.context.results[node.name] = result

            except Exception:
                # For FSM, we let the original exception propagate
                # This allows proper error handling in the calling code
                raise

            # Determine next state and whether to continue
            will_continue = True
            explicit_stop = False

            if isinstance(result, FSMReturn):
                if result.stop:
                    # Mark as terminated but keep current state
                    self._terminated = True
                    will_continue = False
                    explicit_stop = True
                elif result.next_state:
                    self.current_state = result.next_state
                else:
                    # No next_state specified, check if we're in a terminal state
                    if self.is_terminal_state(self.current_state):
                        will_continue = False
            else:
                # Check if current state is terminal
                if self.is_terminal_state(self.current_state):
                    will_continue = False
                else:
                    # Use transition table if available
                    if (
                        self.current_state in self.state_transitions
                        and "default" in self.state_transitions[self.current_state]
                    ):
                        self.current_state = self.state_transitions[self.current_state][
                            "default"
                        ]
                    else:
                        # No transition available, end execution
                        will_continue = False

            # Increment cycle count after execution
            if will_continue:
                # Continuing to next iteration
                self.context.cycle_count += 1
            elif explicit_stop and self.context.cycle_count > 0:
                # Explicit stop after looping - don't increment
                pass
            else:
                # Terminal state or first execution - do increment
                self.context.cycle_count += 1

            # Break if not continuing
            if not will_continue:
                break

        # Check if we hit max cycles and raise error
        if self.context.cycle_count >= self.max_cycles and not self._terminated:
            self.context.metadata["max_cycles_reached"] = True
            raise ExecutionError(
                f"Maximum cycles ({self.max_cycles}) exceeded in FSM '{self.name}'"
            )

        return last_result

    async def run_async(
        self,
        *,
        inputs: dict[str, Any] | None = None,
        context: Context | None = None,
        mode: str = "sequential",  # noqa: ARG002
        max_workers: int | None = None,  # noqa: ARG002
        error_strategy: str | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute the FSM asynchronously.

        Args:
            context: FSM execution context (created if not provided)
            mode: Execution mode (only sequential supported for FSM)
            error_strategy: How to handle errors (stop, continue)
            **kwargs: Input values for the initial state

        Returns:
            The final result from the last executed state
        """
        # Merge inputs with kwargs
        all_inputs = {}
        if inputs:
            all_inputs.update(inputs)
        all_inputs.update(kwargs)

        # Use error_strategy if provided
        if error_strategy is None:
            error_strategy = "stop"

        # Validate FSM structure (allow multiple initial states by default for runtime)
        errors = self.validate(allow_multiple_initial=True)
        if errors:
            raise ValidationError(f"FSM validation failed: {errors}")

        # Initialize or convert context to FSMContext
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

        # Set initial state
        if self.initial_state is None:
            raise ExecutionError("FSM must have an initial state")

        self.current_state = self.initial_state

        # Track initial inputs
        for key, value in all_inputs.items():
            self.context.results[key] = value

        last_result = None

        # Execute FSM cycles
        while self.current_state:
            # Check if we've exceeded max cycles before executing
            if self.context.cycle_count >= self.max_cycles:
                # Don't increment when we've hit the limit
                break

            # Get current state node
            if self.current_state not in self.nodes:
                raise ExecutionError(f"State '{self.current_state}' not found in FSM")

            node = self.nodes[self.current_state]

            # Record state transition
            self.context.state_history.append(self.current_state)

            try:
                # Prepare node inputs
                node_inputs = {}

                # Get inputs from node connections or initial inputs
                if self.context.cycle_count == 0:
                    # For the first cycle, use initial inputs
                    for input_name in node.inputs or []:
                        if input_name in self.context.results:
                            node_inputs[input_name] = self.context.results[input_name]
                        elif input_name == "context":
                            continue  # Context is handled separately
                        else:
                            if node.inputs:
                                raise ExecutionError(
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

                # Execute node with current context (async)
                if node.is_async:
                    result = await node.execute_async(node_inputs, context=self.context)
                else:
                    result = node.execute(node_inputs, context=self.context)

                # Store result in context
                self.context.set_result(self.current_state, result)

                # Store result
                last_result = result.value if isinstance(result, FSMReturn) else result

                # Store in cycle results using the proper method
                if isinstance(result, FSMReturn):
                    self.context.add_cycle_result(self.current_state, result.value)
                else:
                    self.context.add_cycle_result(self.current_state, result)

                # Update main results with latest value
                if node.name:
                    if isinstance(result, FSMReturn):
                        self.context.results[node.name] = result.value
                    else:
                        self.context.results[node.name] = result

            except Exception:
                # For FSM, we let the original exception propagate
                # This allows proper error handling in the calling code
                raise

            # Determine next state and whether to continue
            will_continue = True
            explicit_stop = False

            if isinstance(result, FSMReturn):
                if result.stop:
                    # Mark as terminated but keep current state
                    self._terminated = True
                    will_continue = False
                    explicit_stop = True
                elif result.next_state:
                    self.current_state = result.next_state
                else:
                    # No next_state specified, check if we're in a terminal state
                    if self.is_terminal_state(self.current_state):
                        will_continue = False
            else:
                # Check if current state is terminal
                if self.is_terminal_state(self.current_state):
                    will_continue = False
                else:
                    # Use transition table if available
                    if (
                        self.current_state in self.state_transitions
                        and "default" in self.state_transitions[self.current_state]
                    ):
                        self.current_state = self.state_transitions[self.current_state][
                            "default"
                        ]
                    else:
                        # No transition available, end execution
                        will_continue = False

            # Increment cycle count after execution
            if will_continue:
                # Continuing to next iteration
                self.context.cycle_count += 1
            elif explicit_stop and self.context.cycle_count > 0:
                # Explicit stop after looping - don't increment
                pass
            else:
                # Terminal state or first execution - do increment
                self.context.cycle_count += 1

            # Break if not continuing
            if not will_continue:
                break

        # Check if we hit max cycles and raise error
        if self.context.cycle_count >= self.max_cycles and not self._terminated:
            self.context.metadata["max_cycles_reached"] = True
            raise ExecutionError(
                f"Maximum cycles ({self.max_cycles}) exceeded in FSM '{self.name}'"
            )

        return last_result

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

        # Set initial state if first step
        if self.current_state is None:
            if self.initial_state is None:
                raise ExecutionError("FSM must have an initial state")
            self.current_state = self.initial_state

            # Track initial inputs
            for key, value in kwargs.items():
                self.context.results[key] = value

        # Check if already terminated
        if self.is_terminated:
            return self.context, None

        # Check if we have a current state
        if self.current_state is None:
            raise ExecutionError("FSM has no current state")

        # Get current state node
        if self.current_state not in self.nodes:
            raise ExecutionError(f"State '{self.current_state}' not found in FSM")

        node = self.nodes[self.current_state]

        # Record state transition
        self.context.state_history.append(self.current_state)
        self.context.cycle_count += 1

        # Prepare node inputs
        node_inputs = {}

        # Get inputs from node connections or initial inputs
        if self.context.cycle_count == 1:
            # For the first cycle, use initial inputs
            for input_name in node.inputs or []:
                if input_name in self.context.results:
                    node_inputs[input_name] = self.context.results[input_name]
                elif input_name == "context":
                    continue  # Context is handled separately
                else:
                    if node.inputs:
                        raise ExecutionError(
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

        # Execute node with current context
        if node.is_async:
            # Handle async nodes in sync execution
            import asyncio

            try:
                # Try to get existing event loop
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # Can't use run_until_complete in a running loop
                    raise ExecutionError(
                        f"Cannot execute async state '{self.current_state}' in sync mode while event loop is running. Use run_async() instead."
                    )
                result = loop.run_until_complete(
                    node.execute_async(node_inputs, context=self.context)
                )
            except RuntimeError:
                # No event loop, create one
                result = asyncio.run(
                    node.execute_async(node_inputs, context=self.context)
                )
        else:
            result = node.execute(node_inputs, context=self.context)

        # Store result in context
        self.context.set_result(self.current_state, result)

        # Store in cycle results using the proper method
        if isinstance(result, FSMReturn):
            self.context.add_cycle_result(self.current_state, result.value)
        else:
            self.context.add_cycle_result(self.current_state, result)

        # Update main results with latest value
        if node.name:
            if isinstance(result, FSMReturn):
                self.context.results[node.name] = result.value
            else:
                self.context.results[node.name] = result

        # Determine next state
        if isinstance(result, FSMReturn):
            if result.stop:
                # Mark as terminated but keep current state
                self._terminated = True
            elif result.next_state:
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

        # Return the value from FSMReturn, not the whole object
        if isinstance(result, FSMReturn):
            return self.context, result.value
        else:
            return self.context, result
