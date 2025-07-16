"""DAG (Directed Acyclic Graph) implementation."""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from .core.context import Context
from .core.exceptions import (
    CycleError,
    DisconnectedNodeError,
    ExecutionError,
    InvalidNodeError,
    MissingConnectionError,
    ValidationError,
)
from .core.node import Node
from .core.types import ConditionalReturn, NodeType
from .core.validation import find_cycles, find_disconnected_nodes, find_entry_points

if TYPE_CHECKING:
    from .runner import DAGRunner


@dataclass
class DAG:
    """Directed Acyclic Graph workflow.

    A DAG represents a workflow where data flows from inputs through
    processing nodes to outputs without cycles.
    """

    name: str
    nodes: dict[str, Node] = field(default_factory=dict)
    description: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    # Cached properties
    _entry_points: list[str] | None = None
    _execution_order: list[str] | None = None
    _is_validated: bool = False

    # Runtime state
    context: Context | None = None
    _runner: Any = None  # Lazy-loaded DAGRunner

    def node(
        self,
        func: Callable | None = None,
        *,
        name: str | None = None,
        inputs: list[str] | None = None,
        outputs: list[str] | None = None,
        description: str | None = None,
        retry: int | None = None,
        timeout: float | None = None,
    ) -> Callable:
        """Decorator to add a function as a node in the DAG.

        Can be used as @dag.node or @dag.node(name="custom").
        """

        def decorator(f: Callable) -> Node:
            node = Node(
                func=f,
                name=name,
                inputs=inputs,
                outputs=outputs,
                description=description,
                node_type=NodeType.STANDARD,
                retry=retry,
                timeout=timeout,
            )
            self.add_node(node)
            return node

        if func is None:
            # Called with arguments: @dag.node(name="custom")
            return decorator  # type: ignore
        else:
            # Called without arguments: @dag.node
            return decorator(func)  # type: ignore

    def condition(
        self,
        func: Callable | None = None,
        *,
        name: str | None = None,
        inputs: list[str] | None = None,
        description: str | None = None,
    ) -> Callable:
        """Decorator to add a conditional function as a node in the DAG.

        Conditional nodes have 'true' and 'false' outputs.
        """

        def decorator(f: Callable) -> Node:
            node = Node(
                func=f,
                name=name,
                inputs=inputs,
                outputs=["true", "false"],  # Fixed outputs for conditional
                description=description,
                node_type=NodeType.CONDITIONAL,
            )
            self.add_node(node)
            return node

        if func is None:
            return decorator  # type: ignore
        else:
            return decorator(func)  # type: ignore

    def add_node(self, node_or_name: Node | str, func: Callable | None = None) -> "DAG":
        """Add a node to the DAG.

        Can be called as:
        - dag.add_node(node)  # With a Node object
        - dag.add_node("name", func)  # With name and function

        Returns self for chaining.
        """
        if isinstance(node_or_name, Node):
            node = node_or_name
        elif isinstance(node_or_name, str) and func is not None:
            # Create node from name and function
            node = Node(func=func, name=node_or_name)
        else:
            raise InvalidNodeError(
                "add_node requires either a Node object or (name, func) arguments"
            )

        if node.name is None:
            raise ValueError("Node must have a name")

        if node.name in self.nodes:
            raise ValueError(f"Node with name '{node.name}' already exists")

        # Validate node immediately
        node_errors = node.validate()
        if node_errors:
            raise ValidationError(
                f"Invalid node '{node.name}':\n"
                + "\n".join(f"  - {error}" for error in node_errors)
            )

        self.nodes[node.name] = node
        self._invalidate_cache()
        return self  # For chaining

    def connect(
        self,
        source: str,
        target: str,
        output: str | None = None,
        input: str | None = None,
    ) -> None:
        """Connect two nodes in the DAG."""
        if source not in self.nodes:
            raise ValueError(f"Source node '{source}' not found")
        if target not in self.nodes:
            raise ValueError(f"Target node '{target}' not found")

        source_node = self.nodes[source]
        target_node = self.nodes[target]

        source_node.connect_to(target_node, output=output, input=input)
        self._invalidate_cache()

    def can_connect(
        self,
        source: str,
        target: str,
        output: str | None = None,  # noqa: ARG002
        input: str | None = None,  # noqa: ARG002
    ) -> bool:
        """Check if two nodes can be connected."""
        # For now, just check that nodes exist
        # TODO: Add type checking when implemented
        return source in self.nodes and target in self.nodes

    def validate(
        self,
        allow_disconnected: bool = False,
        check_types: bool = False,
    ) -> list[str]:
        """Validate the DAG structure.

        Returns a list of validation errors, empty if valid.
        """
        errors = []

        # Check for cycles
        cycles = find_cycles(self.nodes)
        if cycles:
            for cycle in cycles:
                errors.append(f"Cycle detected: {' -> '.join(cycle)}")

        # Check for disconnected nodes
        if not allow_disconnected:
            disconnected = find_disconnected_nodes(self.nodes)
            if disconnected:
                for node in disconnected:
                    errors.append(f"Node '{node}' is disconnected from the graph")

        # Check each node's validation
        for node_obj in self.nodes.values():
            node_errors = node_obj.validate()
            errors.extend(node_errors)

        # Check that all required inputs have connections
        # Entry nodes (with no input connections) are allowed to have unconnected inputs
        # Note: Non-entry nodes can also receive some inputs from kwargs (parameters)
        # so we don't require ALL inputs to have connections

        # Check for missing required connections
        entry_nodes = find_entry_points(self.nodes)
        for node_name, node_obj in self.nodes.items():
            if node_name not in entry_nodes and node_obj.inputs:
                # Non-entry nodes should have connections for their inputs
                for input_name in node_obj.inputs:
                    if input_name not in node_obj.input_connections:
                        # This input has no connection - might be OK if provided via kwargs
                        # but we should warn about it
                        errors.append(
                            f"Node '{node_name}' missing connection for input '{input_name}'"
                        )

        # Check for multiple connections to the same input port
        # NOTE: This is currently disabled as it breaks legitimate use cases
        # where multiple nodes connect to the same input (OR semantics)
        # This needs more thought on how to handle properly
        # input_connections_count: dict[tuple[str, str], int] = {}
        # for node_name, node_obj in self.nodes.items():
        #     for output_name, connections in node_obj.output_connections.items():
        #         for target_node, input_name in connections:
        #             key = (target_node.name if target_node.name else "", input_name)
        #             input_connections_count[key] = input_connections_count.get(key, 0) + 1
        #
        # # Report any inputs with multiple connections
        # for (node_name, input_name), count in input_connections_count.items():
        #     if count > 1:
        #         errors.append(
        #             f"Node '{node_name}' input '{input_name}' has multiple connections ({count}). "
        #             "Only one connection per input is allowed."
        #         )

        # Check conditional nodes have both branches connected
        for node_name, node_obj in self.nodes.items():
            if node_obj.node_type == NodeType.CONDITIONAL:
                # Check both true and false outputs have connections
                outputs = node_obj.output_connections
                if "true" not in outputs or not outputs["true"]:
                    errors.append(
                        f"Conditional node '{node_name}': 'true' output has no connection"
                    )
                if "false" not in outputs or not outputs["false"]:
                    errors.append(
                        f"Conditional node '{node_name}': 'false' output has no connection"
                    )

        # Type checking if requested
        if check_types:
            # This would require more sophisticated type analysis
            pass

        self._is_validated = len(errors) == 0
        return errors

    def validate_or_raise(
        self,
        allow_disconnected: bool = False,
        check_types: bool = False,
    ) -> None:
        """Validate the DAG and raise an exception if invalid."""
        errors = self.validate(
            allow_disconnected=allow_disconnected,
            check_types=check_types,
        )
        if errors:
            # Check for specific error types and raise appropriate exceptions
            for error in errors:
                error_lower = error.lower()
                if "cycle" in error_lower:
                    raise CycleError(error)
                elif "disconnected" in error_lower:
                    raise DisconnectedNodeError(error)
                elif "missing" in error_lower and "connection" in error_lower:
                    raise MissingConnectionError(error)

            # If no specific error type matched, raise generic ValidationError
            raise ValidationError(
                f"DAG validation failed with {len(errors)} errors:\n"
                + "\n".join(f"  - {error}" for error in errors)
            )

    @property
    def is_valid(self) -> bool:
        """Check if the DAG is valid."""
        return len(self.validate(allow_disconnected=True)) == 0

    def is_acyclic(self) -> bool:
        """Check if the DAG has no cycles."""
        from .core.validation import find_cycles

        cycles = find_cycles(self.nodes)
        return len(cycles) == 0

    def is_fully_connected(self) -> bool:
        """Check if all nodes are connected to the graph."""
        from .core.validation import find_disconnected_nodes

        disconnected = find_disconnected_nodes(self.nodes)
        return len(disconnected) == 0

    def has_entry_points(self) -> bool:
        """Check if the DAG has at least one entry point."""
        from .core.validation import find_entry_points

        entry_points = find_entry_points(self.nodes)
        return len(entry_points) > 0

    @property
    def entry_points(self) -> list[str]:
        """Get the entry point nodes (nodes with no input connections)."""
        if self._entry_points is None:
            from .core.validation import find_entry_points

            self._entry_points = find_entry_points(self.nodes)
        return self._entry_points

    @property
    def execution_order(self) -> list[str]:
        """Get the topological execution order of nodes."""
        if self._execution_order is None:
            from .core.validation import topological_sort

            self._execution_order = topological_sort(self.nodes)
        return self._execution_order

    def _invalidate_cache(self) -> None:
        """Invalidate cached properties when the DAG changes."""
        self._entry_points = None
        self._execution_order = None
        self._is_validated = False

    def run(
        self,
        context: Context | None = None,
        mode: str = "sequential",  # noqa: ARG002
        error_strategy: str = "stop",  # noqa: ARG002
        **kwargs: Any,
    ) -> Any:
        """Execute the DAG.

        Args:
            context: Execution context (created if not provided)
            mode: Execution mode (sequential, parallel, async)
            error_strategy: How to handle errors (stop, continue, retry)
            **kwargs: Input values for entry nodes

        Returns:
            The result from the final node(s)
        """
        # Initialize context
        self.context = context or Context()

        # Validate DAG before execution
        errors = self.validate(allow_disconnected=True)
        if errors:
            # Check for specific error types
            for error in errors:
                error_lower = error.lower()
                if "cycle" in error_lower:
                    raise CycleError(f"Cannot execute DAG with cycles: {error}")
                elif "missing connection" in error_lower:
                    raise MissingConnectionError(
                        f"Cannot execute DAG with missing connections: {error}"
                    )
            raise ValidationError(f"Cannot execute invalid DAG: {errors}")

        # Get execution order
        exec_order = self.execution_order

        # Find nodes that need external inputs
        entry_nodes = self.entry_points

        # Execute nodes in topological order
        last_result = None

        for node_name in exec_order:
            node = self.nodes[node_name]

            # Prepare inputs for the node
            node_inputs = {}

            if node_name in entry_nodes:
                # Entry node - get inputs from kwargs
                for input_name in node.inputs or []:
                    if input_name in kwargs:
                        node_inputs[input_name] = kwargs[input_name]
                    elif input_name == "context":
                        continue  # Context is handled separately
                    else:
                        # Check if it's a no-argument function
                        if node.inputs:
                            raise ValueError(
                                f"Entry node '{node_name}' missing required input: '{input_name}'"
                            )
            else:
                # Non-entry node - get inputs from connections and kwargs
                skip_node = False

                # First, check for any inputs that might come from kwargs
                for input_name in node.inputs or []:
                    if (
                        input_name in kwargs
                        and input_name not in node.input_connections
                    ):
                        node_inputs[input_name] = kwargs[input_name]

                # Then get inputs from connections
                for input_name, (
                    source_node,
                    output_name,
                ) in node.input_connections.items():
                    source_name = source_node.name
                    if source_name is None:
                        raise ExecutionError("Source node has no name")
                    if source_name not in self.context:
                        if error_strategy == "continue":
                            # Skip this node if its dependency failed
                            skip_node = True
                            break
                        raise ExecutionError(
                            f"Node '{node_name}' requires result from '{source_name}' which hasn't executed"
                        )

                    source_result = self.context[source_name]

                    # Handle output selection for multi-output nodes
                    if isinstance(source_result, dict) and output_name in source_result:
                        node_inputs[input_name] = source_result[output_name]
                    elif isinstance(source_result, ConditionalReturn):
                        # Handle conditional returns
                        if (
                            output_name == "true"
                            and source_result.condition
                            or output_name == "false"
                            and not source_result.condition
                        ):
                            node_inputs[input_name] = source_result.value
                        else:
                            # Skip this node if on wrong branch
                            skip_node = True
                            break
                    else:
                        # Single output node
                        node_inputs[input_name] = source_result

                if skip_node:
                    continue  # Skip to next node

            # Execute the node
            try:
                if node.is_async:
                    raise NotImplementedError(
                        "Async execution not yet supported in sync run()"
                    )

                result = node.execute(node_inputs, context=self.context)

                # Store result in context
                self.context.set_result(node_name, result)
                last_result = result

            except Exception as e:
                if error_strategy == "stop":
                    # Re-raise common exceptions as-is to preserve type
                    if isinstance(e, RuntimeError | ValueError | TypeError | KeyError):
                        raise
                    raise ExecutionError(
                        f"Error executing node '{node_name}': {e}"
                    ) from e
                elif error_strategy == "continue":
                    # Log error and continue
                    self.context.metadata[f"{node_name}_error"] = str(e)
                    continue
                else:
                    raise ValueError(f"Unknown error strategy: {error_strategy}") from e

        # Return the last result (or could return results from all sink nodes)
        return last_result

    async def run_async(
        self,
        context: Context | None = None,
        mode: str = "sequential",  # noqa: ARG002
        error_strategy: str = "stop",  # noqa: ARG002
        **kwargs: Any,
    ) -> Any:
        """Execute the DAG asynchronously.

        Args:
            context: Execution context (created if not provided)
            mode: Execution mode (only sequential supported for async)
            error_strategy: How to handle errors (stop, continue, retry)
            **kwargs: Input values for entry nodes

        Returns:
            The result from the final node(s)
        """
        # Initialize context
        self.context = context or Context()

        # Validate DAG before execution
        errors = self.validate(allow_disconnected=True)
        if errors:
            # Check for specific error types
            for error in errors:
                error_lower = error.lower()
                if "cycle" in error_lower:
                    raise CycleError(f"Cannot execute DAG with cycles: {error}")
                elif "missing connection" in error_lower:
                    raise MissingConnectionError(
                        f"Cannot execute DAG with missing connections: {error}"
                    )
            raise ValidationError(f"Cannot execute invalid DAG: {errors}")

        # Get execution order
        exec_order = self.execution_order

        # Find nodes that need external inputs
        entry_nodes = self.entry_points

        # Execute nodes in topological order
        last_result = None

        for node_name in exec_order:
            node = self.nodes[node_name]

            # Prepare inputs for the node
            node_inputs = {}

            if node_name in entry_nodes:
                # Entry node - get inputs from kwargs
                for input_name in node.inputs or []:
                    if input_name in kwargs:
                        node_inputs[input_name] = kwargs[input_name]
                    elif input_name == "context":
                        continue  # Context is handled separately
                    else:
                        # Check if it's a no-argument function
                        if node.inputs:
                            raise ValueError(
                                f"Entry node '{node_name}' missing required input: '{input_name}'"
                            )
            else:
                # Non-entry node - get inputs from connections and kwargs
                skip_node = False

                # First, check for any inputs that might come from kwargs
                for input_name in node.inputs or []:
                    if (
                        input_name in kwargs
                        and input_name not in node.input_connections
                    ):
                        node_inputs[input_name] = kwargs[input_name]

                # Then get inputs from connections
                for input_name, (
                    source_node,
                    output_name,
                ) in node.input_connections.items():
                    source_name = source_node.name
                    if source_name is None:
                        raise ExecutionError("Source node has no name")
                    if source_name not in self.context:
                        if error_strategy == "continue":
                            # Skip this node if its dependency failed
                            skip_node = True
                            break
                        raise ExecutionError(
                            f"Node '{node_name}' requires result from '{source_name}' which hasn't executed"
                        )

                    source_result = self.context[source_name]

                    # Handle output selection for multi-output nodes
                    if isinstance(source_result, dict) and output_name in source_result:
                        node_inputs[input_name] = source_result[output_name]
                    elif isinstance(source_result, ConditionalReturn):
                        # Handle conditional returns
                        if (
                            output_name == "true"
                            and source_result.condition
                            or output_name == "false"
                            and not source_result.condition
                        ):
                            node_inputs[input_name] = source_result.value
                        else:
                            # Skip this node if on wrong branch
                            skip_node = True
                            break
                    else:
                        # Single output node
                        node_inputs[input_name] = source_result

                if skip_node:
                    continue  # Skip to next node

            # Execute the node
            try:
                if node.is_async:
                    result = await node.execute_async(node_inputs, context=self.context)
                else:
                    result = node.execute(node_inputs, context=self.context)

                # Store result in context
                self.context.set_result(node_name, result)
                last_result = result

            except Exception as e:
                if error_strategy == "stop":
                    # Re-raise common exceptions as-is to preserve type
                    if isinstance(e, RuntimeError | ValueError | TypeError | KeyError):
                        raise
                    raise ExecutionError(
                        f"Error executing node '{node_name}': {e}"
                    ) from e
                elif error_strategy == "continue":
                    # Log error and continue
                    self.context.metadata[f"{node_name}_error"] = str(e)
                    continue
                else:
                    raise ValueError(f"Unknown error strategy: {error_strategy}") from e

        # Return the last result
        return last_result

    def get(self, node_name: str, default: Any = None) -> Any:
        """Get a result from the context."""
        if self.context is None:
            return default
        return self.context.get(node_name, default)

    def __getitem__(self, key: str) -> Any:
        """Dict-like access to results."""
        if self.context is None:
            raise KeyError(f"No execution context available, key '{key}' not found")
        return self.context[key]

    def __contains__(self, key: str) -> bool:
        """Check if a result exists."""
        if self.context is None:
            return False
        return key in self.context

    @property
    def results(self) -> dict[str, Any]:
        """Get all execution results."""
        if self.context is None:
            return {}
        return self.context.results

    def step(
        self, context: Context | None = None, **kwargs: Any
    ) -> tuple[Context, Any]:
        """Execute a single step of the DAG.

        This executes the next available node in topological order
        that has all its dependencies satisfied.

        Args:
            context: Current execution context (created if not provided)
            **kwargs: Input values for entry nodes

        Returns:
            Tuple of (updated_context, step_result)
            Returns (context, None) when no more nodes can be executed
        """
        # Initialize or use provided context
        if context is None:
            context = Context()
        self.context = context

        # Validate DAG before execution
        errors = self.validate(allow_disconnected=True)
        if errors:
            # Check for specific error types
            for error in errors:
                error_lower = error.lower()
                if "cycle" in error_lower:
                    raise CycleError(f"Cannot execute DAG with cycles: {error}")
                elif "missing connection" in error_lower:
                    raise MissingConnectionError(
                        f"Cannot execute DAG with missing connections: {error}"
                    )
            raise ValidationError(f"Cannot execute invalid DAG: {errors}")

        # Get execution order
        exec_order = self.execution_order

        # Find entry nodes
        entry_nodes = self.entry_points

        # Find next node to execute
        for node_name in exec_order:
            # Skip if already executed
            if node_name in context.results:
                continue

            node = self.nodes[node_name]

            # Check if all dependencies are satisfied
            can_execute = True
            skip_node = False

            if node_name in entry_nodes:
                # Entry node - can always execute if not done yet
                pass
            else:
                # Check input connections
                for _input_name, (
                    source_node,
                    output_name,
                ) in node.input_connections.items():
                    source_name = source_node.name
                    if source_name is None:
                        raise ExecutionError("Source node has no name")

                    if source_name not in context:
                        # Dependency not satisfied
                        can_execute = False
                        break

                    # Check for conditional branches
                    source_result = context.get(source_name)
                    if (
                        source_result
                        and isinstance(source_result, ConditionalReturn)
                        and (
                            output_name == "true"
                            and not source_result.condition
                            or output_name == "false"
                            and source_result.condition
                        )
                    ):
                        # Wrong branch - skip this node
                        skip_node = True
                        break

            if skip_node:
                # Mark as skipped
                context.set_result(node_name, None)
                continue

            if not can_execute:
                # Dependencies not ready
                continue

            # Execute this node
            node_inputs = {}

            if node_name in entry_nodes:
                # Entry node - get inputs from kwargs
                for input_name in node.inputs or []:
                    if input_name in kwargs:
                        node_inputs[input_name] = kwargs[input_name]
                    elif input_name == "context":
                        continue
                    else:
                        if node.inputs:
                            raise ValueError(
                                f"Entry node '{node_name}' missing required input: '{input_name}'"
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
                    source_result = context[source_name]

                    # Handle output selection
                    if isinstance(source_result, dict) and output_name in source_result:
                        node_inputs[input_name] = source_result[output_name]
                    elif isinstance(source_result, ConditionalReturn):
                        node_inputs[input_name] = source_result.value
                    else:
                        node_inputs[input_name] = source_result

            # Execute the node
            if node.is_async:
                raise NotImplementedError(
                    "Async execution not supported in sync step()"
                )

            result = node.execute(node_inputs, context=context)

            # Store result
            context.set_result(node_name, result)

            # Return the result
            return context, result

        # No more nodes to execute
        return context, None

    @property
    def runner(self) -> "DAGRunner":
        """Get or create a runner for this DAG."""
        if self._runner is None:
            from .runner import DAGRunner

            self._runner = DAGRunner(self)
        return self._runner
