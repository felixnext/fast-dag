"""Core DAG class definition."""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from ..core.context import Context
from ..core.exceptions import (
    CycleError,
    InvalidNodeError,
    ValidationError,
)
from ..core.node import Node
from ..core.types import NodeType
from ..core.validation import find_cycles, find_disconnected_nodes, find_entry_points

if TYPE_CHECKING:
    pass


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

    def __post_init__(self):
        """Initialize DAG after creation."""
        if self.context is None:
            self.context = Context()

    def __getitem__(self, key: str) -> Any:
        """Get node result by name.

        Supports dot notation for nested workflow results.
        """
        if "." in key:
            # Handle nested workflow results
            parts = key.split(".")
            node_name = parts[0]

            # Check if this is a nested workflow node
            if node_name in self.nodes and self.context:
                node = self.nodes[node_name]
                from ..core.types import NodeType

                # For nested DAG/FSM nodes, look up results in the nested context
                if node.node_type in (NodeType.DAG, NodeType.FSM):
                    # Try to get the nested context from metadata
                    nested_key = f"_nested_{node_name}_context"
                    if self.context.metadata.get(nested_key):
                        nested_context = self.context.metadata[nested_key]
                        # Get the nested node result
                        nested_node_name = ".".join(parts[1:])
                        if hasattr(nested_context, "get_result"):
                            return nested_context.get_result(nested_node_name)

                # For regular nodes, try dict navigation
                result = self.context.get_result(node_name)
                for part in parts[1:]:
                    if isinstance(result, dict) and part in result:
                        result = result[part]
                    else:
                        return None
                return result
            return None
        else:
            # Standard result access
            if self.context:
                return self.context.get_result(key)
            return None

    def __setitem__(self, key: str, value: Any) -> None:
        """Set node result by name."""
        if self.context:
            self.context.set_result(key, value)

    def __contains__(self, key: str) -> bool:
        """Check if node has a result in the context."""
        if self.context:
            return key in self.context
        return False

    def __len__(self) -> int:
        """Get number of nodes."""
        return len(self.nodes)

    def __iter__(self):
        """Iterate over node names."""
        return iter(self.nodes)

    def __str__(self) -> str:
        """String representation."""
        return f"DAG(name='{self.name}', nodes={len(self.nodes)})"

    def __repr__(self) -> str:
        """Detailed representation."""
        return (
            f"DAG(name='{self.name}', nodes={len(self.nodes)}, "
            f"description='{self.description}', validated={self._is_validated})"
        )

    def set_node_hooks(
        self,
        node_name: str,
        *,
        pre_execute: Callable[[Any, dict[str, Any]], None] | None = None,
        post_execute: Callable[[Any, dict[str, Any], Any], Any] | None = None,
        on_error: Callable[[Any, dict[str, Any], Exception], None] | None = None,
    ) -> None:
        """Set lifecycle hooks for a node.

        Args:
            node_name: Name of the node to set hooks for
            pre_execute: Hook called before node execution with (node, inputs)
            post_execute: Hook called after node execution with (node, inputs, result).
                         Can modify and return the result.
            on_error: Hook called on error with (node, inputs, exception)

        Raises:
            ValueError: If node doesn't exist
        """
        if node_name not in self.nodes:
            raise ValueError(f"Node '{node_name}' not found")

        node = self.nodes[node_name]

        # Set hooks (allow None to clear them)
        node.pre_execute = pre_execute
        node.post_execute = post_execute
        node.on_error = on_error

    def add_node(
        self,
        node: Node | str,
        func: Callable | None = None,
        *,
        inputs: list[str] | None = None,
        outputs: list[str] | None = None,
        description: str | None = None,
        node_type: NodeType = NodeType.STANDARD,
        retry: int | None = None,
        retry_delay: float | None = None,
        timeout: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "DAG":
        """Add a node to the DAG.

        Args:
            node: Node instance or name
            func: Function to execute (if node is a string)
            inputs: Input parameter names
            outputs: Output parameter names
            description: Node description
            node_type: Type of node
            retry: Number of retry attempts
            retry_delay: Delay between retries
            timeout: Execution timeout
            metadata: Additional metadata

        Returns:
            The added node
        """
        if isinstance(node, str):
            if func is None:
                raise InvalidNodeError("Function is required when adding node by name")

            node_obj = Node(
                func=func,
                name=node,
                inputs=inputs,
                outputs=outputs,
                description=description,
                node_type=node_type,
                retry=retry,
                retry_delay=retry_delay,
                timeout=timeout,
                metadata=metadata or {},
            )
        else:
            node_obj = node

        # Validate node name
        if not node_obj.name:
            raise InvalidNodeError("Node must have a name")

        if node_obj.name in self.nodes:
            raise ValueError(f"Node '{node_obj.name}' already exists")

        # Add to nodes
        self.nodes[node_obj.name] = node_obj

        # Clear cached properties
        self._invalidate_cache()

        return self  # Return self for chaining

    def get_node(self, name: str) -> Node:
        """Get a node by name.

        Args:
            name: Node name

        Returns:
            Node instance

        Raises:
            KeyError: If node doesn't exist
        """
        if name not in self.nodes:
            raise KeyError(f"Node '{name}' not found")
        return self.nodes[name]

    def get(self, key: str, default: Any = None) -> Any:
        """Get result for a node by name.

        Args:
            key: Node name
            default: Default value if result not found

        Returns:
            Result value or default
        """
        if self.context and key in self.context:
            return self.context.get_result(key)
        return default

    @property
    def results(self) -> dict[str, Any]:
        """Get all node results as a dictionary.

        Returns:
            Dictionary of node name to result value
        """
        if self.context:
            return dict(self.context.results)
        return {}

    def remove_node(self, name: str) -> None:
        """Remove a node from the DAG.

        Args:
            name: Node name

        Raises:
            KeyError: If node doesn't exist
        """
        if name not in self.nodes:
            raise KeyError(f"Node '{name}' not found")

        node = self.nodes[name]

        # Remove connections
        for other_node in self.nodes.values():
            if other_node != node:
                # Remove incoming connections
                for output_name in list(other_node.output_connections.keys()):
                    connections = other_node.output_connections[output_name]
                    other_node.output_connections[output_name] = [
                        (target_node, input_name)
                        for target_node, input_name in connections
                        if target_node != node
                    ]
                    # Remove empty connection lists
                    if not other_node.output_connections[output_name]:
                        del other_node.output_connections[output_name]

        # Remove from nodes
        del self.nodes[name]

        # Clear cached properties
        self._invalidate_cache()

    def clear_nodes(self) -> None:
        """Remove all nodes from the DAG."""
        self.nodes.clear()
        self._invalidate_cache()

    def _invalidate_cache(self) -> None:
        """Invalidate cached properties."""
        self._entry_points = None
        self._execution_order = None
        self._is_validated = False

    def get_entry_points(self) -> list[str]:
        """Get entry point nodes (nodes with no dependencies).

        Returns:
            List of entry point node names
        """
        if self._entry_points is None:
            self._entry_points = find_entry_points(self.nodes)
        return self._entry_points

    def get_execution_order(self) -> list[str]:
        """Get topological execution order.

        Returns:
            List of node names in execution order
        """
        if self._execution_order is None:
            self._execution_order = self._compute_execution_order()
        return self._execution_order

    def _compute_execution_order(self) -> list[str]:
        """Compute topological execution order."""
        # Kahn's algorithm for topological sorting
        in_degree = dict.fromkeys(self.nodes, 0)

        # Count incoming edges
        for node in self.nodes.values():
            for connections in node.output_connections.values():
                for target_node, _ in connections:
                    if target_node.name:
                        in_degree[target_node.name] += 1

        # Find nodes with no incoming edges
        queue = [name for name, degree in in_degree.items() if degree == 0]
        result = []

        while queue:
            current = queue.pop(0)
            result.append(current)

            # Remove edges from current node
            for connections in self.nodes[current].output_connections.values():
                for target_node, _ in connections:
                    if target_node.name:
                        in_degree[target_node.name] -= 1
                        if in_degree[target_node.name] == 0:
                            queue.append(target_node.name)

        if len(result) != len(self.nodes):
            raise CycleError("Cycle detected in DAG")

        return result

    def validate(
        self, allow_disconnected: bool = False, check_types: bool = False
    ) -> list[str]:
        """Validate the DAG structure.

        Args:
            allow_disconnected: If True, allow disconnected nodes
            check_types: If True, validate type compatibility between connections

        Returns:
            List of validation errors
        """
        errors = []

        # Check for empty DAG
        if not self.nodes:
            errors.append("DAG has no nodes")
            return errors

        # Check for cycles
        try:
            cycles = find_cycles(self.nodes)
            if cycles:
                errors.append(f"Cycles detected: {cycles}")
        except Exception as e:
            errors.append(f"Error checking cycles: {e}")

        # Check for disconnected nodes (less strict - allow kwargs)
        if not allow_disconnected:
            try:
                disconnected = find_disconnected_nodes(self.nodes)
                # Only report nodes that are truly disconnected
                # For a node to be considered connected, it must either:
                # 1. Have incoming connections (not be an entry point), OR
                # 2. Have outgoing connections, OR
                # 3. Be reachable from other entry points in the graph

                # First, find all nodes reachable from entry points that have connections
                entry_points = self.get_entry_points()
                connected_entries = [
                    ep
                    for ep in entry_points
                    if any(self.nodes[ep].output_connections.values())
                ]

                # Find all nodes reachable from connected entry points
                reachable = set()
                to_visit = list(connected_entries)
                while to_visit:
                    current = to_visit.pop(0)
                    if current in reachable:
                        continue
                    reachable.add(current)
                    # Add all nodes this one connects to
                    for connections in self.nodes[current].output_connections.values():
                        for target, _ in connections:
                            if target.name not in reachable:
                                to_visit.append(target.name)

                truly_disconnected = []
                for node_name in disconnected:
                    node = self.nodes[node_name]
                    has_connections = any(node.output_connections.values())
                    has_inputs = any(
                        node_name in [target.name for target, _ in connections]
                        for other_node in self.nodes.values()
                        for connections in other_node.output_connections.values()
                    )

                    # A node is disconnected if it's not reachable and has no connections
                    if (
                        node_name not in reachable
                        and not has_connections
                        and not has_inputs
                    ):
                        truly_disconnected.append(node_name)

                if truly_disconnected:
                    errors.append(f"Disconnected nodes: {truly_disconnected}")
            except Exception as e:
                errors.append(f"Error checking disconnected nodes: {e}")

        # Validate individual nodes
        for node_name, node in self.nodes.items():
            try:
                node_errors = node.validate()
                if node_errors:
                    errors.extend(
                        [f"Node '{node_name}': {error}" for error in node_errors]
                    )
            except Exception as e:
                errors.append(f"Error validating node '{node_name}': {e}")

        # Check for multiple connections to same input (not allowed for regular nodes)
        from ..core.types import NodeType

        for node_name, node in self.nodes.items():
            # Skip ANY/ALL nodes which allow multiple connections
            if node.node_type not in (NodeType.ANY, NodeType.ALL):
                # Check if this node appears multiple times as a target with the same input
                input_sources: dict[
                    str, list[str]
                ] = {}  # input_name -> list of sources

                # Check all nodes' output connections
                for source_name, source_node in self.nodes.items():
                    for (
                        _output_name,
                        connections,
                    ) in source_node.output_connections.items():
                        for target_node, input_name in connections:
                            if target_node.name == node_name:
                                if input_name not in input_sources:
                                    input_sources[input_name] = []
                                input_sources[input_name].append(source_name)

                # Check for multiple connections to same input
                for input_name, sources in input_sources.items():
                    if len(sources) > 1:
                        errors.append(
                            f"Node '{node_name}' has multiple connections to input '{input_name}' "
                            f"from {sources}. Use @dag.any() or @dag.all() decorators for multi-input convergence."
                        )

        # Check conditional nodes have both outputs connected
        for node_name, node in self.nodes.items():
            if node.node_type == NodeType.CONDITIONAL:
                outputs = node.output_connections
                # Check for either true/false or on_true/on_false outputs
                has_true = "true" in outputs and outputs["true"]
                has_on_true = "on_true" in outputs and outputs["on_true"]
                has_false = "false" in outputs and outputs["false"]
                has_on_false = "on_false" in outputs and outputs["on_false"]

                if not (has_true or has_on_true):
                    errors.append(
                        f"Conditional node '{node_name}': 'true' or 'on_true' output has no connection"
                    )
                if not (has_false or has_on_false):
                    errors.append(
                        f"Conditional node '{node_name}': 'false' or 'on_false' output has no connection"
                    )

        # Check type compatibility if requested
        if check_types:
            from typing import get_type_hints

            for node_name, node in self.nodes.items():
                # Get type hints for this node's function
                try:
                    hints = get_type_hints(node.func)
                except Exception:
                    # Skip if we can't get type hints
                    continue

                # Check each output connection
                for _output_name, connections in node.output_connections.items():
                    for target_node, input_name in connections:
                        # Get source output type
                        source_type = hints.get("return", None)
                        if source_type is None:
                            continue

                        # Get target input type
                        try:
                            target_hints = get_type_hints(target_node.func)
                            target_type = target_hints.get(input_name, None)
                            if target_type is None:
                                continue

                            # Check if types are compatible
                            # This is a simple check - just check if they're the same type
                            if source_type != target_type:
                                errors.append(
                                    f"Type mismatch: '{node_name}' outputs {source_type.__name__} "
                                    f"but '{target_node.name}' input '{input_name}' expects {target_type.__name__}"
                                )
                        except Exception:
                            # Skip if we can't check types
                            continue

        # Cache validation result
        self._is_validated = len(errors) == 0

        return errors

    @property
    def is_valid(self) -> bool:
        """Check if DAG is valid.

        Returns:
            True if DAG is valid
        """
        return len(self.validate()) == 0

    def is_acyclic(self) -> bool:
        """Check if DAG is acyclic.

        Returns:
            True if DAG has no cycles
        """
        try:
            cycles = find_cycles(self.nodes)
            return len(cycles) == 0
        except Exception:
            return False

    def is_fully_connected(self) -> bool:
        """Check if all nodes are connected.

        Returns:
            True if all nodes are connected
        """
        try:
            disconnected = find_disconnected_nodes(self.nodes)
            return len(disconnected) == 0
        except Exception:
            return False

    def validate_or_raise(self, allow_disconnected: bool = False) -> None:
        """Validate the DAG and raise ValidationError if invalid."""
        errors = self.validate(allow_disconnected=allow_disconnected)
        if errors:
            # Check for specific error types and raise appropriate exceptions
            from ..core.exceptions import (
                CycleError,
                DisconnectedNodeError,
                MissingConnectionError,
            )

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
    def entry_points(self) -> list[str]:
        """Get entry point nodes."""
        return self.get_entry_points()

    @property
    def execution_order(self) -> list[str]:
        """Get execution order."""
        return self.get_execution_order()

    @property
    def runner(self) -> Any:
        """Get or create the default runner for this DAG."""
        if self._runner is None:
            from typing import cast

            from ..runner import DAGRunner, ExecutionMode
            from . import DAG as FullDAG

            # Cast self to the full DAG type that runner expects
            self._runner = DAGRunner(
                dag=cast(FullDAG, self),
                mode=ExecutionMode.SEQUENTIAL,
                max_workers=4,
            )
        return self._runner

    def clear_cached_properties(self) -> None:
        """Clear cached properties."""
        self._invalidate_cache()

    def has_entry_points(self) -> bool:
        """Check if DAG has any entry points.

        Returns:
            True if DAG has at least one entry point
        """
        return len(self.get_entry_points()) > 0

    def can_connect(self, source: str, target: str, check_types: bool = True) -> bool:
        """Check if two nodes can be connected.

        Args:
            source: Source node name
            target: Target node name
            check_types: Whether to check type compatibility

        Returns:
            True if nodes can be connected
        """
        if source not in self.nodes or target not in self.nodes:
            return False

        source_node = self.nodes[source]
        target_node = self.nodes[target]

        # Check type compatibility if requested
        if check_types:
            from typing import get_type_hints

            try:
                # Get source return type
                source_hints = get_type_hints(source_node.func)
                source_type = source_hints.get("return", None)

                # Get target input types
                target_hints = get_type_hints(target_node.func)

                # For simplicity, check if the source return type matches any target input type
                # This is a basic check - more sophisticated type checking could be added
                if source_type and target_hints:
                    # Get the first non-context parameter type
                    for param_name, param_type in target_hints.items():
                        if param_name != "return" and param_name != "context":
                            # Basic type compatibility check
                            if source_type != param_type:
                                # Check for None/Optional compatibility
                                import typing

                                origin = getattr(param_type, "__origin__", None)
                                if origin is typing.Union:
                                    # Check if source type is in the union
                                    args = getattr(param_type, "__args__", ())
                                    if source_type not in args:
                                        return False
                                else:
                                    return False
                            break
            except Exception:
                # If we can't get type hints, assume compatible
                pass

        # Check if connection would create a cycle
        # Temporarily add the connection and check for cycles
        if "result" not in source_node.output_connections:
            source_node.output_connections["result"] = []
        source_node.output_connections["result"].append((target_node, "input"))

        # Check for cycles
        try:
            cycles = find_cycles(self.nodes)
            has_cycle = len(cycles) > 0
        except Exception:
            has_cycle = True

        # Remove temporary connection
        source_node.output_connections["result"] = [
            (node, input_name)
            for node, input_name in source_node.output_connections["result"]
            if node != target_node
        ]
        if not source_node.output_connections["result"]:
            del source_node.output_connections["result"]

        return not has_cycle

    def check_connection_issues(self) -> list[str]:
        """Check for connection issues in the DAG.

        Returns:
            List of connection issues
        """
        issues = []

        # Check for missing connections
        for node_name, node in self.nodes.items():
            if node.node_type == NodeType.CONDITIONAL:
                # Check if both branches are connected
                has_true = any(
                    "true" in connections or "on_true" in connections
                    for connections in node.output_connections
                )
                has_false = any(
                    "false" in connections or "on_false" in connections
                    for connections in node.output_connections
                )

                if not has_true:
                    issues.append(
                        f"Conditional node '{node_name}' missing true branch connection"
                    )
                if not has_false:
                    issues.append(
                        f"Conditional node '{node_name}' missing false branch connection"
                    )

        return issues
