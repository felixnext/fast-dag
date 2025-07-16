"""DAG (Directed Acyclic Graph) implementation."""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from .core.context import Context
from .core.exceptions import InvalidNodeError, ValidationError
from .core.node import Node
from .core.types import NodeType
from .core.validation import find_cycles, find_disconnected_nodes


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

    def node(
        self,
        func: Callable | None = None,
        *,
        name: str | None = None,
        inputs: list[str] | None = None,
        outputs: list[str] | None = None,
        description: str | None = None,
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
        from .core.validation import find_entry_points

        entry_points = set(find_entry_points(self.nodes))

        for node_name, node_obj in self.nodes.items():
            # Skip entry nodes - their inputs come from external sources
            if node_name in entry_points:
                continue

            for input_name in node_obj.inputs or []:
                if input_name not in node_obj.input_connections:
                    errors.append(
                        f"Node '{node_name}': input '{input_name}' has no connection"
                    )

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
        **kwargs: Any,  # noqa: ARG002
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
        # For now, just a placeholder
        self.context = context or Context()
        return None

    async def run_async(
        self,
        context: Context | None = None,
        **kwargs: Any,  # noqa: ARG002
    ) -> Any:
        """Execute the DAG asynchronously."""
        # Placeholder
        self.context = context or Context()
        return None

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
