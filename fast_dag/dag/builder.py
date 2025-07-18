"""DAG building and connection functionality."""

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

from ..core.exceptions import InvalidNodeError, MissingConnectionError
from ..core.node import Node
from ..core.types import NodeType

if TYPE_CHECKING:
    from .core import DAG


class DAGLike(Protocol):
    """Protocol for DAG-like objects."""

    nodes: dict[str, Node]

    def get_node(self, name: str) -> Node: ...
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
    ) -> "DAG": ...
    def _invalidate_cache(self) -> None: ...


T = TypeVar("T", bound=DAGLike)


class DAGBuilder:
    """Mixin class for DAG building functionality."""

    # Type hints for attributes from DAG
    nodes: dict[str, Node]

    def get_node(self, name: str) -> Node:
        """Get node by name - defined in DAG."""
        raise NotImplementedError

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
        """Add node - defined in DAG."""
        raise NotImplementedError

    def _invalidate_cache(self) -> None:
        """Invalidate cache - defined in DAG."""
        raise NotImplementedError

    def connect(
        self,
        source: str | Node,
        target: str | Node,
        *,
        output: str = "result",
        input: str | None = None,
    ) -> None:
        """Connect two nodes in the DAG.

        Args:
            source: Source node name or instance
            target: Target node name or instance
            output: Output name from source node
            input: Input name for target node (auto-detected if None)
        """
        # Resolve node names
        source_name = source if isinstance(source, str) else source.name
        target_name = target if isinstance(target, str) else target.name

        if not source_name or not target_name:
            raise InvalidNodeError("Source and target nodes must have names")

        # Get node instances
        source_node = self.get_node(source_name)
        target_node = self.get_node(target_name)

        # Auto-detect input parameter name if not provided
        if input is None:
            input = self._auto_detect_input(target_node, source_node, output)

        # Add connection
        if output not in source_node.output_connections:
            source_node.output_connections[output] = []

        source_node.output_connections[output].append((target_node, input))

        # Also update target node's input_connections
        # For ANY/ALL nodes, use multi_input_connections to allow multiple sources per input
        if target_node.node_type in (NodeType.ANY, NodeType.ALL):
            if input not in target_node.multi_input_connections:
                target_node.multi_input_connections[input] = []
            target_node.multi_input_connections[input].append((source_node, output))
            # Also update regular input_connections with the last connection for compatibility
            target_node.input_connections[input] = (source_node, output)
        else:
            target_node.input_connections[input] = (source_node, output)

        # Clear cached properties
        self._invalidate_cache()

    def _auto_detect_input(
        self,
        target_node: Node,
        source_node: Node,  # noqa: ARG002
        output: str,  # noqa: ARG002
    ) -> str:
        """Auto-detect input parameter name for connection."""
        # Get function signature
        try:
            import inspect

            sig = inspect.signature(target_node.func)
            params = list(sig.parameters.keys())

            # Skip 'self' and 'context' parameters
            params = [p for p in params if p not in ("self", "context")]

            if not params:
                raise MissingConnectionError(
                    f"Target node '{target_node.name}' has no input parameters"
                )

            # Try to match by output name first
            if output in params:
                return output

            # Try to match by parameter name containing output
            for param in params:
                if output in param or param in output:
                    return param

            # Default to first parameter
            return params[0]

        except Exception as e:
            raise MissingConnectionError(
                f"Could not auto-detect input parameter for '{target_node.name}': {e}"
            ) from e

    def disconnect(
        self, source: str | Node, target: str | Node, output: str = "result"
    ) -> None:
        """Disconnect two nodes.

        Args:
            source: Source node name or instance
            target: Target node name or instance
            output: Output name from source node
        """
        # Resolve node names
        source_name = source if isinstance(source, str) else source.name
        target_name = target if isinstance(target, str) else target.name

        if not source_name or not target_name:
            raise InvalidNodeError("Source and target nodes must have names")

        # Get node instances
        source_node = self.get_node(source_name)
        target_node = self.get_node(target_name)

        # Remove connection
        if output in source_node.output_connections:
            # Find the input name used for this connection
            input_name = None
            for node, inp in source_node.output_connections[output]:
                if node == target_node:
                    input_name = inp
                    break

            source_node.output_connections[output] = [
                (node, inp)
                for node, inp in source_node.output_connections[output]
                if node != target_node
            ]

            # Remove empty connection lists
            if not source_node.output_connections[output]:
                del source_node.output_connections[output]

            # Also remove from target node's input_connections
            if input_name and input_name in target_node.input_connections:
                del target_node.input_connections[input_name]

        # Clear cached properties
        self._invalidate_cache()

    def __rshift__(self: T, other: "DAG") -> T:  # noqa: ARG002
        """Support for >> operator to connect DAGs."""
        # This would be used for chaining DAGs
        # For now, just return self
        return self

    def node(
        self,
        func: Callable | None = None,
        *,
        name: str | None = None,
        inputs: list[str] | None = None,
        outputs: list[str] | None = None,
        description: str | None = None,
        retry: int | None = None,
        retry_delay: float | None = None,
        timeout: float | None = None,
    ) -> Callable:
        """Decorator to add a function as a node in the DAG.

        Can be used as @dag.node or @dag.node(name="custom").
        """

        def decorator(f: Callable) -> Node:
            # Check if a node with this name already exists (from @cached_node)
            func_name = name or f.__name__
            if func_name in self.nodes:
                existing_node = self.nodes[func_name]
                # Only allow updating if the existing node is for the same function
                # This supports @cached_node followed by @dag.node
                if existing_node.func == f:
                    # Same function, just update attributes
                    if inputs is not None:
                        existing_node.inputs = inputs
                    if outputs is not None:
                        existing_node.outputs = outputs
                    if description is not None:
                        existing_node.description = description
                    if retry is not None:
                        existing_node.retry = retry
                    if retry_delay is not None:
                        existing_node.retry_delay = retry_delay
                    if timeout is not None:
                        existing_node.timeout = timeout
                    return existing_node
                else:
                    # Different function with same name - raise error
                    raise ValueError(f"Node '{func_name}' already exists")
            else:
                node = Node(
                    func=f,
                    name=name,
                    inputs=inputs,
                    outputs=outputs,
                    description=description,
                    node_type=NodeType.STANDARD,
                    retry=retry,
                    retry_delay=retry_delay,
                    timeout=timeout,
                )

                # Check if the function has caching attributes from @cached_node
                if hasattr(f, "_cached") and f._cached:
                    node.cached = True
                    node.cache_backend = getattr(f, "_cache_backend", "memory")
                    node.cache_ttl = getattr(f, "_cache_ttl", None)

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
                outputs=["true", "false"],
                description=description,
                node_type=NodeType.CONDITIONAL,
            )
            self.add_node(node)
            return node

        if func is None:
            return decorator  # type: ignore
        else:
            return decorator(func)  # type: ignore

    def select(
        self,
        func: Callable | None = None,
        *,
        name: str | None = None,
        inputs: list[str] | None = None,
        outputs: list[str] | None = None,
        description: str | None = None,
    ) -> Callable:
        """Decorator to add a select function as a node in the DAG.

        Select nodes route data to different outputs based on conditions.
        """

        def decorator(f: Callable) -> Node:
            node = Node(
                func=f,
                name=name,
                inputs=inputs,
                outputs=outputs or ["result"],
                description=description,
                node_type=NodeType.SELECT,
            )
            self.add_node(node)
            return node

        if func is None:
            return decorator  # type: ignore
        else:
            return decorator(func)  # type: ignore

    def any(
        self,
        func: Callable | None = None,
        *,
        name: str | None = None,
        inputs: list[str] | None = None,
        outputs: list[str] | None = None,
        description: str | None = None,
    ) -> Callable:
        """Decorator to add an ANY node (executes when any input is available)."""

        def decorator(f: Callable) -> Node:
            node = Node(
                func=f,
                name=name,
                inputs=inputs,
                outputs=outputs or ["result"],
                description=description,
                node_type=NodeType.ANY,
            )
            self.add_node(node)
            return node

        if func is None:
            return decorator  # type: ignore
        else:
            return decorator(func)  # type: ignore

    def any_node(
        self,
        func: Callable | None = None,
        *,
        name: str | None = None,
        inputs: list[str] | None = None,
        outputs: list[str] | None = None,
        description: str | None = None,
    ) -> Callable:
        """Decorator to add an ANY node (executes when any input is available)."""
        return self.any(
            func, name=name, inputs=inputs, outputs=outputs, description=description
        )

    def all(
        self,
        func: Callable | None = None,
        *,
        name: str | None = None,
        inputs: list[str] | None = None,
        outputs: list[str] | None = None,
        description: str | None = None,
    ) -> Callable:
        """Decorator to add an ALL node (executes when all inputs are available)."""

        def decorator(f: Callable) -> Node:
            node = Node(
                func=f,
                name=name,
                inputs=inputs,
                outputs=outputs or ["result"],
                description=description,
                node_type=NodeType.ALL,
            )
            self.add_node(node)
            return node

        if func is None:
            return decorator  # type: ignore
        else:
            return decorator(func)  # type: ignore

    def all_node(
        self,
        func: Callable | None = None,
        *,
        name: str | None = None,
        inputs: list[str] | None = None,
        outputs: list[str] | None = None,
        description: str | None = None,
    ) -> Callable:
        """Decorator to add an ALL node (executes when all inputs are available)."""
        return self.all(
            func, name=name, inputs=inputs, outputs=outputs, description=description
        )

    def cached_node(
        self,
        func: Callable | None = None,
        *,
        backend: str = "memory",
        cache_ttl: float | None = None,
        key_func: Callable | None = None,  # noqa: ARG002
    ) -> Callable:
        """Decorator to add caching to a node.

        Can be used as:
        - @dag.cached_node
        - @dag.cached_node()
        - @dag.cached_node(cache_ttl=60)

        Args:
            func: Function to decorate (when used without parentheses)
            backend: Cache backend to use
            cache_ttl: Time-to-live for cached results
            key_func: Function to generate cache keys

        Returns:
            Decorator function or decorated function
        """

        def decorator(f: Callable) -> Callable:
            # Check if function is already a node in the DAG
            node_name = getattr(f, "__name__", None) or getattr(f, "name", None)
            if node_name and node_name in self.nodes:
                # Update existing node with caching properties
                existing_node = self.nodes[node_name]
                existing_node.cached = True
                existing_node.cache_backend = backend
                existing_node.cache_ttl = cache_ttl
                return f
            else:
                # Set caching attributes on the function for @dag.node to pick up
                if hasattr(f, "__name__"):
                    f._cached = True  # type: ignore[attr-defined]
                    f._cache_backend = backend  # type: ignore[attr-defined]
                    f._cache_ttl = cache_ttl  # type: ignore[attr-defined]

                # Create a cached node when used as standalone decorator
                node = Node(
                    func=f,
                    name=node_name,
                    cached=True,
                    cache_backend=backend,
                    cache_ttl=cache_ttl,
                )
                self.add_node(node)

                return f

        # Handle being called with or without parentheses
        if func is not None:
            # Called as @dag.cached_node
            return decorator(func)
        else:
            # Called as @dag.cached_node() or @dag.cached_node(ttl=60)
            return decorator
