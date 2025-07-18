"""FSM state builder and decorator functionality."""

from collections.abc import Callable

from ..core.node import Node
from ..core.types import NodeType
from .core import CoreFSM


class FSMBuilder(CoreFSM):
    """FSM builder functionality with state decorators."""

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
                # Store in metadata for validation later
                if "initial_states" not in self.metadata:
                    self.metadata["initial_states"] = []
                self.metadata["initial_states"].append(node.name)

                # Always set the initial state - last one wins
                self.initial_state = node.name

            # Add to terminal states if marked
            if terminal and node.name is not None:
                self.terminal_states.add(node.name)

            return node

        if func is None:
            return decorator  # type: ignore
        else:
            return decorator(func)  # type: ignore
