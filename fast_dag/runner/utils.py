"""Runner utility functions."""

from typing import Any

from ..core.context import Context
from ..core.exceptions import ExecutionError
from ..core.types import ConditionalReturn
from .core import CoreDAGRunner


class RunnerUtils(CoreDAGRunner):
    """Runner utility functions."""

    def _should_skip_node(self, node_name: str, context: Context) -> bool:
        """Check if node should be skipped based on conditional branches."""
        node = self.dag.nodes[node_name]

        # For ANY nodes, never skip - they can execute with partial inputs
        from ..core.types import NodeType

        if node.node_type == NodeType.ANY:
            return False

        # Check if this node depends on a conditional directly
        for _input_name, (source_node, output_name) in node.input_connections.items():
            source_name = source_node.name
            if source_name and source_name in context:
                source_result = context[source_name]

                if isinstance(source_result, ConditionalReturn) and (
                    (output_name == "true" or output_name == "on_true")
                    and not source_result.condition
                    or (output_name == "false" or output_name == "on_false")
                    and source_result.condition
                ):
                    return True

        # Check if ALL required dependencies are available
        # If any dependency is missing and not an entry node, skip this node
        for _input_name, (source_node, _) in node.input_connections.items():
            source_name = source_node.name
            if source_name and source_name not in context:
                # Check if the missing dependency is on a different conditional branch
                # by checking if it was skipped
                return True

        return False

    def _prepare_node_inputs(
        self,
        node_name: str,
        node: Any,
        context: Context,
        entry_nodes: list[str],
        kwargs: dict[str, Any],
    ) -> dict[str, Any]:
        """Prepare inputs for node execution."""
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
            # Non-entry node - get inputs from connections
            from ..core.types import ConditionalReturn, NodeType

            # Handle ANY nodes - execute if ANY input is available
            if node.node_type == NodeType.ANY:
                # For ANY nodes, check if we have any available input
                any_input_available = False

                # If the node has only one input parameter, use that for any source
                if node.inputs and len(node.inputs) == 1:
                    single_input_name = node.inputs[0]

                    # Check all connections for an available source
                    # Use multi_input_connections if available, otherwise fall back to input_connections
                    connections = []
                    if (
                        hasattr(node, "multi_input_connections")
                        and single_input_name in node.multi_input_connections
                    ):
                        connections = node.multi_input_connections[single_input_name]
                    elif single_input_name in node.input_connections:
                        connections = [
                            (
                                node.input_connections[single_input_name][0],
                                node.input_connections[single_input_name][1],
                            )
                        ]

                    for source_node, output_name in connections:
                        source_name = source_node.name
                        if source_name is None:
                            continue
                        if source_name in context:
                            source_result = context[source_name]

                            # Handle output selection for multi-output nodes
                            if (
                                isinstance(source_result, dict)
                                and output_name in source_result
                            ):
                                node_inputs[single_input_name] = source_result[
                                    output_name
                                ]
                            elif isinstance(source_result, ConditionalReturn):
                                node_inputs[single_input_name] = source_result.value
                            else:
                                node_inputs[single_input_name] = source_result
                            any_input_available = True
                            break  # For ANY node, one input is enough
                else:
                    # Multiple input parameters - match by connection
                    for input_name, (
                        source_node,
                        output_name,
                    ) in node.input_connections.items():
                        source_name = source_node.name
                        if source_name is None:
                            continue
                        if source_name in context:
                            source_result = context[source_name]

                            # Handle output selection for multi-output nodes
                            if (
                                isinstance(source_result, dict)
                                and output_name in source_result
                            ):
                                node_inputs[input_name] = source_result[output_name]
                            elif isinstance(source_result, ConditionalReturn):
                                node_inputs[input_name] = source_result.value
                            else:
                                node_inputs[input_name] = source_result
                            any_input_available = True
                            # Don't break - collect all available inputs for ANY nodes

                # If no inputs available for ANY node, prepare inputs with None values
                # This allows the node to execute and handle the case where all inputs failed
                if not any_input_available and node.inputs:
                    # Set all inputs to None so the node can handle the error case
                    for input_name in node.inputs:
                        node_inputs[input_name] = None
            else:
                # For regular and ALL nodes, all inputs must be available
                for input_name, (
                    source_node,
                    output_name,
                ) in node.input_connections.items():
                    source_name = source_node.name
                    if source_name is None:
                        raise ExecutionError("Source node has no name")
                    if source_name not in context:
                        raise ExecutionError(
                            f"Node '{node_name}' requires result from '{source_name}' which hasn't executed"
                        )

                    source_result = context[source_name]

                    # Handle output selection for multi-output nodes
                    if isinstance(source_result, dict) and output_name in source_result:
                        node_inputs[input_name] = source_result[output_name]
                    elif isinstance(source_result, ConditionalReturn):
                        node_inputs[input_name] = source_result.value
                    else:
                        node_inputs[input_name] = source_result

        # After getting all connected inputs, check if there are any kwargs
        # that can fill in missing inputs (for both entry and non-entry nodes)
        if node.inputs:
            for input_name in node.inputs:
                if input_name not in node_inputs and input_name in kwargs:
                    node_inputs[input_name] = kwargs[input_name]

        return node_inputs

    def _compute_dependency_levels(self) -> list[list[str]]:
        """Group nodes by dependency levels for parallel execution."""
        levels = []
        remaining = set(self.dag.nodes.keys())
        completed = set()

        while remaining:
            # Find nodes that can execute at this level
            level_nodes = []

            for node_name in remaining:
                node = self.dag.nodes[node_name]

                # Check if all dependencies are satisfied
                can_execute = True
                for _, (source_node, _) in node.input_connections.items():
                    if source_node.name and source_node.name not in completed:
                        can_execute = False
                        break

                if can_execute:
                    level_nodes.append(node_name)

            if not level_nodes:
                # Circular dependency or error
                raise ExecutionError(
                    f"Cannot determine execution order. Remaining nodes: {remaining}"
                )

            levels.append(level_nodes)
            completed.update(level_nodes)
            remaining.difference_update(level_nodes)

        return levels

    def _get_node_timeout(self, node: Any) -> float | None:
        """Get timeout for a specific node."""
        # Node-specific timeout takes precedence
        if hasattr(node, "timeout") and node.timeout is not None:
            return node.timeout
        # Otherwise use global timeout
        return self.timeout
