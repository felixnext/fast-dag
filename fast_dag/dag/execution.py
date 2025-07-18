"""DAG execution functionality."""

from typing import TYPE_CHECKING, Any

from ..core.context import Context
from ..core.exceptions import ExecutionError, ValidationError
from ..core.node import Node

if TYPE_CHECKING:
    from .core import DAG


class DAGExecutor:
    """Mixin class for DAG execution functionality."""

    # Type hints for attributes from DAG
    name: str
    nodes: dict[str, Node]
    context: Context | None
    description: str | None
    metadata: dict[str, Any]

    def validate(
        self, allow_disconnected: bool = False, check_types: bool = False
    ) -> list[str]:
        """Validate method - defined in DAG."""
        raise NotImplementedError

    def get_execution_order(self) -> list[str]:
        """Get execution order - defined in DAG."""
        raise NotImplementedError

    def get_entry_points(self) -> list[str]:
        """Get entry points - defined in DAG."""
        raise NotImplementedError

    def _invalidate_cache(self) -> None:
        """Invalidate cache - defined in DAG."""
        raise NotImplementedError

    def run(
        self,
        *,
        inputs: dict[str, Any] | None = None,
        context: Context | None = None,
        mode: str = "sync",
        max_workers: int | None = None,
        error_strategy: str | None = None,
        **kwargs,
    ) -> Any:
        """Execute the DAG and return the result.

        Args:
            inputs: Input values for the DAG
            context: Execution context (creates new if None)
            mode: Execution mode ('sync', 'async', 'parallel')
            max_workers: Maximum number of workers for parallel execution
            **kwargs: Additional input values

        Returns:
            Final result of the DAG execution
        """
        # Validate DAG first
        # Allow disconnected nodes if error_strategy supports continuing
        allow_disconnected = error_strategy in (
            "continue",
            "continue_none",
            "continue_skip",
        )
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

        # Prepare inputs
        all_inputs = {}
        if inputs:
            all_inputs.update(inputs)
        all_inputs.update(kwargs)

        # Prepare context
        if context is None:
            context = Context()

        # Get runner
        runner = self._get_runner(mode, max_workers, error_strategy)

        # Execute
        try:
            result = runner.run(context=context, **all_inputs)
        except ValueError as e:
            # Wrap ValueError in ExecutionError at the DAG level
            raise ExecutionError(str(e)) from e
        except Exception:
            # Let other errors bubble up
            raise

        # Store context
        self.context = context

        return result

    async def run_async(
        self,
        *,
        inputs: dict[str, Any] | None = None,
        context: Context | None = None,
        max_workers: int | None = None,
        **kwargs,
    ) -> Any:
        """Execute the DAG asynchronously.

        Args:
            inputs: Input values for the DAG
            context: Execution context (creates new if None)
            max_workers: Maximum number of workers for parallel execution
            **kwargs: Additional input values

        Returns:
            Final result of the DAG execution
        """
        # Validate DAG first
        errors = self.validate()
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

        # Prepare inputs
        all_inputs = {}
        if inputs:
            all_inputs.update(inputs)
        all_inputs.update(kwargs)

        # Prepare context
        if context is None:
            context = Context()

        # Get async runner
        from ..runner import DAGRunner, ExecutionMode

        runner = DAGRunner(
            dag=self,  # type: ignore[arg-type]
            mode=ExecutionMode.ASYNC,
            max_workers=max_workers or 4,
        )

        # Execute
        result = await runner.run_async(context=context, **all_inputs)

        # Store context
        self.context = context

        return result

    def step(
        self,
        *,
        context: Context | None = None,
        inputs: dict[str, Any] | None = None,
        **kwargs,
    ) -> tuple[Context, Any]:
        """Execute a single step of the DAG.

        Args:
            context: Current execution context
            inputs: Input values for this step
            **kwargs: Additional input values

        Returns:
            Tuple of (updated_context, step_result)
        """
        # Validate DAG first
        errors = self.validate()
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

        # Prepare inputs
        all_inputs = {}
        if inputs:
            all_inputs.update(inputs)
        all_inputs.update(kwargs)

        # Prepare context
        if context is None:
            context = Context()

        # Execute step using sequential runner
        self._get_runner("sync")

        # Get execution order
        exec_order = self.get_execution_order()
        entry_nodes = self.get_entry_points()

        # Find next node to execute
        result = None
        for node_name in exec_order:
            node = self.nodes[node_name]

            # Check if node has already been executed
            if context.has_result(node_name):
                continue

            # Check if all dependencies are satisfied
            can_execute = True
            for dep_name in self.nodes:
                dep_node = self.nodes[dep_name]
                for connections in dep_node.output_connections.values():
                    for target_node, _ in connections:
                        if target_node.name == node_name and not context.has_result(
                            dep_name
                        ):
                            can_execute = False
                            break
                    if not can_execute:
                        break
                if not can_execute:
                    break

            if can_execute:
                # Prepare inputs
                node_inputs = {}
                if node_name in entry_nodes:
                    node_inputs.update(all_inputs)

                # Collect inputs from dependencies
                for dep_name in self.nodes:
                    dep_node = self.nodes[dep_name]
                    for output_name, connections in dep_node.output_connections.items():
                        for target_node, input_name in connections:
                            if target_node.name == node_name and context.has_result(
                                dep_name
                            ):
                                dep_result = context.get_result(dep_name)
                                if output_name == "result":
                                    node_inputs[input_name] = dep_result
                                elif hasattr(dep_result, output_name):
                                    node_inputs[input_name] = getattr(
                                        dep_result, output_name
                                    )

                # Execute node
                result = node.execute(node_inputs, context=context)
                context.set_result(node_name, result)
                break

        return context, result

    def _get_runner(
        self,
        mode: str = "sync",
        max_workers: int | None = None,
        error_strategy: str | None = None,
    ) -> Any:
        """Get the appropriate runner for the execution mode.

        Args:
            mode: Execution mode
            max_workers: Maximum number of workers
            error_strategy: Error handling strategy

        Returns:
            Runner instance
        """
        # Import here to avoid circular imports
        from ..runner import DAGRunner, ExecutionMode

        # Map string mode to ExecutionMode enum
        mode_map = {
            "sync": ExecutionMode.SEQUENTIAL,
            "async": ExecutionMode.ASYNC,
            "parallel": ExecutionMode.PARALLEL,
        }

        if mode not in mode_map:
            raise ValueError(f"Unknown execution mode: {mode}")

        runner = DAGRunner(
            dag=self,  # type: ignore[arg-type]
            mode=mode_map[mode],
            max_workers=max_workers or 4,
            error_strategy=error_strategy or "stop",
        )

        return runner

    def dry_run(
        self,
        *,
        inputs: dict[str, Any] | None = None,
        **kwargs,
    ) -> dict[str, Any]:
        """Perform a dry run of the DAG without executing nodes.

        Args:
            inputs: Input values for the DAG
            **kwargs: Additional input values

        Returns:
            Dictionary with execution plan information
        """
        # Validate DAG first
        errors = self.validate()
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

        # Prepare inputs
        all_inputs = {}
        if inputs:
            all_inputs.update(inputs)
        all_inputs.update(kwargs)

        # Get execution order
        execution_order = self.get_execution_order()

        # Build execution plan
        plan = {
            "execution_order": execution_order,
            "total_nodes": len(execution_order),
            "entry_points": self.get_entry_points(),
            "inputs": all_inputs,
            "node_details": {},
        }

        # Add node details
        for node_name in execution_order:
            node = self.nodes[node_name]
            plan["node_details"][node_name] = {  # type: ignore[index]
                "name": node.name,
                "function": node.func.__name__,
                "inputs": node.inputs,
                "outputs": node.outputs,
                "node_type": node.node_type.value,
                "has_retry": node.retry is not None and node.retry > 0,
                "has_timeout": node.timeout is not None,
                "connections": {
                    output: [
                        (target.name, input_name) for target, input_name in connections
                    ]
                    for output, connections in node.output_connections.items()
                },
            }

        return plan

    def get_metrics(self) -> dict[str, Any]:
        """Get execution metrics from the last run.

        Returns:
            Dictionary with execution metrics
        """
        if not self.context:
            return {"error": "No execution context available"}

        return {
            "execution_time": self.context.metrics.get("execution_time", 0),
            "nodes_executed": self.context.metrics.get("nodes_executed", 0),
            "nodes_failed": self.context.metrics.get("nodes_failed", 0),
            "nodes_skipped": self.context.metrics.get("nodes_skipped", 0),
            "cache_hits": self.context.metrics.get("cache_hits", 0),
            "cache_misses": self.context.metrics.get("cache_misses", 0),
            "total_results": len(self.context.results),
            "metadata": self.context.metadata,
        }

    def reset(self) -> None:
        """Reset the DAG execution state."""
        self.context = Context()
        self._invalidate_cache()

    def clone(self) -> "DAG":
        """Create a deep copy of the DAG.

        Returns:
            New DAG instance with same structure
        """
        # Import here to avoid circular imports
        from . import DAG

        new_dag = DAG(
            name=f"{self.name}_clone",
            description=self.description,
            metadata=self.metadata.copy(),
        )

        # Copy nodes
        for _node_name, node in self.nodes.items():
            new_node = Node(
                func=node.func,
                name=node.name,
                inputs=node.inputs.copy() if node.inputs else None,
                outputs=node.outputs.copy() if node.outputs else None,
                description=node.description,
                node_type=node.node_type,
                retry=node.retry,
                retry_delay=node.retry_delay,
                timeout=node.timeout,
                metadata=node.metadata.copy(),
            )
            new_dag.add_node(new_node)

        # Copy connections
        for node_name, node in self.nodes.items():
            new_node = new_dag.nodes[node_name]

            # Copy output connections
            for output, connections in node.output_connections.items():
                new_connections = []
                for target_node, input_name in connections:
                    new_target = new_dag.nodes[target_node.name]
                    new_connections.append((new_target, input_name))
                new_node.output_connections[output] = new_connections

            # Copy input connections
            for input_name, (
                source_node,
                output_name,
            ) in node.input_connections.items():
                new_source = new_dag.nodes[source_node.name]
                new_node.input_connections[input_name] = (new_source, output_name)

        return new_dag

    def _can_execute_node(self, node: Node, node_name: str) -> bool:  # noqa: ARG002
        """Check if a node can be executed based on its input dependencies."""
        from ..core.types import NodeType

        if node.node_type == NodeType.ANY:
            # ANY nodes can execute if at least one input is available
            # If there are no connections, they can execute (entry nodes)
            if not node.input_connections and not node.multi_input_connections:
                return True

            # Check multi_input_connections first for ANY nodes
            all_source_nodes = []
            if node.multi_input_connections:
                for connections in node.multi_input_connections.values():
                    all_source_nodes.extend(connections)
            else:
                # Fallback to regular input_connections
                all_source_nodes = list(node.input_connections.values())

            if not all_source_nodes:
                return True

            # For ANY nodes, they can execute if at least one input is available
            if self.context is None:
                return False
            for source_node, _ in all_source_nodes:
                if source_node.name and source_node.name in self.context.results:
                    return True
            return False

        elif node.node_type == NodeType.ALL:
            # ALL nodes need all inputs to be available
            # If there are no connections, they can execute (entry nodes)
            if not node.input_connections and not node.multi_input_connections:
                return True

            # Check multi_input_connections first for ALL nodes
            all_source_nodes = []
            if node.multi_input_connections:
                for connections in node.multi_input_connections.values():
                    all_source_nodes.extend(connections)
            else:
                # Fallback to regular input_connections
                all_source_nodes = list(node.input_connections.values())

            if not all_source_nodes:
                return True

            # For ALL nodes, they need all inputs to be available
            if self.context is None:
                return False
            for source_node, _ in all_source_nodes:
                if (
                    source_node.name is None
                    or source_node.name not in self.context.results
                ):
                    return False
            return True

        else:
            # Regular nodes and other types
            # Check all regular inputs
            if self.context is None:
                return False
            for source_node, _ in node.input_connections.values():
                if (
                    source_node.name is None
                    or source_node.name not in self.context.results
                ):
                    return False

            # If a node has no connections and is not an entry point, it cannot execute
            if not node.input_connections and not node.multi_input_connections:
                # Entry nodes have no input connections but should execute
                return True

            return True

    def _prepare_node_inputs(
        self,
        node: Node,
        node_name: str,
        entry_nodes: list[str],
        kwargs: dict[str, Any],
        error_strategy: str,
    ) -> tuple[dict[str, Any], bool]:
        """Prepare inputs for node execution.

        Returns:
            tuple: (node_inputs, skip_node)
        """
        node_inputs = {}
        skip_node = False

        if node_name in entry_nodes:
            # Entry node - get inputs from kwargs
            for input_name in node.inputs or []:
                if input_name in kwargs:
                    node_inputs[input_name] = kwargs[input_name]
        else:
            # Non-entry node - get inputs from dependencies
            from ..core.types import NodeType

            if node.node_type in (NodeType.ANY, NodeType.ALL):
                # For ANY/ALL nodes, collect inputs from multi_input_connections
                collected_inputs = []
                all_source_nodes = []

                if node.multi_input_connections:
                    for connections in node.multi_input_connections.values():
                        all_source_nodes.extend(connections)
                else:
                    # Fallback to regular input_connections
                    all_source_nodes = list(node.input_connections.values())

                for source_node, output_name in all_source_nodes:
                    if (
                        self.context
                        and source_node.name
                        and source_node.name in self.context.results
                    ):
                        result = self.context.get_result(source_node.name)
                        # Handle specific output selection
                        if isinstance(result, dict) and output_name != "result":
                            if output_name in result:
                                collected_inputs.append(result[output_name])
                            else:
                                # Output not found in result dict
                                if error_strategy == "raise":
                                    raise ExecutionError(
                                        f"Output '{output_name}' not found in result from node '{source_node.name}'"
                                    )
                                elif error_strategy == "skip":
                                    skip_node = True
                                    break
                                else:  # continue
                                    pass
                        else:
                            collected_inputs.append(result)

                if node.node_type == NodeType.ANY and not collected_inputs:
                    # ANY node with no available inputs should skip
                    if error_strategy == "skip":
                        skip_node = True
                    else:
                        node_inputs["inputs"] = []
                elif node.node_type == NodeType.ALL and len(collected_inputs) < len(
                    all_source_nodes
                ):
                    # ALL node with missing inputs should handle based on error strategy
                    if error_strategy == "raise":
                        missing = [
                            sn.name
                            for sn, _ in all_source_nodes
                            if sn.name
                            and self.context
                            and sn.name not in self.context.results
                        ]
                        raise ExecutionError(
                            f"ALL node '{node_name}' missing inputs from: {missing}"
                        )
                    elif error_strategy == "skip":
                        skip_node = True
                    else:  # continue
                        node_inputs["inputs"] = collected_inputs
                else:
                    node_inputs["inputs"] = collected_inputs
            else:
                # Regular node - get inputs from connections
                for input_name, (
                    source_node,
                    output_name,
                ) in node.input_connections.items():
                    if source_node.name and self.context:
                        result = self.context.get_result(source_node.name)
                        # Handle specific output selection
                        if isinstance(result, dict) and output_name != "result":
                            if output_name in result:
                                node_inputs[input_name] = result[output_name]
                            else:
                                # Output not found in result dict
                                if error_strategy == "raise":
                                    raise ExecutionError(
                                        f"Output '{output_name}' not found in result from node '{source_node.name}'"
                                    )
                                elif error_strategy == "skip":
                                    skip_node = True
                                    break
                                else:  # continue
                                    node_inputs[input_name] = None
                        else:
                            node_inputs[input_name] = result

        # Add context if needed
        if node.inputs and "context" in node.inputs:
            node_inputs["context"] = self.context

        return node_inputs, skip_node

    def _get_parallel_groups(self) -> list[list[str]]:
        """Get groups of nodes that can be executed in parallel."""
        groups = []
        remaining_nodes = set(self.nodes.keys())

        while remaining_nodes:
            # Find nodes that can execute now (dependencies satisfied)
            ready_nodes = []
            for node_name in remaining_nodes:
                node = self.nodes[node_name]
                dependencies_satisfied = True

                for _input_name, (source_node, _) in node.input_connections.items():
                    if source_node.name and source_node.name in remaining_nodes:
                        dependencies_satisfied = False
                        break

                if dependencies_satisfied:
                    ready_nodes.append(node_name)

            if ready_nodes:
                groups.append(ready_nodes)
                remaining_nodes -= set(ready_nodes)
            else:
                # If no nodes are ready, we have a problem (cycle or missing deps)
                break

        return groups
