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
        from .core import DAG

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
            # Import here to avoid circular imports at module level
            from .core import DAG as DAGClass

            if isinstance(new_dag, DAGClass):
                new_dag.add_node(new_node)

        # Copy connections
        for node_name, node in self.nodes.items():
            new_node = new_dag.nodes[node_name]
            for output, connections in node.output_connections.items():
                new_connections = []
                for target_node, input_name in connections:
                    new_target = new_dag.nodes[target_node.name]
                    new_connections.append((new_target, input_name))
                new_node.output_connections[output] = new_connections

        return new_dag
