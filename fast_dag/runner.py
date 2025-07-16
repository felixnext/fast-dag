"""Runner abstraction for DAG execution."""

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .core.context import Context
from .core.exceptions import ExecutionError
from .core.types import ConditionalReturn
from .dag import DAG


class ExecutionMode(Enum):
    """Execution modes for DAG runner."""

    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    ASYNC = "async"


@dataclass
class ExecutionMetrics:
    """Metrics collected during DAG execution."""

    total_duration: float = 0.0
    nodes_executed: int = 0
    node_times: dict[str, float] = field(default_factory=dict)
    parallel_groups: list[list[str]] = field(default_factory=list)
    errors: dict[str, str] = field(default_factory=dict)


@dataclass
class DAGRunner:
    """Runner for executing DAGs with different strategies.

    Supports sequential, parallel, and async execution modes,
    with configurable error handling and metrics collection.
    """

    dag: DAG
    mode: ExecutionMode = ExecutionMode.SEQUENTIAL
    max_workers: int = 4
    timeout: float | None = None
    error_strategy: str = "stop"  # stop, continue, retry

    # Runtime state
    _metrics: ExecutionMetrics = field(default_factory=ExecutionMetrics)
    _executor: ThreadPoolExecutor | None = None

    def configure(
        self,
        mode: ExecutionMode | None = None,
        max_workers: int | None = None,
        timeout: float | None = None,
        error_strategy: str | None = None,
    ) -> None:
        """Configure runner settings."""
        if mode is not None:
            self.mode = mode
        if max_workers is not None:
            self.max_workers = max_workers
        if timeout is not None:
            self.timeout = timeout
        if error_strategy is not None:
            self.error_strategy = error_strategy

    def run(self, context: Context | None = None, **kwargs: Any) -> Any:
        """Execute the DAG with configured settings.

        Args:
            context: Execution context (created if not provided)
            **kwargs: Input values for entry nodes

        Returns:
            The result from the final node(s)
        """
        start_time = time.time()
        self._metrics = ExecutionMetrics()

        try:
            if self.mode == ExecutionMode.SEQUENTIAL:
                result = self._run_sequential(context, **kwargs)
            elif self.mode == ExecutionMode.PARALLEL:
                result = self._run_parallel(context, **kwargs)
            elif self.mode == ExecutionMode.ASYNC:
                # For sync run() with async mode, create event loop
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    result = loop.run_until_complete(self._run_async(context, **kwargs))
                finally:
                    loop.close()
            else:
                raise ValueError(f"Unknown execution mode: {self.mode}")
        finally:
            self._metrics.total_duration = time.time() - start_time

        return result

    async def run_async(self, context: Context | None = None, **kwargs: Any) -> Any:
        """Execute the DAG asynchronously.

        Args:
            context: Execution context (created if not provided)
            **kwargs: Input values for entry nodes

        Returns:
            The result from the final node(s)
        """

        start_time = time.time()
        self._metrics = ExecutionMetrics()

        try:
            if self.timeout:
                result = await asyncio.wait_for(
                    self._run_async(context, **kwargs), timeout=self.timeout
                )
            else:
                result = await self._run_async(context, **kwargs)
        except asyncio.TimeoutError as e:
            raise TimeoutError(
                f"DAG execution exceeded timeout of {self.timeout}s"
            ) from e
        finally:
            self._metrics.total_duration = time.time() - start_time

        return result

    def _run_sequential(self, context: Context | None = None, **kwargs: Any) -> Any:
        """Run DAG in sequential mode."""
        # Initialize context
        context = context or Context()
        self.dag.context = context

        # Validate DAG
        errors = self.dag.validate(allow_disconnected=True)
        if errors:
            raise ExecutionError(f"Cannot execute invalid DAG: {errors}")

        # Get execution order
        exec_order = self.dag.execution_order
        entry_nodes = self.dag.entry_points

        # Execute nodes in order
        last_result = None

        for node_name in exec_order:
            node_start = time.time()

            try:
                node = self.dag.nodes[node_name]

                # Check if node should be skipped (wrong conditional branch)
                if self._should_skip_node(node_name, context):
                    continue

                # Prepare inputs
                node_inputs = self._prepare_node_inputs(
                    node_name, node, context, entry_nodes, kwargs
                )

                # Execute node with retry logic
                result = self._execute_node_with_retry(node, node_inputs, context)

                # Store result
                context.set_result(node_name, result)
                last_result = result
                self._metrics.nodes_executed += 1

            except Exception as e:
                self._metrics.errors[node_name] = str(e)

                if self.error_strategy == "stop":
                    raise
                elif self.error_strategy == "continue":
                    context.metadata[f"{node_name}_error"] = str(e)
                    continue
                else:
                    raise ValueError(
                        f"Unknown error strategy: {self.error_strategy}"
                    ) from e
            finally:
                self._metrics.node_times[node_name] = time.time() - node_start

        return last_result

    def _run_parallel(self, context: Context | None = None, **kwargs: Any) -> Any:
        """Run DAG in parallel mode."""
        # Initialize context
        context = context or Context()
        self.dag.context = context

        # Validate DAG
        errors = self.dag.validate(allow_disconnected=True)
        if errors:
            raise ExecutionError(f"Cannot execute invalid DAG: {errors}")

        # Get execution order and dependencies
        entry_nodes = self.dag.entry_points

        # Group nodes by dependency level
        dependency_levels = self._compute_dependency_levels()

        # Execute levels in order
        last_result = None

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            self._executor = executor

            for level_nodes in dependency_levels:
                # Track parallel group for metrics
                self._metrics.parallel_groups.append(level_nodes)

                # Submit all nodes at this level
                futures = {}

                for node_name in level_nodes:
                    if self._should_skip_node(node_name, context):
                        continue

                    node = self.dag.nodes[node_name]
                    node_inputs = self._prepare_node_inputs(
                        node_name, node, context, entry_nodes, kwargs
                    )

                    # Submit for parallel execution
                    future = executor.submit(
                        self._execute_node_task, node_name, node, node_inputs, context
                    )
                    futures[future] = node_name

                # Wait for all nodes at this level to complete
                for future in as_completed(futures):
                    node_name = futures[future]

                    try:
                        result = future.result()
                        if result is not None:
                            last_result = result
                    except Exception as e:
                        self._metrics.errors[node_name] = str(e)

                        if self.error_strategy == "stop":
                            # Cancel remaining futures
                            for f in futures:
                                if not f.done():
                                    f.cancel()
                            raise ExecutionError(
                                f"Error executing node '{node_name}': {e}"
                            ) from e
                        elif self.error_strategy == "continue":
                            context.metadata[f"{node_name}_error"] = str(e)

            self._executor = None

        return last_result

    async def _run_async(self, context: Context | None = None, **kwargs: Any) -> Any:
        """Run DAG in async mode."""
        # Initialize context
        context = context or Context()
        self.dag.context = context

        # Validate DAG
        errors = self.dag.validate(allow_disconnected=True)
        if errors:
            raise ExecutionError(f"Cannot execute invalid DAG: {errors}")

        # Get execution order and dependencies
        entry_nodes = self.dag.entry_points

        # Group nodes by dependency level
        dependency_levels = self._compute_dependency_levels()

        # Execute levels in order
        last_result = None

        for level_nodes in dependency_levels:
            # Track parallel group for metrics
            self._metrics.parallel_groups.append(level_nodes)

            # Create tasks for all nodes at this level
            tasks = []
            task_names = []

            for node_name in level_nodes:
                if self._should_skip_node(node_name, context):
                    continue

                node = self.dag.nodes[node_name]
                node_inputs = self._prepare_node_inputs(
                    node_name, node, context, entry_nodes, kwargs
                )

                # Create async task
                task = self._execute_node_async(node_name, node, node_inputs, context)
                tasks.append(task)
                task_names.append(node_name)

            # Execute all tasks concurrently
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for node_name, result in zip(task_names, results):
                    if isinstance(result, Exception):
                        self._metrics.errors[node_name] = str(result)

                        if self.error_strategy == "stop":
                            raise ExecutionError(
                                f"Error executing node '{node_name}': {result}"
                            ) from result
                        elif self.error_strategy == "continue":
                            context.metadata[f"{node_name}_error"] = str(result)
                    elif result is not None:
                        last_result = result

        return last_result

    def _execute_node_task(
        self,
        node_name: str,
        node: Any,
        node_inputs: dict[str, Any],
        context: Context,
    ) -> Any:
        """Execute a node as a thread pool task."""
        node_start = time.time()

        try:
            result = self._execute_node_with_retry(node, node_inputs, context)
            context.set_result(node_name, result)
            self._metrics.nodes_executed += 1
            return result
        finally:
            self._metrics.node_times[node_name] = time.time() - node_start

    async def _execute_node_async(
        self,
        node_name: str,
        node: Any,
        node_inputs: dict[str, Any],
        context: Context,
    ) -> Any:
        """Execute a node asynchronously."""
        node_start = time.time()

        try:
            # Execute with timeout if specified
            timeout = self._get_node_timeout(node)

            if node.is_async:
                coro = node.execute_async(node_inputs, context=context)
            else:
                # Run sync function in thread pool
                loop = asyncio.get_running_loop()
                coro = loop.run_in_executor(None, node.execute, node_inputs, context)

            if timeout:
                result = await asyncio.wait_for(coro, timeout=timeout)
            else:
                result = await coro

            context.set_result(node_name, result)
            self._metrics.nodes_executed += 1
            return result

        finally:
            self._metrics.node_times[node_name] = time.time() - node_start

    def _execute_node_with_retry(
        self,
        node: Any,
        node_inputs: dict[str, Any],
        context: Context,
    ) -> Any:
        """Execute node with retry logic if configured."""
        max_retries = 1

        # Check if node has retry configuration
        if self.error_strategy == "retry" and hasattr(node, "retry"):
            max_retries = getattr(node, "retry", 1)

        last_error = None
        for attempt in range(max_retries):
            try:
                # Check for node timeout
                timeout = self._get_node_timeout(node)

                if timeout and not node.is_async:
                    # Use threading for timeout in sync mode
                    import threading

                    result_holder = [None]
                    error_holder = [None]

                    def run(rh=result_holder, eh=error_holder):
                        try:
                            rh[0] = node.execute(node_inputs, context=context)
                        except Exception as e:
                            eh[0] = e

                    thread = threading.Thread(target=run)
                    thread.start()
                    thread.join(timeout)

                    if thread.is_alive():
                        raise TimeoutError(
                            f"Node execution exceeded timeout of {timeout}s"
                        )

                    if error_holder[0]:
                        raise error_holder[0]

                    return result_holder[0]
                else:
                    return node.execute(node_inputs, context=context)

            except Exception as e:
                last_error = e
                if attempt < max_retries - 1:
                    # Wait before retry
                    time.sleep(0.1 * (attempt + 1))
                    continue
                raise

        if last_error:
            raise last_error
        raise RuntimeError("No error recorded but execution failed")

    def _should_skip_node(self, node_name: str, context: Context) -> bool:
        """Check if node should be skipped based on conditional branches."""
        node = self.dag.nodes[node_name]

        # Check if this node depends on a conditional
        for _input_name, (source_node, output_name) in node.input_connections.items():
            source_name = source_node.name
            if source_name and source_name in context:
                source_result = context[source_name]

                if isinstance(source_result, ConditionalReturn) and (
                    output_name == "true"
                    and not source_result.condition
                    or output_name == "false"
                    and source_result.condition
                ):
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
                    # Single output node
                    node_inputs[input_name] = source_result

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

    def get_metrics(self) -> dict[str, Any]:
        """Get execution metrics."""
        return {
            "total_duration": self._metrics.total_duration,
            "nodes_executed": self._metrics.nodes_executed,
            "node_times": self._metrics.node_times,
            "parallel_groups": self._metrics.parallel_groups,
            "errors": self._metrics.errors,
        }
