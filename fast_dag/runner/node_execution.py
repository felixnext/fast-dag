"""Node execution functionality for DAG runner."""

import asyncio
import time
from typing import Any

from ..core.context import Context
from .utils import RunnerUtils


class NodeExecutor(RunnerUtils):
    """Node execution functionality."""

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
            # Execute node (timeout is handled by the node itself)
            if node.is_async:
                result = await node.execute_async(node_inputs, context=context)
            else:
                # Run sync function in thread pool
                loop = asyncio.get_running_loop()
                result = await loop.run_in_executor(
                    None, node.execute, node_inputs, context
                )

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
        if hasattr(node, "retry") and node.retry is not None:
            max_retries = node.retry

        last_error = None
        for attempt in range(max_retries):
            try:
                # Handle async nodes in sync execution
                if node.is_async:
                    # Create a new event loop to run the async function
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        result = loop.run_until_complete(
                            node.execute_async(node_inputs, context=context)
                        )
                        return result
                    finally:
                        loop.close()
                else:
                    # Let the node handle its own timeout and hooks
                    return node.execute(node_inputs, context=context)

            except Exception as e:
                last_error = e
                # The on_error hook is called inside node.execute
                if attempt < max_retries - 1:
                    # Wait before retry
                    time.sleep(0.1 * (attempt + 1))
                    continue
                raise

        if last_error:
            raise last_error
        raise RuntimeError("No error recorded but execution failed")
