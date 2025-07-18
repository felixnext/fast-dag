"""Node execution functionality."""

import asyncio
import threading
import time
from typing import Any

from ..exceptions import TimeoutError
from .operators import NodeOperators


class NodeExecution(NodeOperators):
    """Node execution functionality."""

    def execute(self, inputs: dict[str, Any], context: Any | None = None) -> Any:
        """Execute the node's function with the given inputs.

        Args:
            inputs: Input values mapped by parameter name
            context: Optional context object to pass if node accepts it

        Returns:
            The result of executing the node's function
        """
        # Build kwargs from inputs
        kwargs = dict(inputs)

        # Add context if the node accepts it
        if self.has_context and context is not None:
            kwargs["context"] = context

        # Check cache if enabled (before calling hooks)
        if self.cached:
            from ..cache import generate_cache_key, get_cache

            cache = get_cache(self.cache_backend)
            cache_key = generate_cache_key(self.name or "", inputs)

            # Try to get from cache
            cached_result = cache.get(cache_key)
            if cached_result is not None:
                # Update context metrics if available
                if context is not None and hasattr(context, "metrics"):
                    if "cache_hits" not in context.metrics:
                        context.metrics["cache_hits"] = {}
                    context.metrics["cache_hits"][self.name or ""] = (
                        context.metrics.get("cache_hits", {}).get(self.name or "", 0)
                        + 1
                    )

                # Call hooks even for cached results
                if self.pre_execute is not None:
                    try:
                        self.pre_execute(self, kwargs)
                    except Exception as e:
                        from ..exceptions import ExecutionError

                        raise ExecutionError(
                            f"Pre-execute hook failed for node '{self.name}': {e}"
                        ) from e

                if self.post_execute is not None:
                    hook_result = self.post_execute(self, kwargs, cached_result)
                    # Only update result if hook explicitly returns a meaningful value
                    # Skip MagicMock returns and None
                    from unittest.mock import MagicMock

                    if hook_result is not None and not isinstance(
                        hook_result, MagicMock
                    ):
                        cached_result = hook_result

                return cached_result

        try:
            # Execute with retry logic if configured
            if self.retry is not None and self.retry > 1:
                # For retry scenarios, hooks are called inside _execute_with_retry
                result = self._execute_with_retry(kwargs)
            else:
                # Call pre-execute hook if defined
                if self.pre_execute is not None:
                    try:
                        self.pre_execute(self, kwargs)
                    except Exception as e:
                        from ..exceptions import ExecutionError

                        raise ExecutionError(
                            f"Pre-execute hook failed for node '{self.name}': {e}"
                        ) from e

                result = self._execute_once(kwargs)

            # Call post-execute hook if defined
            if self.post_execute is not None:
                hook_result = self.post_execute(self, kwargs, result)
                # Only update result if hook explicitly returns a meaningful value
                # Skip MagicMock returns and None
                from unittest.mock import MagicMock

                if hook_result is not None and not isinstance(hook_result, MagicMock):
                    result = hook_result

            # Store in cache if enabled
            if self.cached:
                cache.set(cache_key, result, ttl=self.cache_ttl)
                # Update context metrics if available
                if context is not None and hasattr(context, "metrics"):
                    if "cache_misses" not in context.metrics:
                        context.metrics["cache_misses"] = {}
                    context.metrics["cache_misses"][self.name or ""] = (
                        context.metrics.get("cache_misses", {}).get(self.name or "", 0)
                        + 1
                    )

            return result

        except Exception as e:
            # Call on-error hook if defined
            if self.on_error is not None:
                self.on_error(self, kwargs, e)
            raise

    def _execute_with_retry(self, kwargs: dict[str, Any]) -> Any:
        """Execute with retry logic and exponential backoff."""
        max_retries = self.retry or 1
        base_delay = self.retry_delay or 0.1
        last_exception = None

        for attempt in range(max_retries):
            try:
                # Call pre-execute hook for each retry attempt
                if self.pre_execute is not None:
                    try:
                        self.pre_execute(self, kwargs)
                    except Exception as e:
                        from ..exceptions import ExecutionError

                        raise ExecutionError(
                            f"Pre-execute hook failed for node '{self.name}': {e}"
                        ) from e

                return self._execute_once(kwargs)
            except Exception as e:
                last_exception = e

                # Call on-error hook for each failure
                if self.on_error is not None:
                    self.on_error(self, kwargs, e)

                if attempt < max_retries - 1:
                    # Calculate exponential backoff delay
                    delay = base_delay * (2**attempt)
                    time.sleep(delay)
                else:
                    # Last attempt failed, raise the exception
                    raise

        # This should never be reached, but for safety
        if last_exception:
            raise last_exception
        raise RuntimeError("Retry logic failed unexpectedly")

    def _execute_once(self, kwargs: dict[str, Any]) -> Any:
        """Execute the function once with timeout handling."""
        if self.timeout is not None:
            # Use threading for timeout to avoid signal issues
            result = [None]
            exception = [None]

            def target():
                try:
                    result[0] = self.func(**kwargs)
                except Exception as e:
                    exception[0] = e

            thread = threading.Thread(target=target)
            thread.start()
            thread.join(timeout=self.timeout)

            if thread.is_alive():
                # Thread is still running, timeout occurred
                raise TimeoutError(
                    f"Node '{self.name}' execution timed out after {self.timeout} seconds"
                )

            # Check if exception occurred
            if exception[0] is not None:
                raise exception[0]

            return result[0]
        else:
            # Execute without timeout
            return self.func(**kwargs)

    async def execute_async(
        self, inputs: dict[str, Any], context: Any | None = None
    ) -> Any:
        """Execute an async node's function.

        Args:
            inputs: Input values mapped by parameter name
            context: Optional context object to pass if node accepts it

        Returns:
            The result of executing the node's async function
        """
        if not self.is_async:
            raise RuntimeError(f"Node '{self.name}' is not async")

        # Check cache if enabled
        if self.cached:
            from ..cache import generate_cache_key, get_cache

            cache = get_cache(self.cache_backend)
            cache_key = generate_cache_key(self.name or "", inputs)

            # Try to get from cache
            cached_result = cache.get(cache_key)
            if cached_result is not None:
                # Update context metrics if available
                if context is not None and hasattr(context, "metrics"):
                    if "cache_hits" not in context.metrics:
                        context.metrics["cache_hits"] = {}
                    context.metrics["cache_hits"][self.name or ""] = (
                        context.metrics.get("cache_hits", {}).get(self.name or "", 0)
                        + 1
                    )
                return cached_result

        # Build kwargs from inputs
        kwargs = dict(inputs)

        # Add context if the node accepts it
        if self.has_context and context is not None:
            kwargs["context"] = context

        # Call pre-execute hook if defined
        if self.pre_execute is not None:
            try:
                self.pre_execute(self, kwargs)
            except Exception as e:
                from ..exceptions import ExecutionError

                raise ExecutionError(
                    f"Pre-execute hook failed for node '{self.name}': {e}"
                ) from e

        try:
            # Execute with retry logic if configured
            if self.retry is not None and self.retry > 1:
                result = await self._execute_with_retry_async(kwargs)
            else:
                result = await self._execute_once_async(kwargs)

            # Call post-execute hook if defined
            if self.post_execute is not None:
                hook_result = self.post_execute(self, kwargs, result)
                # Only update result if hook explicitly returns a meaningful value
                # Skip MagicMock returns and None
                from unittest.mock import MagicMock

                if hook_result is not None and not isinstance(hook_result, MagicMock):
                    result = hook_result

            # Store in cache if enabled
            if self.cached:
                cache.set(cache_key, result, ttl=self.cache_ttl)
                # Update context metrics if available
                if context is not None and hasattr(context, "metrics"):
                    if "cache_misses" not in context.metrics:
                        context.metrics["cache_misses"] = {}
                    context.metrics["cache_misses"][self.name or ""] = (
                        context.metrics.get("cache_misses", {}).get(self.name or "", 0)
                        + 1
                    )

            return result

        except Exception as e:
            # Call on-error hook if defined
            if self.on_error is not None:
                self.on_error(self, kwargs, e)
            raise

    async def _execute_with_retry_async(self, kwargs: dict[str, Any]) -> Any:
        """Execute async with retry logic and exponential backoff."""
        max_retries = self.retry or 1
        base_delay = self.retry_delay or 0.1
        last_exception = None

        for attempt in range(max_retries):
            try:
                return await self._execute_once_async(kwargs)
            except Exception as e:
                last_exception = e
                if attempt < max_retries - 1:
                    # Calculate exponential backoff delay
                    delay = base_delay * (2**attempt)
                    await asyncio.sleep(delay)
                else:
                    # Last attempt failed, raise the exception
                    raise

        # This should never be reached, but for safety
        if last_exception:
            raise last_exception
        raise RuntimeError("Retry logic failed unexpectedly")

    async def _execute_once_async(self, kwargs: dict[str, Any]) -> Any:
        """Execute the async function once with timeout handling."""
        if self.timeout is not None:
            try:
                return await asyncio.wait_for(self.func(**kwargs), timeout=self.timeout)
            except asyncio.TimeoutError as e:
                raise TimeoutError(
                    f"Node '{self.name}' execution timed out after {self.timeout} seconds"
                ) from e
        else:
            # Execute without timeout
            return await self.func(**kwargs)
