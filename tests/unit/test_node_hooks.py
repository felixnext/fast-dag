"""Unit tests for node lifecycle hooks."""

from unittest.mock import MagicMock

import pytest

from fast_dag import DAG
from fast_dag.core.context import Context
from fast_dag.core.exceptions import ExecutionError, TimeoutError


class TestNodeHooks:
    """Test node lifecycle hooks."""

    def test_pre_execute_hook(self):
        """Test pre_execute hook is called."""
        dag = DAG("test")
        pre_hook = MagicMock()

        @dag.node
        def process(x: int) -> int:
            return x * 2

        # Set hook
        dag.set_node_hooks("process", pre_execute=pre_hook)

        # Execute
        result = dag.run(x=5)

        # Verify hook was called
        pre_hook.assert_called_once()
        args = pre_hook.call_args[0]
        assert len(args) == 2  # node, inputs
        assert args[0].name == "process"
        assert args[1]["x"] == 5
        assert result == 10

    def test_post_execute_hook(self):
        """Test post_execute hook is called."""
        dag = DAG("test")
        post_hook = MagicMock()

        @dag.node
        def compute(x: int) -> int:
            return x + 1

        # Set hook
        dag.set_node_hooks("compute", post_execute=post_hook)

        # Execute
        result = dag.run(x=10)

        # Verify hook was called
        post_hook.assert_called_once()
        args = post_hook.call_args[0]
        assert len(args) == 3  # node, inputs, result
        assert args[0].name == "compute"
        assert args[1]["x"] == 10
        assert args[2] == 11
        assert result == 11

    def test_on_error_hook(self):
        """Test on_error hook is called."""
        dag = DAG("test")
        error_hook = MagicMock()

        @dag.node
        def failing_node(x: int) -> int:
            raise ValueError("Test error")

        # Set hook
        dag.set_node_hooks("failing_node", on_error=error_hook)

        # Execute and expect error
        with pytest.raises(ExecutionError):
            dag.run(x=5)

        # Verify error hook was called
        error_hook.assert_called_once()
        args = error_hook.call_args[0]
        assert len(args) == 3  # node, inputs, exception
        assert args[0].name == "failing_node"
        assert args[1]["x"] == 5
        assert isinstance(args[2], ValueError)

    def test_all_hooks_together(self):
        """Test all hooks work together."""
        dag = DAG("test")
        pre_hook = MagicMock()
        post_hook = MagicMock()
        error_hook = MagicMock()

        @dag.node
        def process(x: int) -> int:
            if x < 0:
                raise ValueError("Negative input")
            return x * 3

        # Set all hooks
        dag.set_node_hooks(
            "process", pre_execute=pre_hook, post_execute=post_hook, on_error=error_hook
        )

        # Test successful execution
        result = dag.run(x=4)

        # Verify successful path
        pre_hook.assert_called_once()
        post_hook.assert_called_once()
        error_hook.assert_not_called()
        assert result == 12

        # Reset mocks
        pre_hook.reset_mock()
        post_hook.reset_mock()
        error_hook.reset_mock()

        # Test error path
        with pytest.raises(ExecutionError):
            dag.run(x=-1)

        # Verify error path
        pre_hook.assert_called_once()
        post_hook.assert_not_called()
        error_hook.assert_called_once()

    def test_post_execute_hook_modifies_result(self):
        """Test post_execute hook can modify result."""
        dag = DAG("test")

        def post_hook(node, inputs, result):
            return result * 2  # Double the result

        @dag.node
        def compute(x: int) -> int:
            return x + 5

        # Set hook
        dag.set_node_hooks("compute", post_execute=post_hook)

        # Execute
        result = dag.run(x=3)

        # Result should be modified: (3 + 5) * 2 = 16
        assert result == 16

    def test_hooks_with_multiple_nodes(self):
        """Test hooks with multiple nodes."""
        dag = DAG("multi")
        hook1 = MagicMock()
        hook2 = MagicMock()

        @dag.node
        def node1(x: int) -> int:
            return x * 2

        @dag.node
        def node2(y: int) -> int:
            return y + 10

        # Set different hooks
        dag.set_node_hooks("node1", pre_execute=hook1)
        dag.set_node_hooks("node2", post_execute=hook2)

        # Connect nodes
        dag.connect("node1", "node2", input="y")

        # Execute
        result = dag.run(x=5)

        # Verify both hooks called
        hook1.assert_called_once()
        hook2.assert_called_once()
        assert result == 20  # (5 * 2) + 10

    def test_hooks_with_async_execution(self):
        """Test hooks with async node execution."""
        dag = DAG("async")
        pre_hook = MagicMock()
        post_hook = MagicMock()

        @dag.node
        async def async_process(x: int) -> int:
            return x * 3

        # Set hooks
        dag.set_node_hooks(
            "async_process", pre_execute=pre_hook, post_execute=post_hook
        )

        # Execute
        result = dag.run(x=4)

        # Hooks should work with async too
        pre_hook.assert_called_once()
        post_hook.assert_called_once()
        assert result == 12

    def test_hook_with_context_access(self):
        """Test hook can access execution context."""
        dag = DAG("context")
        MagicMock()

        @dag.node
        def process(x: int, context: Context) -> int:
            context.metadata["custom"] = "value"
            return x + 1

        def pre_hook(node, inputs):
            # Hook can access context through inputs
            ctx = inputs.get("context")
            if ctx:
                ctx.metadata["hook_called"] = True

        dag.set_node_hooks("process", pre_execute=pre_hook)

        # Execute
        result = dag.run(x=5)

        # Verify context was modified
        assert result == 6
        assert dag.context.metadata["custom"] == "value"
        assert dag.context.metadata["hook_called"] is True

    def test_hook_error_handling(self):
        """Test error handling in hooks."""
        dag = DAG("hook_error")

        def failing_pre_hook(node, inputs):
            raise RuntimeError("Hook failed")

        @dag.node
        def process(x: int) -> int:
            return x * 2

        dag.set_node_hooks("process", pre_execute=failing_pre_hook)

        # Hook error should propagate
        with pytest.raises(ExecutionError):
            dag.run(x=5)

    def test_hook_with_cached_node(self):
        """Test hooks with cached nodes."""
        dag = DAG("cached")
        pre_hook = MagicMock()
        post_hook = MagicMock()

        @dag.node
        @dag.cached_node()
        def expensive_compute(x: int) -> int:
            return x * 100

        dag.set_node_hooks(
            "expensive_compute", pre_execute=pre_hook, post_execute=post_hook
        )

        # First execution
        result1 = dag.run(x=5)
        assert result1 == 500

        # Second execution (should hit cache)
        result2 = dag.run(x=5)
        assert result2 == 500

        # Hooks should be called both times
        assert pre_hook.call_count == 2
        assert post_hook.call_count == 2

    def test_hook_with_node_retry(self):
        """Test hooks with node retry."""
        dag = DAG("retry")
        pre_hook = MagicMock()
        error_hook = MagicMock()

        attempts = 0

        @dag.node
        def flaky_node(x: int) -> int:
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise ValueError("Temporary failure")
            return x * 2

        # Set retry and hooks
        dag.nodes["flaky_node"].retry = 3
        dag.set_node_hooks("flaky_node", pre_execute=pre_hook, on_error=error_hook)

        # Execute
        result = dag.run(x=10)

        # Should succeed after retries
        assert result == 20

        # Pre-hook called for each attempt
        assert pre_hook.call_count == 3

        # Error hook called for each failure
        assert error_hook.call_count == 2

    def test_hook_with_timeout(self):
        """Test hooks with node timeout."""
        dag = DAG("timeout")
        pre_hook = MagicMock()
        error_hook = MagicMock()

        @dag.node
        def slow_node(x: int) -> int:
            import time

            time.sleep(1)  # Longer than timeout
            return x * 2

        # Set timeout and hooks
        dag.nodes["slow_node"].timeout = 0.1
        dag.set_node_hooks("slow_node", pre_execute=pre_hook, on_error=error_hook)

        # Execute should timeout
        with pytest.raises(ExecutionError):
            dag.run(x=5)

        # Hooks should be called
        pre_hook.assert_called_once()
        error_hook.assert_called_once()

        # Error should be TimeoutError
        error_args = error_hook.call_args[0]
        assert isinstance(error_args[2], TimeoutError)

    def test_hook_chaining(self):
        """Test multiple hooks can be chained."""
        dag = DAG("chain")

        results = []

        def hook1(node, inputs, result=None):
            results.append("hook1")
            return result

        def hook2(node, inputs, result=None):
            results.append("hook2")
            return result

        @dag.node
        def process(x: int) -> int:
            return x + 1

        # This should overwrite the previous hook
        dag.set_node_hooks("process", post_execute=hook1)
        dag.set_node_hooks("process", post_execute=hook2)

        # Execute
        dag.run(x=5)

        # Only the last hook should be called
        assert results == ["hook2"]

    def test_hook_with_conditional_node(self):
        """Test hooks with conditional nodes."""
        from fast_dag.core.types import ConditionalReturn

        dag = DAG("conditional")
        pre_hook = MagicMock()
        post_hook = MagicMock()

        @dag.node
        def condition(x: int) -> ConditionalReturn:
            return ConditionalReturn(condition=x > 0, value=x)

        @dag.node
        def positive(x: int) -> str:
            return "positive"

        @dag.node
        def negative(x: int) -> str:
            return "negative"

        # Set hooks on conditional node
        dag.set_node_hooks("condition", pre_execute=pre_hook, post_execute=post_hook)

        # Connect
        dag.connect("condition", "positive", output="on_true", input="x")
        dag.connect("condition", "negative", output="on_false", input="x")

        # Execute
        result = dag.run(x=5)

        # Hooks should be called
        pre_hook.assert_called_once()
        post_hook.assert_called_once()
        assert result == "positive"

    def test_hook_nonexistent_node(self):
        """Test setting hook on non-existent node."""
        dag = DAG("test")

        with pytest.raises(ValueError, match="Node 'nonexistent' not found"):
            dag.set_node_hooks("nonexistent", pre_execute=lambda _n, _i: None)

    def test_clear_hooks(self):
        """Test clearing hooks."""
        dag = DAG("clear")
        hook = MagicMock()

        @dag.node
        def process(x: int) -> int:
            return x * 2

        # Set hook
        dag.set_node_hooks("process", pre_execute=hook)

        # Clear hook
        dag.set_node_hooks("process", pre_execute=None)

        # Execute
        dag.run(x=5)

        # Hook should not be called
        hook.assert_not_called()

    def test_hook_with_nested_dag(self):
        """Test hooks with nested DAGs."""
        # Create inner DAG
        inner = DAG("inner")

        @inner.node
        def inner_process(x: int) -> int:
            return x * 2

        # Create outer DAG
        outer = DAG("outer")
        outer_hook = MagicMock()

        @outer.node
        def start() -> int:
            return 5

        # Add inner DAG as node
        outer.add_dag("inner_dag", inner)

        # Set hook on nested DAG node
        outer.set_node_hooks("inner_dag", pre_execute=outer_hook)

        # Connect
        outer.connect("start", "inner_dag", input="x")

        # Execute
        result = outer.run()

        # Outer hook should be called
        outer_hook.assert_called_once()
        assert result == 10
