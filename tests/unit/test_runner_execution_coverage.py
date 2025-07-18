"""
Tests for runner execution to improve coverage.
"""

import asyncio
from unittest.mock import MagicMock, patch

import pytest

from fast_dag import DAG, Context
from fast_dag.core.exceptions import ExecutionError
from fast_dag.runner import DAGRunner, ExecutionMode


class TestDAGRunnerCoverage:
    """Test DAG executor comprehensively"""

    def test_run_with_async_mode_new_event_loop(self):
        """Test run with async mode creating new event loop (lines 36-43)"""
        dag = DAG("async_test")

        @dag.node
        def task() -> int:
            return 42

        executor = DAGRunner(dag, mode=ExecutionMode.ASYNC)

        # Mock the _run_async method to avoid actual async execution
        with patch.object(executor, "_run_async", return_value=42):
            result = executor.run()
            assert result == 42

    def test_run_with_unknown_mode(self):
        """Test run with unknown execution mode (line 45)"""
        dag = DAG("unknown_mode")

        @dag.node
        def task() -> int:
            return 1

        executor = DAGRunner(dag, mode=ExecutionMode.SEQUENTIAL)

        # Mock mode.value to return unknown value
        executor.mode = MagicMock()
        executor.mode.value = "unknown"

        with pytest.raises(ValueError, match="Unknown execution mode:"):
            executor.run()

    def test_run_async_with_timeout(self):
        """Test run_async with timeout (lines 68-71)"""
        dag = DAG("timeout_test")

        @dag.node
        def task() -> int:
            return 42

        executor = DAGRunner(dag, mode=ExecutionMode.ASYNC, timeout=1.0)

        # Mock the _run_async method
        with patch.object(executor, "_run_async", return_value=42):
            result = asyncio.run(executor.run_async())
            assert result == 42

    def test_run_async_timeout_error(self):
        """Test run_async with timeout error (lines 74-78)"""
        dag = DAG("timeout_error")

        @dag.node
        def task() -> int:
            return 42

        executor = DAGRunner(dag, mode=ExecutionMode.ASYNC, timeout=0.1)

        # Mock _run_async to raise TimeoutError
        async def slow_async(*args, **kwargs):
            await asyncio.sleep(0.2)
            return 42

        with (
            patch.object(executor, "_run_async", side_effect=slow_async),
            pytest.raises(TimeoutError, match="DAG execution exceeded timeout of 0.1s"),
        ):
            asyncio.run(executor.run_async())

    def test_sequential_validation_error(self):
        """Test sequential execution with validation error (lines 92-94)"""
        dag = DAG("invalid_dag")

        @dag.node
        def task() -> int:
            return 1

        executor = DAGRunner(dag, mode=ExecutionMode.SEQUENTIAL)

        # Mock validate to return errors
        with (
            patch.object(dag, "validate", return_value=["validation error"]),
            pytest.raises(ExecutionError, match="Cannot execute invalid DAG"),
        ):
            executor.run()

    def test_sequential_should_skip_node(self):
        """Test sequential execution skipping node (lines 110-111)"""
        dag = DAG("skip_test")

        @dag.node
        def task() -> int:
            return 1

        executor = DAGRunner(dag, mode=ExecutionMode.SEQUENTIAL)

        # Mock _should_skip_node to return True
        with patch.object(executor, "_should_skip_node", return_value=True):
            result = executor.run()
            # Should return None since all nodes are skipped
            assert result is None

    def test_sequential_node_inputs_none(self):
        """Test sequential execution with None node inputs (lines 119-120)"""
        dag = DAG("none_inputs")

        @dag.node
        def task() -> int:
            return 1

        executor = DAGRunner(dag, mode=ExecutionMode.SEQUENTIAL)

        # Mock _prepare_node_inputs to return None
        with patch.object(executor, "_prepare_node_inputs", return_value=None):
            result = executor.run()
            # Should return None since no nodes are executed
            assert result is None

    def test_sequential_error_strategy_stop_type_error(self):
        """Test sequential execution with TypeError that should bubble up (lines 135-140)"""
        dag = DAG("type_error")

        @dag.node
        def task() -> int:
            return 1

        executor = DAGRunner(dag, mode=ExecutionMode.SEQUENTIAL, error_strategy="stop")

        # Mock _execute_node_with_retry to raise TypeError
        type_error = TypeError("missing 1 required positional argument: 'x'")
        with (
            patch.object(executor, "_execute_node_with_retry", side_effect=type_error),
            pytest.raises(TypeError, match="missing 1 required positional argument"),
        ):
            executor.run()

    def test_sequential_error_strategy_stop_execution_error(self):
        """Test sequential execution with ExecutionError that shouldn't be wrapped (lines 142-143)"""
        dag = DAG("execution_error")

        @dag.node
        def task() -> int:
            return 1

        executor = DAGRunner(dag, mode=ExecutionMode.SEQUENTIAL, error_strategy="stop")

        # Mock _execute_node_with_retry to raise ExecutionError
        execution_error = ExecutionError("test execution error")
        with (
            patch.object(
                executor, "_execute_node_with_retry", side_effect=execution_error
            ),
            pytest.raises(ExecutionError, match="test execution error"),
        ):
            executor.run()

    def test_sequential_error_strategy_stop_runtime_error_with_node_name(self):
        """Test sequential execution with RuntimeError that already has node name (lines 148-152)"""
        dag = DAG("runtime_error")

        @dag.node
        def task() -> int:
            return 1

        executor = DAGRunner(dag, mode=ExecutionMode.SEQUENTIAL, error_strategy="stop")

        # Mock _execute_node_with_retry to raise RuntimeError with node name
        runtime_error = RuntimeError("Node 'task': some error")
        with (
            patch.object(
                executor, "_execute_node_with_retry", side_effect=runtime_error
            ),
            pytest.raises(RuntimeError, match="Node 'task': some error"),
        ):
            executor.run()

    def test_sequential_error_strategy_stop_runtime_error_without_node_name(self):
        """Test sequential execution with RuntimeError without node name (lines 147-151)"""
        dag = DAG("runtime_error")

        @dag.node
        def task() -> int:
            return 1

        executor = DAGRunner(dag, mode=ExecutionMode.SEQUENTIAL, error_strategy="stop")

        # Mock _execute_node_with_retry to raise RuntimeError without node name
        runtime_error = RuntimeError("some error")
        with (
            patch.object(
                executor, "_execute_node_with_retry", side_effect=runtime_error
            ),
            pytest.raises(RuntimeError, match="Node 'task': some error"),
        ):
            executor.run()

    def test_sequential_error_strategy_stop_other_error(self):
        """Test sequential execution with other error types (lines 153-156)"""
        dag = DAG("other_error")

        @dag.node
        def task() -> int:
            return 1

        executor = DAGRunner(dag, mode=ExecutionMode.SEQUENTIAL, error_strategy="stop")

        # Mock _execute_node_with_retry to raise other error
        other_error = ZeroDivisionError("division by zero")
        with (
            patch.object(executor, "_execute_node_with_retry", side_effect=other_error),
            pytest.raises(
                ExecutionError, match="Error executing node 'task': division by zero"
            ),
        ):
            executor.run()

    def test_sequential_error_strategy_continue(self):
        """Test sequential execution with continue error strategy (lines 157-159)"""
        dag = DAG("continue_error")

        @dag.node
        def task1() -> int:
            return 1

        @dag.node
        def task2() -> int:
            return 2

        executor = DAGRunner(
            dag, mode=ExecutionMode.SEQUENTIAL, error_strategy="continue"
        )

        # Mock _execute_node_with_retry to raise error on first task
        def side_effect(*args, **kwargs):
            if args[0].name == "task1":
                raise ValueError("task1 error")
            return 2

        with patch.object(
            executor, "_execute_node_with_retry", side_effect=side_effect
        ):
            result = executor.run()
            assert result == 2
            # Check error was recorded in metadata
            assert "task1_error" in executor.dag.context.metadata

    def test_sequential_error_strategy_continue_none(self):
        """Test sequential execution with continue_none error strategy (lines 160-163)"""
        dag = DAG("continue_none")

        @dag.node
        def task1() -> int:
            return 1

        @dag.node
        def task2() -> int:
            return 2

        executor = DAGRunner(
            dag, mode=ExecutionMode.SEQUENTIAL, error_strategy="continue_none"
        )

        # Mock _execute_node_with_retry to raise error on first task
        def side_effect(*args, **kwargs):
            if args[0].name == "task1":
                raise ValueError("task1 error")
            return 2

        with patch.object(
            executor, "_execute_node_with_retry", side_effect=side_effect
        ):
            result = executor.run()
            assert result == 2
            # Check error was recorded and result set to None
            assert "task1_error" in executor.dag.context.metadata
            assert executor.dag.context.get_result("task1") is None

    def test_sequential_error_strategy_continue_skip(self):
        """Test sequential execution with continue_skip error strategy (lines 164-167)"""
        dag = DAG("continue_skip")

        @dag.node
        def task1() -> int:
            return 1

        @dag.node
        def task2() -> int:
            return 2

        executor = DAGRunner(
            dag, mode=ExecutionMode.SEQUENTIAL, error_strategy="continue_skip"
        )

        # Mock _execute_node_with_retry to raise error on first task
        def side_effect(*args, **kwargs):
            if args[0].name == "task1":
                raise ValueError("task1 error")
            return 2

        with patch.object(
            executor, "_execute_node_with_retry", side_effect=side_effect
        ):
            result = executor.run()
            assert result == 2
            # Check error was recorded in metadata
            assert "task1_error" in executor.dag.context.metadata

    def test_sequential_error_strategy_unknown(self):
        """Test sequential execution with unknown error strategy (lines 168-171)"""
        dag = DAG("unknown_strategy")

        @dag.node
        def task() -> int:
            return 1

        executor = DAGRunner(dag, mode=ExecutionMode.SEQUENTIAL, error_strategy="stop")

        # Mock error_strategy to return unknown value
        executor.error_strategy = "unknown"

        # Mock _execute_node_with_retry to raise error
        with (
            patch.object(
                executor,
                "_execute_node_with_retry",
                side_effect=ValueError("test error"),
            ),
            pytest.raises(ValueError, match="Unknown error strategy: unknown"),
        ):
            executor.run()

    def test_parallel_validation_error(self):
        """Test parallel execution with validation error (lines 202)"""
        dag = DAG("parallel_invalid")

        @dag.node
        def task() -> int:
            return 1

        executor = DAGRunner(dag, mode=ExecutionMode.PARALLEL)

        # Mock validate to return errors
        with (
            patch.object(dag, "validate", return_value=["validation error"]),
            pytest.raises(ExecutionError, match="Cannot execute invalid DAG"),
        ):
            executor.run()

    def test_parallel_get_parallel_groups(self):
        """Test parallel execution getting parallel groups (lines 225)"""
        dag = DAG("parallel_groups")

        @dag.node
        def task1() -> int:
            return 1

        @dag.node
        def task2() -> int:
            return 2

        executor = DAGRunner(dag, mode=ExecutionMode.PARALLEL)

        # Mock _compute_dependency_levels on the executor to return empty list
        with patch.object(executor, "_compute_dependency_levels", return_value=[]):
            result = executor.run()
            # With no parallel groups, no nodes execute, so result should be None
            assert result is None

    def test_parallel_executor_error_handling(self):
        """Test parallel execution with ThreadPoolExecutor error handling (lines 246-274)"""
        dag = DAG("parallel_error")

        @dag.node
        def task1() -> int:
            return 1

        @dag.node
        def task2() -> int:
            return 2

        executor = DAGRunner(dag, mode=ExecutionMode.PARALLEL, error_strategy="stop")

        # Mock _compute_dependency_levels on the executor to return groups
        with (
            patch.object(
                executor,
                "_compute_dependency_levels",
                return_value=[["task1", "task2"]],
            ),
            patch.object(
                executor,
                "_execute_node_with_retry",
                side_effect=RuntimeError("parallel error"),
            ),
            pytest.raises(
                ExecutionError, match="Error executing node 'task1': parallel error"
            ),
        ):
            executor.run()

    def test_async_execution_validation_error(self):
        """Test async execution with validation error (lines 301)"""
        dag = DAG("async_invalid")

        @dag.node
        def task() -> int:
            return 1

        executor = DAGRunner(dag, mode=ExecutionMode.ASYNC)

        # Mock validate to return errors
        with (
            patch.object(dag, "validate", return_value=["validation error"]),
            pytest.raises(ExecutionError, match="Cannot execute invalid DAG"),
        ):
            asyncio.run(executor.run_async())

    def test_async_execution_timeout_handling(self):
        """Test async execution with timeout handling (lines 322)"""
        dag = DAG("async_timeout")

        @dag.node
        def task() -> int:
            return 1

        executor = DAGRunner(dag, mode=ExecutionMode.ASYNC, timeout=0.1)

        # Mock _run_async to simulate timeout
        async def slow_execution(*args, **kwargs):
            await asyncio.sleep(0.2)
            return 1

        with (
            patch.object(executor, "_run_async", side_effect=slow_execution),
            pytest.raises(TimeoutError, match="DAG execution exceeded timeout of 0.1s"),
        ):
            asyncio.run(executor.run_async())

    def test_async_execution_get_parallel_groups(self):
        """Test async execution getting parallel groups (lines 349)"""
        dag = DAG("async_groups")

        @dag.node
        def task1() -> int:
            return 1

        @dag.node
        def task2() -> int:
            return 2

        executor = DAGRunner(dag, mode=ExecutionMode.ASYNC)

        # Mock _compute_dependency_levels on the executor to return empty list
        with patch.object(executor, "_compute_dependency_levels", return_value=[]):
            result = asyncio.run(executor.run_async())
            assert result is None

    def test_async_execution_error_handling(self):
        """Test async execution with error handling (lines 354, 359-363)"""
        dag = DAG("async_error")

        @dag.node
        def task() -> int:
            return 1

        executor = DAGRunner(dag, mode=ExecutionMode.ASYNC, error_strategy="stop")

        # Create async side effect for the async method
        async def async_error(*args, **kwargs):
            raise RuntimeError("async error")

        # Mock _compute_dependency_levels on the executor to return groups
        with (
            patch.object(
                executor, "_compute_dependency_levels", return_value=[["task"]]
            ),
            patch.object(executor, "_execute_node_async", side_effect=async_error),
            pytest.raises(
                ExecutionError, match="Error executing node 'task': async error"
            ),
        ):
            asyncio.run(executor.run_async())

    def test_execution_with_context_provided(self):
        """Test execution with context provided"""
        dag = DAG("with_context")

        @dag.node
        def task() -> int:
            return 42

        executor = DAGRunner(dag, mode=ExecutionMode.SEQUENTIAL)

        # Provide custom context
        context = Context()
        context.metadata["test"] = "value"

        result = executor.run(context=context)
        assert result == 42
        assert executor.dag.context.metadata["test"] == "value"

    def test_execution_with_kwargs(self):
        """Test execution with kwargs for entry nodes"""
        dag = DAG("with_kwargs")

        @dag.node
        def task(x: int) -> int:
            return x * 2

        executor = DAGRunner(dag, mode=ExecutionMode.SEQUENTIAL)

        result = executor.run(x=5)
        assert result == 10

    def test_metrics_tracking(self):
        """Test that metrics are properly tracked"""
        dag = DAG("metrics_test")

        @dag.node
        def task() -> int:
            return 1

        executor = DAGRunner(dag, mode=ExecutionMode.SEQUENTIAL)

        result = executor.run()
        assert result == 1

        # Check metrics were created and populated
        assert hasattr(executor, "_metrics")
        assert executor._metrics.nodes_executed == 1
        assert executor._metrics.total_duration > 0
