"""
Unit tests for DAG execution functionality.
"""

import asyncio

import pytest

from fast_dag import DAG, ConditionalReturn, Context, ExecutionError, TimeoutError


class TestBasicExecution:
    """Test basic DAG execution"""

    def test_simple_linear_execution(self):
        """Test executing a simple linear workflow"""
        dag = DAG("simple")

        @dag.node
        def start(x: int) -> int:
            return x * 2

        @dag.node
        def middle(x: int) -> int:
            return x + 10

        @dag.node
        def end(x: int) -> int:
            return x * 3

        # Connect: start -> middle -> end
        dag.nodes["start"] >> dag.nodes["middle"] >> dag.nodes["end"]

        result = dag.run(x=5)

        # Should execute: 5 * 2 = 10, 10 + 10 = 20, 20 * 3 = 60
        assert result == 60

    def test_execution_with_multiple_inputs(self):
        """Test DAG with nodes having multiple inputs"""
        dag = DAG("multi_input")

        @dag.node
        def input_a() -> int:
            return 5

        @dag.node
        def input_b() -> int:
            return 3

        @dag.node
        def combine(a: int, b: int) -> int:
            return a + b

        dag.connect("input_a", "combine", input="a")
        dag.connect("input_b", "combine", input="b")

        result = dag.run()
        assert result == 8

    def test_execution_with_initial_inputs(self):
        """Test execution with initial input values"""
        dag = DAG("with_inputs")

        @dag.node
        def process(value: int, multiplier: int) -> int:
            return value * multiplier

        result = dag.run(value=10, multiplier=5)
        assert result == 50

    def test_execution_with_custom_context(self):
        """Test execution with pre-populated context"""
        dag = DAG("with_context")

        @dag.node
        def use_context(x: int, context: Context) -> int:
            previous = context.get("previous_result", 0)
            return x + previous

        # Create context with data
        ctx = Context()
        ctx.set_result("previous_result", 100)

        result = dag.run(x=50, context=ctx)
        assert result == 150


class TestResultAccess:
    """Test accessing results after execution"""

    def test_dict_access_to_results(self):
        """Test dictionary-style access to node results"""
        dag = DAG("test")

        @dag.node
        def step1() -> int:
            return 42

        @dag.node
        def step2(x: int) -> str:
            return f"Result: {x}"

        dag.connect("step1", "step2")

        dag.run()

        # Access results
        assert dag["step1"] == 42
        assert dag["step2"] == "Result: 42"

    def test_get_method_access(self):
        """Test get method with default"""
        dag = DAG("test")

        @dag.node
        def compute() -> int:
            return 123

        dag.run()

        assert dag.get("compute") == 123
        assert dag.get("missing", default="not found") == "not found"

    def test_contains_operator(self):
        """Test 'in' operator for checking result existence"""
        dag = DAG("test")

        @dag.node
        def executed() -> int:
            return 1

        dag.run()

        assert "executed" in dag
        assert "not_executed" not in dag

    def test_results_property(self):
        """Test accessing all results"""
        dag = DAG("test")

        @dag.node
        def a() -> int:
            return 1

        @dag.node
        def b(x: int) -> int:
            return x + 1

        dag.connect("a", "b")
        dag.run()

        results = dag.results
        assert results == {"a": 1, "b": 2}

    def test_context_after_execution(self):
        """Test accessing context after execution"""
        dag = DAG("test")

        @dag.node
        def task() -> int:
            return 42

        dag.run()

        ctx = dag.context
        assert ctx is not None
        assert ctx["task"] == 42


class TestConditionalExecution:
    """Test conditional branching execution"""

    def test_conditional_true_branch(self):
        """Test execution following true branch"""
        dag = DAG("conditional")

        @dag.node
        def start() -> int:
            return 10

        @dag.condition()
        def check(x: int) -> ConditionalReturn:
            return ConditionalReturn(condition=x > 5, value=x)

        @dag.node
        def true_path(x: int) -> str:
            return f"Large: {x}"

        @dag.node
        def false_path(x: int) -> str:
            return f"Small: {x}"

        dag.connect("start", "check")
        dag.connect("check", "true_path", output="true")
        dag.connect("check", "false_path", output="false")

        dag.run()

        # Should take true path
        assert "true_path" in dag
        assert dag["true_path"] == "Large: 10"
        assert "false_path" not in dag  # Should not execute

    def test_conditional_false_branch(self):
        """Test execution following false branch"""
        dag = DAG("conditional")

        @dag.node
        def start() -> int:
            return 3

        @dag.condition()
        def check(x: int) -> ConditionalReturn:
            return ConditionalReturn(condition=x > 5, value=x)

        @dag.node
        def true_path(x: int) -> str:
            return f"Large: {x}"

        @dag.node
        def false_path(x: int) -> str:
            return f"Small: {x}"

        dag.connect("start", "check")
        dag.connect("check", "true_path", output="true")
        dag.connect("check", "false_path", output="false")

        dag.run()

        # Should take false path
        assert "false_path" in dag
        assert dag["false_path"] == "Small: 3"
        assert "true_path" not in dag  # Should not execute


class TestParallelExecution:
    """Test parallel branch execution"""

    def test_parallel_branches(self):
        """Test executing parallel branches"""
        dag = DAG("parallel")

        @dag.node
        def start() -> int:
            return 10

        @dag.node
        def branch_a(x: int) -> int:
            return x * 2

        @dag.node
        def branch_b(x: int) -> int:
            return x + 5

        @dag.node
        def merge(a: int, b: int) -> int:
            return a + b

        # start -> [branch_a, branch_b] -> merge
        dag.nodes["start"] >> [dag.nodes["branch_a"], dag.nodes["branch_b"]]
        dag.connect("branch_a", "merge", input="a")
        dag.connect("branch_b", "merge", input="b")

        result = dag.run()

        # branch_a: 10 * 2 = 20
        # branch_b: 10 + 5 = 15
        # merge: 20 + 15 = 35
        assert result == 35
        assert dag["branch_a"] == 20
        assert dag["branch_b"] == 15


class TestAsyncExecution:
    """Test async execution functionality"""

    @pytest.mark.asyncio
    async def test_async_node_execution(self):
        """Test executing async nodes"""
        dag = DAG("async_test")

        @dag.node
        async def async_start() -> int:
            await asyncio.sleep(0.01)
            return 5

        @dag.node
        async def async_process(x: int) -> int:
            await asyncio.sleep(0.01)
            return x * 2

        dag.connect("async_start", "async_process")

        result = await dag.run_async()
        assert result == 10

    @pytest.mark.asyncio
    async def test_mixed_sync_async_execution(self):
        """Test mixing sync and async nodes"""
        dag = DAG("mixed")

        @dag.node
        def sync_start() -> int:
            return 3

        @dag.node
        async def async_middle(x: int) -> int:
            await asyncio.sleep(0.01)
            return x * 2

        @dag.node
        def sync_end(x: int) -> int:
            return x + 10

        dag.nodes["sync_start"] >> dag.nodes["async_middle"] >> dag.nodes["sync_end"]

        result = await dag.run_async()
        assert result == 16  # (3 * 2) + 10


class TestErrorHandling:
    """Test error handling during execution"""

    def test_node_execution_error(self):
        """Test handling errors in node execution"""
        dag = DAG("error_test")

        @dag.node
        def failing_node(x: int) -> int:
            raise ValueError("Intentional error")

        with pytest.raises(ExecutionError) as exc_info:
            dag.run(x=5)

        assert "failing_node" in str(exc_info.value)
        assert "Intentional error" in str(exc_info.value)

    def test_error_with_continue_strategy(self):
        """Test continuing execution after error"""
        dag = DAG("continue_on_error")

        @dag.node
        def start() -> int:
            return 1

        @dag.node
        def will_fail(x: int) -> int:
            raise ValueError("Fail")

        @dag.node
        def independent() -> int:
            return 42

        dag.connect("start", "will_fail")
        # independent has no dependencies

        # Run with continue strategy
        dag.run(error_strategy="continue")

        # Should still execute independent node
        assert dag.get("independent") == 42
        assert dag.get("will_fail") is None  # Failed node

    def test_error_with_continue_none_strategy(self):
        """Test continue_none error strategy"""
        dag = DAG("continue_none_test")

        @dag.node
        def start() -> int:
            return 10

        @dag.node
        def will_fail(x: int) -> int:
            raise ValueError("Fail")

        @dag.node
        def dependent(x: int | None) -> str:
            if x is None:
                return "Got None"
            return f"Got {x}"

        @dag.node
        def independent() -> int:
            return 42

        dag.connect("start", "will_fail")
        dag.connect("will_fail", "dependent")
        # independent has no dependencies

        # Run with continue_none strategy
        dag.run(error_strategy="continue_none")

        # Should execute all nodes
        assert dag.get("start") == 10
        assert dag.get("will_fail") is None  # Failed node gets None
        assert dag.get("dependent") == "Got None"  # Dependent gets None
        assert dag.get("independent") == 42

        # Should have error metadata
        assert "will_fail_error" in dag.context.metadata
        assert dag.context.metadata["will_fail_error"] == "Fail"

    def test_error_with_continue_skip_strategy(self):
        """Test continue_skip error strategy"""
        dag = DAG("continue_skip_test")

        @dag.node
        def start() -> int:
            return 10

        @dag.node
        def will_fail(x: int) -> int:
            raise ValueError("Fail")

        @dag.node
        def dependent(x: int) -> str:
            return f"Got {x}"

        @dag.node
        def independent() -> int:
            return 42

        dag.connect("start", "will_fail")
        dag.connect("will_fail", "dependent")
        # independent has no dependencies

        # Run with continue_skip strategy
        dag.run(error_strategy="continue_skip")

        # Should execute start and independent, but skip dependent
        assert dag.get("start") == 10
        assert dag.get("will_fail") is None  # Failed node not in results
        assert dag.get("dependent") is None  # Dependent was skipped
        assert dag.get("independent") == 42

        # Should have error metadata
        assert "will_fail_error" in dag.context.metadata
        assert dag.context.metadata["will_fail_error"] == "Fail"

    def test_timeout_handling(self):
        """Test timeout during execution"""
        dag = DAG("timeout_test")

        @dag.node(timeout=0.1)
        async def slow_node() -> int:
            await asyncio.sleep(1.0)  # Will timeout
            return 42

        with pytest.raises(TimeoutError, match="timed out"):
            asyncio.run(dag.run_async())


class TestMetricsCollection:
    """Test execution metrics collection"""

    def test_execution_metrics(self):
        """Test collecting execution metrics"""
        dag = DAG("metrics_test")

        @dag.node
        def step1() -> int:
            return 1

        @dag.node
        def step2(x: int) -> int:
            return x + 1

        dag.connect("step1", "step2")

        dag.run()

        metrics = dag.context.metrics

        # Should have execution times
        assert "total_duration" in metrics
        assert "node_times" in metrics
        assert "step1" in metrics["node_times"]
        assert "step2" in metrics["node_times"]

        # Should have execution order
        assert "execution_order" in metrics
        assert metrics["execution_order"] == ["step1", "step2"]

    def test_parallel_execution_metrics(self):
        """Test metrics for parallel execution"""
        dag = DAG("parallel_metrics")

        @dag.node
        def a() -> int:
            return 1

        @dag.node
        def b() -> int:
            return 2

        @dag.node
        def c(x: int, y: int) -> int:
            return x + y

        dag.connect("a", "c", input="x")
        dag.connect("b", "c", input="y")

        # Run in parallel mode
        dag.run(mode="parallel")

        metrics = dag.context.metrics

        # Should show parallel execution
        assert "parallel_groups" in metrics
        assert ["a", "b"] in metrics["parallel_groups"] or ["b", "a"] in metrics[
            "parallel_groups"
        ]
