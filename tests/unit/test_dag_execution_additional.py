"""
Additional unit tests for DAG execution functionality to improve coverage.
"""

import asyncio
from unittest.mock import patch

import pytest

from fast_dag import DAG, Context, ExecutionError, ValidationError
from fast_dag.core.exceptions import (
    CycleError,
)


class TestAsyncExecution:
    """Test async execution functionality"""

    @pytest.mark.asyncio
    async def test_run_async_simple(self):
        """Test basic async execution"""
        dag = DAG("async_test")

        @dag.node
        async def async_node(x: int) -> int:
            await asyncio.sleep(0.01)
            return x * 2

        result = await dag.run_async(x=5)
        assert result == 10

    @pytest.mark.asyncio
    async def test_run_async_with_context(self):
        """Test async execution with custom context"""
        dag = DAG("async_context")
        context = Context()

        @dag.node
        async def node1(x: int) -> int:
            return x + 1

        @dag.node
        async def node2(x: int) -> int:
            return x * 2

        dag.nodes["node1"] >> dag.nodes["node2"]

        result = await dag.run_async(x=5, context=context)
        assert result == 12
        assert context.get_result("node1") == 6
        assert context.get_result("node2") == 12

    @pytest.mark.asyncio
    async def test_run_async_validation_errors(self):
        """Test async execution with validation errors"""
        dag = DAG("invalid_dag")

        # Create a cycle
        @dag.node
        def node_a(x: int) -> int:
            return x

        @dag.node
        def node_b(x: int) -> int:
            return x

        dag.nodes["node_a"] >> dag.nodes["node_b"] >> dag.nodes["node_a"]

        with pytest.raises(CycleError):
            await dag.run_async(x=5)

    @pytest.mark.asyncio
    async def test_run_async_with_max_workers(self):
        """Test async execution with max_workers parameter"""
        dag = DAG("async_workers")

        @dag.node
        async def slow_node1() -> int:
            await asyncio.sleep(0.01)
            return 1

        @dag.node
        async def slow_node2() -> int:
            await asyncio.sleep(0.01)
            return 2

        @dag.node
        async def combine(a: int, b: int) -> int:
            return a + b

        dag.connect("slow_node1", "combine", input="a")
        dag.connect("slow_node2", "combine", input="b")

        result = await dag.run_async(max_workers=2)
        assert result == 3


class TestStepExecution:
    """Test step-by-step execution"""

    def test_step_execution(self):
        """Test executing DAG step by step"""
        dag = DAG("step_test")

        @dag.node
        def node1(x: int) -> int:
            return x * 2

        @dag.node
        def node2(x: int) -> int:
            return x + 10

        dag.nodes["node1"] >> dag.nodes["node2"]

        # Execute step by step
        context1, result1 = dag.step(x=5)
        assert result1 == 10  # node1 result
        assert context1.get_result("node1") == 10

        context2, result2 = dag.step(context=context1)
        assert result2 == 20  # node2 result
        assert context2.get_result("node2") == 20

        # No more steps
        context3, result3 = dag.step(context=context2)
        assert result3 is None

    def test_step_with_context(self):
        """Test step execution with provided context"""
        dag = DAG("step_context")
        context = Context()

        @dag.node
        def add_one(x: int) -> int:
            return x + 1

        updated_context, result = dag.step(context=context, x=10)
        assert result == 11
        assert updated_context.get_result("add_one") == 11

    def test_step_validation_error(self):
        """Test step execution with validation error"""
        dag = DAG("invalid_step")

        # Create disconnected node
        @dag.node
        def disconnected() -> int:
            return 42

        @dag.node
        def entry(x: int) -> int:
            return x

        # Don't connect them

        with pytest.raises(ValidationError):
            dag.step(x=5)


class TestDryRun:
    """Test dry run functionality"""

    def test_dry_run_basic(self):
        """Test basic dry run"""
        dag = DAG("dry_run_test")

        @dag.node
        def node1(x: int) -> int:
            return x * 2

        @dag.node
        def node2(x: int) -> int:
            return x + 10

        dag.nodes["node1"] >> dag.nodes["node2"]

        plan = dag.dry_run()
        assert plan["execution_order"] == ["node1", "node2"]
        assert plan["total_nodes"] == 2

    def test_dry_run_complex(self):
        """Test dry run with complex dependencies"""
        dag = DAG("complex_dry")

        @dag.node
        def a() -> int:
            return 1

        @dag.node
        def b() -> int:
            return 2

        @dag.node
        def c(x: int, y: int) -> int:
            return x + y

        @dag.node
        def d(z: int) -> int:
            return z * 2

        dag.connect("a", "c", input="x")
        dag.connect("b", "c", input="y")
        dag.connect("c", "d", input="z")

        plan = dag.dry_run()
        order = plan["execution_order"]
        # a and b can run in parallel, then c, then d
        assert len(order) == 4
        assert "a" in order[:2]
        assert "b" in order[:2]
        assert order[2] == "c"
        assert order[3] == "d"


class TestMetricsAndReset:
    """Test metrics and reset functionality"""

    def test_get_metrics(self):
        """Test getting execution metrics"""
        dag = DAG("metrics_test")

        @dag.node
        def slow_node(x: int) -> int:
            import time

            time.sleep(0.01)
            return x * 2

        result = dag.run(x=5)
        assert result == 10

        metrics = dag.get_metrics()
        assert "total_results" in metrics
        assert metrics["total_results"] == 1
        assert "metadata" in metrics

    def test_reset(self):
        """Test resetting DAG state"""
        dag = DAG("reset_test")

        @dag.node
        def node1(x: int) -> int:
            return x * 2

        # Run once
        result = dag.run(x=5)
        assert result == 10
        assert dag.context is not None
        assert dag.context.get_result("node1") == 10

        # Reset
        dag.reset()
        assert dag.context is not None  # Reset creates new context
        assert len(dag.context.results) == 0

        # Run again
        result = dag.run(x=3)
        assert result == 6


class TestClone:
    """Test DAG cloning"""

    def test_clone_basic(self):
        """Test basic DAG cloning"""
        dag = DAG("original", description="Original DAG")

        @dag.node
        def node1(x: int) -> int:
            return x * 2

        @dag.node
        def node2(x: int) -> int:
            return x + 10

        dag.nodes["node1"] >> dag.nodes["node2"]

        # Clone the DAG
        cloned = dag.clone()

        assert cloned.name == "original_clone"
        assert cloned.description == "Original DAG"
        assert len(cloned.nodes) == 2
        assert "node1" in cloned.nodes
        assert "node2" in cloned.nodes

        # Test execution
        result = cloned.run(x=5)
        assert result == 20

        # Original should not be affected
        assert len(dag.context.results) == 0  # Original context not used

    def test_clone_with_metadata(self):
        """Test cloning DAG with metadata"""
        dag = DAG("meta_dag", metadata={"version": "1.0", "author": "test"})

        @dag.node
        def process(x: int) -> int:
            return x

        cloned = dag.clone()
        assert cloned.metadata == {"version": "1.0", "author": "test"}
        # Ensure it's a copy
        cloned.metadata["version"] = "2.0"
        assert dag.metadata["version"] == "1.0"


class TestExecutionHelpers:
    """Test execution helper methods"""

    def test_can_execute_node_any_type(self):
        """Test _can_execute_node for ANY type nodes"""
        dag = DAG("any_test")

        @dag.any
        def any_node(inputs: list[int]) -> int:
            return sum(inputs)

        @dag.node
        def source1() -> int:
            return 1

        @dag.node
        def source2() -> int:
            return 2

        dag.connect("source1", "any_node")
        dag.connect("source2", "any_node")

        # Initialize context
        dag.context = Context()

        # Before any source executes, ANY node cannot execute
        assert not dag._can_execute_node(dag.nodes["any_node"], "any_node")

        # After one source executes, ANY node can execute
        dag.context.set_result("source1", 1)
        assert dag._can_execute_node(dag.nodes["any_node"], "any_node")

    def test_can_execute_node_all_type(self):
        """Test _can_execute_node for ALL type nodes"""
        dag = DAG("all_test")

        @dag.all
        def all_node(inputs: list[int]) -> int:
            return sum(inputs)

        @dag.node
        def source1() -> int:
            return 1

        @dag.node
        def source2() -> int:
            return 2

        dag.connect("source1", "all_node")
        dag.connect("source2", "all_node")

        # Initialize context
        dag.context = Context()

        # Before all sources execute, ALL node cannot execute
        assert not dag._can_execute_node(dag.nodes["all_node"], "all_node")

        # After one source executes, still cannot execute
        dag.context.set_result("source1", 1)
        assert not dag._can_execute_node(dag.nodes["all_node"], "all_node")

        # After all sources execute, can execute
        dag.context.set_result("source2", 2)
        assert dag._can_execute_node(dag.nodes["all_node"], "all_node")

    def test_prepare_node_inputs_any_type(self):
        """Test _prepare_node_inputs for ANY type nodes"""
        dag = DAG("any_prep")

        @dag.any
        def any_node(inputs: list[int]) -> int:
            return sum(inputs)

        @dag.node
        def source1() -> int:
            return 10

        @dag.node
        def source2() -> int:
            return 20

        dag.connect("source1", "any_node")
        dag.connect("source2", "any_node")

        # Initialize context
        dag.context = Context()
        dag.context.set_result("source1", 10)

        inputs, skip = dag._prepare_node_inputs(
            dag.nodes["any_node"], "any_node", [], {}, "raise"
        )

        assert not skip
        assert "inputs" in inputs
        assert inputs["inputs"] == [10]

    def test_prepare_node_inputs_all_type_with_missing(self):
        """Test _prepare_node_inputs for ALL type with missing inputs"""
        dag = DAG("all_prep")

        @dag.all
        def all_node(inputs: list[int]) -> int:
            return sum(inputs)

        @dag.node
        def source1() -> int:
            return 10

        @dag.node
        def source2() -> int:
            return 20

        dag.connect("source1", "all_node")
        dag.connect("source2", "all_node")

        # Initialize context with only one source
        dag.context = Context()
        dag.context.set_result("source1", 10)

        # Test with raise strategy
        with pytest.raises(ExecutionError) as exc_info:
            dag._prepare_node_inputs(dag.nodes["all_node"], "all_node", [], {}, "raise")
        assert "missing inputs from" in str(exc_info.value)

        # Test with skip strategy
        inputs, skip = dag._prepare_node_inputs(
            dag.nodes["all_node"], "all_node", [], {}, "skip"
        )
        assert skip

        # Test with continue strategy
        inputs, skip = dag._prepare_node_inputs(
            dag.nodes["all_node"], "all_node", [], {}, "continue"
        )
        assert not skip
        assert inputs["inputs"] == [10]

    def test_prepare_node_inputs_with_dict_output(self):
        """Test _prepare_node_inputs with dict outputs"""
        dag = DAG("dict_output")

        @dag.node
        def source() -> dict[str, int]:
            return {"a": 1, "b": 2}

        @dag.node
        def consumer(value: int) -> int:
            return value * 10

        dag.connect("source", "consumer", output="b", input="value")

        # Initialize context
        dag.context = Context()
        dag.context.set_result("source", {"a": 1, "b": 2})

        inputs, skip = dag._prepare_node_inputs(
            dag.nodes["consumer"], "consumer", [], {}, "raise"
        )

        assert not skip
        assert inputs["value"] == 2

    def test_prepare_node_inputs_missing_dict_key(self):
        """Test _prepare_node_inputs with missing dict key"""
        dag = DAG("missing_key")

        @dag.node
        def source() -> dict[str, int]:
            return {"a": 1}

        @dag.node
        def consumer(value: int) -> int:
            return value * 10

        dag.connect("source", "consumer", output="b", input="value")

        # Initialize context
        dag.context = Context()
        dag.context.set_result("source", {"a": 1})

        # Test with raise strategy
        with pytest.raises(ExecutionError) as exc_info:
            dag._prepare_node_inputs(dag.nodes["consumer"], "consumer", [], {}, "raise")
        assert "Output 'b' not found" in str(exc_info.value)

        # Test with skip strategy
        inputs, skip = dag._prepare_node_inputs(
            dag.nodes["consumer"], "consumer", [], {}, "skip"
        )
        assert skip

        # Test with continue strategy
        inputs, skip = dag._prepare_node_inputs(
            dag.nodes["consumer"], "consumer", [], {}, "continue"
        )
        assert not skip
        assert inputs["value"] is None

    def test_get_parallel_groups(self):
        """Test _get_parallel_groups method"""
        dag = DAG("parallel_groups")

        @dag.node
        def a() -> int:
            return 1

        @dag.node
        def b() -> int:
            return 2

        @dag.node
        def c(x: int, y: int) -> int:
            return x + y

        @dag.node
        def d(z: int) -> int:
            return z * 2

        dag.connect("a", "c", input="x")
        dag.connect("b", "c", input="y")
        dag.connect("c", "d", input="z")

        groups = dag._get_parallel_groups()

        # Should have 3 groups: [a,b], [c], [d]
        assert len(groups) == 3
        assert set(groups[0]) == {"a", "b"}
        assert groups[1] == ["c"]
        assert groups[2] == ["d"]

    def test_get_parallel_groups_with_cycle(self):
        """Test _get_parallel_groups with cycle (should handle gracefully)"""
        dag = DAG("cycle_groups")

        @dag.node
        def a(x: int) -> int:
            return x

        @dag.node
        def b(x: int) -> int:
            return x

        # Create cycle
        dag.nodes["a"] >> dag.nodes["b"] >> dag.nodes["a"]

        groups = dag._get_parallel_groups()
        # Should return empty or partial groups when cycle detected
        assert len(groups) < 2  # Won't complete all nodes


class TestExecutionEdgeCases:
    """Test edge cases in execution"""

    def test_run_with_inputs_dict(self):
        """Test run with inputs parameter as dict"""
        dag = DAG("inputs_dict")

        @dag.node
        def process(x: int, y: int) -> int:
            return x + y

        result = dag.run(inputs={"x": 5, "y": 3})
        assert result == 8

    def test_run_with_custom_error_strategy(self):
        """Test run with custom error strategy"""
        dag = DAG("error_strategy")

        @dag.node
        def failing_node() -> int:
            raise ValueError("Test error")

        @dag.node
        def dependent(x: int) -> int:
            return x * 2

        dag.connect("failing_node", "dependent", input="x")

        # With skip strategy, should skip dependent node
        with pytest.raises(ExecutionError):
            dag.run(error_strategy="skip")

    def test_run_value_error_wrapping(self):
        """Test that ValueError is wrapped in ExecutionError"""
        dag = DAG("value_error")

        @dag.node
        def bad_node() -> int:
            # This will cause a ValueError in the runner
            return 42  # Return valid int

        # Mock the runner to raise ValueError
        with patch.object(dag, "_get_runner") as mock_runner:
            mock_runner.return_value.run.side_effect = ValueError("Test value error")

            with pytest.raises(ExecutionError) as exc_info:
                dag.run()
            assert "Test value error" in str(exc_info.value)

    def test_types_compatible_with_union(self):
        """Test _types_compatible with Union types"""
        dag = DAG("union_types")

        # Test with None union
        assert dag._types_compatible(int, int | None)
        assert dag._types_compatible(str, str | None)

        # Test with multiple types
        assert dag._types_compatible(int, int | str)
        assert dag._types_compatible(str, int | str)
        assert not dag._types_compatible(float, int | str)

    def test_check_connection_detailed(self):
        """Test check_connection method with various scenarios"""
        dag = DAG("check_conn")

        @dag.node
        def source() -> dict[str, int]:
            return {"out1": 1, "out2": 2}

        @dag.node
        def target(in1: str) -> int:  # Type mismatch
            return len(in1)

        # Test non-existent nodes
        issues = dag.check_connection("missing", "target")
        assert any("does not exist" in issue for issue in issues)

        # Test missing output
        issues = dag.check_connection("source", "target", output="missing_out")
        assert any("does not have output" in issue for issue in issues)

        # Test missing input
        issues = dag.check_connection("source", "target", input="missing_in")
        assert any("does not have input" in issue for issue in issues)

        # Test type mismatch
        issues = dag.check_connection("source", "target", output="out1", input="in1")
        assert any("Type mismatch" in issue for issue in issues)


class TestNodeExecutionInContext:
    """Test node execution with context parameter"""

    def test_node_with_context_parameter(self):
        """Test node that accepts context as parameter"""
        dag = DAG("context_param")

        @dag.node
        def needs_context(x: int, context: Context) -> int:
            # Store intermediate result in context
            context.metadata["intermediate"] = x * 2
            return x * 3

        result = dag.run(x=5)
        assert result == 15
        assert dag.context.metadata.get("intermediate") == 10

    def test_entry_node_with_kwargs(self):
        """Test entry node receiving inputs from kwargs"""
        dag = DAG("entry_kwargs")

        @dag.node
        def entry(a: int, b: int) -> int:
            return a + b

        result = dag.run(a=5, b=10)
        assert result == 15

        result = dag.run(a=5, b=20)
        assert result == 25
