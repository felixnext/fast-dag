"""
Tests for runner utils to improve coverage.
"""

from unittest.mock import MagicMock

import pytest

from fast_dag import DAG, Context
from fast_dag.core.exceptions import ExecutionError
from fast_dag.core.types import ConditionalReturn
from fast_dag.runner import DAGRunner


class TestRunnerUtilsCoverage:
    """Test runner utils comprehensively"""

    def test_should_skip_node_any_node_type(self):
        """Test _should_skip_node for ANY node type (lines 21-22)"""
        dag = DAG("any_node_test")

        @dag.any
        def any_task() -> int:
            return 1

        runner = DAGRunner(dag)
        context = Context()

        # ANY nodes should never be skipped
        assert not runner._should_skip_node("any_task", context)

    def test_should_skip_node_conditional_false_true_branch(self):
        """Test _should_skip_node for conditional false on true branch (lines 30-36)"""
        dag = DAG("conditional_test")

        @dag.condition
        def check() -> ConditionalReturn:
            return ConditionalReturn(condition=False, value=1)

        @dag.node
        def on_true(x: int) -> int:
            return x + 1

        # Connect conditional to true branch
        dag.connect("check", "on_true", output="on_true", input="x")

        runner = DAGRunner(dag)
        context = Context()

        # Set conditional result to false
        context.set_result("check", ConditionalReturn(condition=False, value=None))

        # Should skip node on true branch when condition is false
        assert runner._should_skip_node("on_true", context)

    def test_should_skip_node_conditional_true_false_branch(self):
        """Test _should_skip_node for conditional true on false branch (lines 33-36)"""
        dag = DAG("conditional_test")

        @dag.condition
        def check() -> ConditionalReturn:
            return ConditionalReturn(condition=True, value=1)

        @dag.node
        def on_false(x: int) -> int:
            return x + 1

        # Connect conditional to false branch
        dag.connect("check", "on_false", output="on_false", input="x")

        runner = DAGRunner(dag)
        context = Context()

        # Set conditional result to true
        context.set_result("check", ConditionalReturn(condition=True, value=None))

        # Should skip node on false branch when condition is true
        assert runner._should_skip_node("on_false", context)

    def test_should_skip_node_missing_dependency(self):
        """Test _should_skip_node for missing dependency (lines 40-45)"""
        dag = DAG("missing_dep_test")

        @dag.node
        def source() -> int:
            return 1

        @dag.node
        def target(x: int) -> int:
            return x

        dag.connect("source", "target", input="x")

        runner = DAGRunner(dag)
        context = Context()

        # Target depends on source, but source hasn't executed
        assert runner._should_skip_node("target", context)

    def test_prepare_node_inputs_entry_node_missing_input(self):
        """Test _prepare_node_inputs for entry node with missing input (lines 69-72)"""
        dag = DAG("entry_missing_input")

        @dag.node
        def task(x: int) -> int:
            return x

        runner = DAGRunner(dag)
        context = Context()
        entry_nodes = ["task"]
        kwargs = {}  # Missing required input

        node = dag.nodes["task"]

        with pytest.raises(
            ValueError, match="Entry node 'task' missing required input: 'x'"
        ):
            runner._prepare_node_inputs("task", node, context, entry_nodes, kwargs)

    def test_prepare_node_inputs_any_node_multi_input_connections(self):
        """Test _prepare_node_inputs for ANY node with multi_input_connections (lines 89-94)"""
        dag = DAG("any_multi_input")

        @dag.any
        def any_task(data: int) -> int:
            return data

        runner = DAGRunner(dag)
        context = Context()
        context.set_result("source1", 10)
        context.set_result("source2", 20)

        node = dag.nodes["any_task"]

        # Mock multi_input_connections
        mock_source1 = MagicMock()
        mock_source1.name = "source1"
        mock_source2 = MagicMock()
        mock_source2.name = "source2"

        node.multi_input_connections = {
            "data": [(mock_source1, "result"), (mock_source2, "result")]
        }

        result = runner._prepare_node_inputs("any_task", node, context, [], {})

        # Should use first available input
        assert result["data"] == 10

    def test_prepare_node_inputs_any_node_source_name_none(self):
        """Test _prepare_node_inputs for ANY node with source_name None (lines 104-105)"""
        dag = DAG("any_source_none")

        @dag.any
        def any_task(data: int) -> int:
            return data

        runner = DAGRunner(dag)
        context = Context()
        context.set_result("source1", 10)

        node = dag.nodes["any_task"]

        # Mock connections with None source name
        mock_source1 = MagicMock()
        mock_source1.name = None
        mock_source2 = MagicMock()
        mock_source2.name = "source1"

        node.multi_input_connections = {
            "data": [(mock_source1, "result"), (mock_source2, "result")]
        }

        result = runner._prepare_node_inputs("any_task", node, context, [], {})

        # Should skip None source and use available one
        assert result["data"] == 10

    def test_prepare_node_inputs_any_node_dict_output(self):
        """Test _prepare_node_inputs for ANY node with dict output (lines 110-116)"""
        dag = DAG("any_dict_output")

        @dag.any
        def any_task(data: int) -> int:
            return data

        runner = DAGRunner(dag)
        context = Context()
        context.set_result("source", {"value": 42, "other": 100})

        node = dag.nodes["any_task"]

        # Mock connection with specific output
        mock_source = MagicMock()
        mock_source.name = "source"

        node.multi_input_connections = {"data": [(mock_source, "value")]}

        result = runner._prepare_node_inputs("any_task", node, context, [], {})

        # Should extract specific output from dict
        assert result["data"] == 42

    def test_prepare_node_inputs_any_node_conditional_return(self):
        """Test _prepare_node_inputs for ANY node with ConditionalReturn (lines 117-118)"""
        dag = DAG("any_conditional")

        @dag.any
        def any_task(data: int) -> int:
            return data

        runner = DAGRunner(dag)
        context = Context()
        context.set_result("source", ConditionalReturn(condition=True, value=42))

        node = dag.nodes["any_task"]

        # Mock connection
        mock_source = MagicMock()
        mock_source.name = "source"

        node.multi_input_connections = {"data": [(mock_source, "result")]}

        result = runner._prepare_node_inputs("any_task", node, context, [], {})

        # Should extract value from ConditionalReturn
        assert result["data"] == 42

    def test_prepare_node_inputs_any_node_multiple_parameters(self):
        """Test _prepare_node_inputs for ANY node with multiple parameters (lines 125-146)"""
        dag = DAG("any_multiple_params")

        @dag.any
        def any_task(data1: int, data2: int) -> int:
            return data1 + data2

        runner = DAGRunner(dag)
        context = Context()
        context.set_result("source1", 10)
        context.set_result("source2", 20)

        node = dag.nodes["any_task"]

        # Mock connections for multiple parameters
        mock_source1 = MagicMock()
        mock_source1.name = "source1"
        mock_source2 = MagicMock()
        mock_source2.name = "source2"

        node.input_connections = {
            "data1": (mock_source1, "result"),
            "data2": (mock_source2, "result"),
        }

        result = runner._prepare_node_inputs("any_task", node, context, [], {})

        # Should collect all available inputs
        assert result["data1"] == 10
        assert result["data2"] == 20

    def test_prepare_node_inputs_any_node_no_inputs_available(self):
        """Test _prepare_node_inputs for ANY node with no inputs available (lines 150-153)"""
        dag = DAG("any_no_inputs")

        @dag.any
        def any_task(data: int) -> int:
            return data or 0

        runner = DAGRunner(dag)
        context = Context()  # No sources available

        node = dag.nodes["any_task"]

        # Mock connection to unavailable source
        mock_source = MagicMock()
        mock_source.name = "missing_source"

        node.multi_input_connections = {"data": [(mock_source, "result")]}

        result = runner._prepare_node_inputs("any_task", node, context, [], {})

        # Should set input to None when no inputs available
        assert result["data"] is None

    def test_prepare_node_inputs_regular_node_source_name_none(self):
        """Test _prepare_node_inputs for regular node with source_name None (lines 160-162)"""
        dag = DAG("regular_source_none")

        @dag.node
        def task(x: int) -> int:
            return x

        runner = DAGRunner(dag)
        context = Context()

        node = dag.nodes["task"]

        # Mock connection with None source name
        mock_source = MagicMock()
        mock_source.name = None

        node.input_connections = {"x": (mock_source, "result")}

        with pytest.raises(ExecutionError, match="Source node has no name"):
            runner._prepare_node_inputs("task", node, context, [], {})

    def test_prepare_node_inputs_regular_node_missing_source(self):
        """Test _prepare_node_inputs for regular node with missing source (lines 163-166)"""
        dag = DAG("regular_missing_source")

        @dag.node
        def task(x: int) -> int:
            return x

        runner = DAGRunner(dag)
        context = Context()

        node = dag.nodes["task"]

        # Mock connection to missing source
        mock_source = MagicMock()
        mock_source.name = "missing_source"

        node.input_connections = {"x": (mock_source, "result")}

        with pytest.raises(
            ExecutionError,
            match="Node 'task' requires result from 'missing_source' which hasn't executed",
        ):
            runner._prepare_node_inputs("task", node, context, [], {})

    def test_prepare_node_inputs_regular_node_dict_output(self):
        """Test _prepare_node_inputs for regular node with dict output (lines 171-172)"""
        dag = DAG("regular_dict_output")

        @dag.node
        def task(x: int) -> int:
            return x

        runner = DAGRunner(dag)
        context = Context()
        context.set_result("source", {"value": 42, "other": 100})

        node = dag.nodes["task"]

        # Mock connection with specific output
        mock_source = MagicMock()
        mock_source.name = "source"

        node.input_connections = {"x": (mock_source, "value")}

        result = runner._prepare_node_inputs("task", node, context, [], {})

        # Should extract specific output from dict
        assert result["x"] == 42

    def test_prepare_node_inputs_regular_node_conditional_return(self):
        """Test _prepare_node_inputs for regular node with ConditionalReturn (lines 173-174)"""
        dag = DAG("regular_conditional")

        @dag.node
        def task(x: int) -> int:
            return x

        runner = DAGRunner(dag)
        context = Context()
        context.set_result("source", ConditionalReturn(condition=True, value=42))

        node = dag.nodes["task"]

        # Mock connection
        mock_source = MagicMock()
        mock_source.name = "source"

        node.input_connections = {"x": (mock_source, "result")}

        result = runner._prepare_node_inputs("task", node, context, [], {})

        # Should extract value from ConditionalReturn
        assert result["x"] == 42

    def test_prepare_node_inputs_kwargs_fill_missing(self):
        """Test _prepare_node_inputs with kwargs filling missing inputs (lines 180-183)"""
        dag = DAG("kwargs_fill")

        @dag.node
        def task(x: int, y: int) -> int:
            return x + y

        runner = DAGRunner(dag)
        context = Context()
        context.set_result("source", 10)

        node = dag.nodes["task"]

        # Mock connection for only one input
        mock_source = MagicMock()
        mock_source.name = "source"

        node.input_connections = {"x": (mock_source, "result")}

        # Kwargs should fill in the missing input 'y'
        result = runner._prepare_node_inputs("task", node, context, [], {"y": 99})

        # Should use connected input for x and kwargs for y
        assert result["x"] == 10
        assert result["y"] == 99

    def test_compute_dependency_levels_circular_dependency(self):
        """Test _compute_dependency_levels with circular dependency (lines 210-214)"""
        dag = DAG("circular_deps")

        @dag.node
        def task1(x: int) -> int:
            return x

        @dag.node
        def task2(y: int) -> int:
            return y

        runner = DAGRunner(dag)

        # Create circular dependency manually
        mock_source1 = MagicMock()
        mock_source1.name = "task2"
        mock_source2 = MagicMock()
        mock_source2.name = "task1"

        dag.nodes["task1"].input_connections = {"x": (mock_source1, "result")}
        dag.nodes["task2"].input_connections = {"y": (mock_source2, "result")}

        with pytest.raises(ExecutionError, match="Cannot determine execution order"):
            runner._compute_dependency_levels()

    def test_compute_dependency_levels_valid_ordering(self):
        """Test _compute_dependency_levels with valid ordering"""
        dag = DAG("valid_ordering")

        @dag.node
        def task1() -> int:
            return 1

        @dag.node
        def task2(x: int) -> int:
            return x

        @dag.node
        def task3(y: int) -> int:
            return y

        dag.connect("task1", "task2", input="x")
        dag.connect("task2", "task3", input="y")

        runner = DAGRunner(dag)

        levels = runner._compute_dependency_levels()

        # Should have three levels: task1, task2, task3
        assert len(levels) == 3
        assert levels[0] == ["task1"]
        assert levels[1] == ["task2"]
        assert levels[2] == ["task3"]

    def test_get_node_timeout_node_specific(self):
        """Test _get_node_timeout with node-specific timeout (lines 225-226)"""
        dag = DAG("node_timeout")

        @dag.node
        def task() -> int:
            return 1

        runner = DAGRunner(dag, timeout=10.0)

        # Mock node with specific timeout
        node = dag.nodes["task"]
        node.timeout = 5.0

        timeout = runner._get_node_timeout(node)

        # Should use node-specific timeout
        assert timeout == 5.0

    def test_get_node_timeout_global(self):
        """Test _get_node_timeout with global timeout (lines 227-228)"""
        dag = DAG("global_timeout")

        @dag.node
        def task() -> int:
            return 1

        runner = DAGRunner(dag, timeout=10.0)

        node = dag.nodes["task"]

        timeout = runner._get_node_timeout(node)

        # Should use global timeout
        assert timeout == 10.0

    def test_get_node_timeout_none(self):
        """Test _get_node_timeout with no timeout"""
        dag = DAG("no_timeout")

        @dag.node
        def task() -> int:
            return 1

        runner = DAGRunner(dag)

        node = dag.nodes["task"]

        timeout = runner._get_node_timeout(node)

        # Should return None
        assert timeout is None

    def test_should_skip_node_source_name_none(self):
        """Test _should_skip_node with source_name None"""
        dag = DAG("source_name_none")

        @dag.node
        def task() -> int:
            return 1

        runner = DAGRunner(dag)
        context = Context()

        node = dag.nodes["task"]

        # Mock connection with None source name
        mock_source = MagicMock()
        mock_source.name = None

        node.input_connections = {"x": (mock_source, "result")}

        # Should not skip when source name is None
        assert not runner._should_skip_node("task", context)

    def test_should_skip_node_no_conditional_return(self):
        """Test _should_skip_node with regular result (not ConditionalReturn)"""
        dag = DAG("no_conditional")

        @dag.node
        def source() -> int:
            return 42

        @dag.node
        def task(x: int) -> int:
            return x

        dag.connect("source", "task", input="x")

        runner = DAGRunner(dag)
        context = Context()
        context.set_result("source", 42)  # Regular result, not ConditionalReturn

        # Should not skip when result is not ConditionalReturn
        assert not runner._should_skip_node("task", context)
