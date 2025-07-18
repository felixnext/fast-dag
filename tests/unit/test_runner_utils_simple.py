"""
Simple tests for runner utils to improve coverage without complex connections.
"""

from unittest.mock import MagicMock

import pytest

from fast_dag import DAG, Context
from fast_dag.core.exceptions import ExecutionError
from fast_dag.runner import DAGRunner


class TestRunnerUtilsSimple:
    """Test runner utils with simple scenarios"""

    def test_should_skip_node_any_node_type(self):
        """Test _should_skip_node for ANY node type"""
        dag = DAG("any_node_test")

        @dag.any
        def any_task() -> int:
            return 1

        runner = DAGRunner(dag)
        context = Context()

        # ANY nodes should never be skipped
        assert not runner._should_skip_node("any_task", context)

    def test_should_skip_node_missing_dependency(self):
        """Test _should_skip_node for missing dependency"""
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
        """Test _prepare_node_inputs for entry node with missing input"""
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

    def test_prepare_node_inputs_entry_node_context_input(self):
        """Test _prepare_node_inputs for entry node with context input"""
        dag = DAG("entry_context_input")

        @dag.node
        def task(x: int, context: Context) -> int:
            return x

        runner = DAGRunner(dag)
        context = Context()
        entry_nodes = ["task"]
        kwargs = {"x": 42}  # Context should be skipped

        node = dag.nodes["task"]

        result = runner._prepare_node_inputs("task", node, context, entry_nodes, kwargs)

        # Should only have x, context is handled separately
        assert result == {"x": 42}

    def test_prepare_node_inputs_entry_node_no_inputs(self):
        """Test _prepare_node_inputs for entry node with no inputs"""
        dag = DAG("entry_no_inputs")

        @dag.node
        def task() -> int:
            return 42

        runner = DAGRunner(dag)
        context = Context()
        entry_nodes = ["task"]
        kwargs = {}

        node = dag.nodes["task"]

        # Should not raise error for no-argument function
        result = runner._prepare_node_inputs("task", node, context, entry_nodes, kwargs)
        assert result == {}

    def test_compute_dependency_levels_circular_dependency(self):
        """Test _compute_dependency_levels with circular dependency"""
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
        """Test _get_node_timeout with node-specific timeout"""
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
        """Test _get_node_timeout with global timeout"""
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

    def test_should_skip_node_no_connections(self):
        """Test _should_skip_node with no connections"""
        dag = DAG("no_connections")

        @dag.node
        def task() -> int:
            return 1

        runner = DAGRunner(dag)
        context = Context()

        # Should not skip when no connections
        assert not runner._should_skip_node("task", context)

    def test_prepare_node_inputs_any_node_no_inputs_available(self):
        """Test _prepare_node_inputs for ANY node with no inputs available"""
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

        node.input_connections = {"data": (mock_source, "result")}

        result = runner._prepare_node_inputs("any_task", node, context, [], {})

        # Should set input to None when no inputs available
        assert result["data"] is None

    def test_prepare_node_inputs_regular_node_source_name_none(self):
        """Test _prepare_node_inputs for regular node with source_name None"""
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
        """Test _prepare_node_inputs for regular node with missing source"""
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
