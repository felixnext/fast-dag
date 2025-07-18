"""
Extra tests for runner utils to improve coverage.
"""

from unittest.mock import MagicMock

from fast_dag import DAG, Context
from fast_dag.runner import DAGRunner


class TestRunnerUtilsExtraCoverage:
    """Test runner utils edge cases"""

    def test_should_skip_node_with_context_param(self):
        """Test _should_skip_node when node has context parameter (line 66)"""
        dag = DAG("test")

        @dag.node
        def task(x: int, context: Context) -> int:
            return x + 1

        runner = DAGRunner(dag)
        context = Context()

        # Should not skip just because it has context parameter
        assert not runner._should_skip_node("task", context)

    def test_prepare_node_inputs_source_none_for_any_node(self):
        """Test _prepare_node_inputs when source is None for ANY node (line 131)"""
        dag = DAG("test")

        @dag.any
        def any_task(data: int) -> int:
            return data or 0

        runner = DAGRunner(dag)
        context = Context()

        node = dag.nodes["any_task"]

        # Mock connection with None source
        mock_source = MagicMock()
        mock_source.name = None

        node.input_connections = {"data": (mock_source, "result")}

        # Should handle None source gracefully for ANY nodes
        result = runner._prepare_node_inputs("any_task", node, context, [], {})
        assert result == {"data": None}

    def test_prepare_node_inputs_regular_node_dict_missing_key(self):
        """Test _prepare_node_inputs when dict output missing key (line 140)"""
        dag = DAG("test")

        @dag.node
        def source() -> dict:
            return {"value": 42}

        @dag.node
        def target(x: int) -> int:
            return x

        dag.connect("source", "target", output="missing_key", input="x")

        runner = DAGRunner(dag)
        context = Context()
        context.set_result("source", {"value": 42})

        node = dag.nodes["target"]

        # Should get the dict itself when key is missing
        result = runner._prepare_node_inputs("target", node, context, [], {})
        assert result["x"] == {"value": 42}

    def test_prepare_node_inputs_any_node_conditional_value(self):
        """Test _prepare_node_inputs for ANY node extracting conditional value (line 142)"""
        dag = DAG("test")

        @dag.any
        def any_task(data: int) -> int:
            return data

        runner = DAGRunner(dag)
        context = Context()

        from fast_dag.core.types import ConditionalReturn

        context.set_result("source", ConditionalReturn(condition=False, value=55))

        node = dag.nodes["any_task"]

        # Mock connection
        mock_source = MagicMock()
        mock_source.name = "source"

        node.input_connections = {"data": (mock_source, "result")}

        result = runner._prepare_node_inputs("any_task", node, context, [], {})
        assert result["data"] == 55
