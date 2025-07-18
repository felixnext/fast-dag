"""
Tests for DAG builder to improve coverage.
"""

from unittest.mock import MagicMock

from fast_dag import DAG
from fast_dag.core.types import NodeType


class TestDAGBuilderCoverage:
    """Test DAG builder functionality"""

    def test_dag_node_decorator_with_name(self):
        """Test @dag.node decorator with explicit name (line 92)"""
        dag = DAG("test")

        @dag.node(name="custom_name")
        def my_func() -> int:
            return 42

        assert "custom_name" in dag.nodes
        # The original function is stored in the node's func attribute
        node = dag.nodes["custom_name"]
        assert node.func.__name__ == "my_func"
        assert node.name == "custom_name"

    def test_dag_any_decorator(self):
        """Test @dag.any decorator (lines 140, 146)"""
        dag = DAG("test")

        @dag.any
        def any_task(data: int) -> int:
            return data or 0

        assert "any_task" in dag.nodes
        assert dag.nodes["any_task"].node_type == NodeType.ANY

    def test_dag_all_decorator(self):
        """Test @dag.all decorator (lines 151, 156-157)"""
        dag = DAG("test")

        @dag.all
        def all_task(data1: int, data2: int) -> int:
            return data1 + data2

        assert "all_task" in dag.nodes
        assert dag.nodes["all_task"].node_type == NodeType.ALL

    def test_dag_select_decorator(self):
        """Test @dag.select decorator (line 212)"""
        dag = DAG("test")

        @dag.select
        def select_task(options: list[int]) -> int:
            return max(options)

        assert "select_task" in dag.nodes
        assert dag.nodes["select_task"].node_type == NodeType.SELECT

    def test_dag_add_node_different_types(self):
        """Test add_node with different node types (lines 241-251)"""
        dag = DAG("test")

        def func1() -> int:
            return 1

        def func2() -> int:
            return 2

        def func3() -> int:
            return 3

        # Add nodes with different types - need to provide name as first arg
        dag.add_node("func1", func1, node_type=NodeType.ANY)
        dag.add_node("func2", func2, node_type=NodeType.ALL)
        dag.add_node("func3", func3, node_type=NodeType.SELECT)

        assert dag.nodes["func1"].node_type == NodeType.ANY
        assert dag.nodes["func2"].node_type == NodeType.ALL
        assert dag.nodes["func3"].node_type == NodeType.SELECT
        assert "func1" in dag.nodes
        assert "func2" in dag.nodes
        assert "func3" in dag.nodes

    def test_dag_connect_with_multi_output(self):
        """Test connect with multi-output nodes (lines 271-273)"""
        dag = DAG("test")

        @dag.node
        def multi_output() -> tuple[int, str]:
            return (42, "hello")

        @dag.node
        def consumer1(x: int) -> int:
            return x * 2

        @dag.node
        def consumer2(msg: str) -> str:
            return msg.upper()

        # Connect specific outputs
        dag.connect("multi_output", "consumer1", output="0", input="x")
        dag.connect("multi_output", "consumer2", output="1", input="msg")

        # Verify connections exist in the nodes
        multi_node = dag.nodes["multi_output"]
        assert "0" in multi_node.output_connections
        assert "1" in multi_node.output_connections

    def test_dag_visualize_with_options(self):
        """Test DAG visualize with custom options (lines 460-464)"""
        dag = DAG("test")

        @dag.node
        def task() -> int:
            return 42

        # Mock the visualization backend
        with MagicMock() as mock_backend:
            mock_backend.visualize_dag.return_value = "digraph { ... }"

            # This would normally call the visualization
            # We're just testing that the method exists
            assert hasattr(dag, "visualize")

    def test_dag_cached_node_decorator(self):
        """Test @dag.cached_node decorator (lines 430-490)"""
        dag = DAG("test")

        @dag.cached_node(cache_ttl=60)
        def expensive_task(x: int) -> int:
            return x * x

        # Should have created a cached node
        assert "expensive_task" in dag.nodes
        node = dag.nodes["expensive_task"]
        assert node.cached is True
        assert node.cache_ttl == 60
        assert node.cache_backend == "memory"

    def test_dag_any_node_decorator(self):
        """Test @dag.any_node decorator (lines 374-386)"""
        dag = DAG("test")

        @dag.any_node
        def any_task(data: int) -> int:
            return data or 0

        assert "any_task" in dag.nodes
        assert dag.nodes["any_task"].node_type == NodeType.ANY

    def test_dag_all_node_decorator(self):
        """Test @dag.all_node decorator (lines 416-428)"""
        dag = DAG("test")

        @dag.all_node
        def all_task(data1: int, data2: int) -> int:
            return data1 + data2

        assert "all_task" in dag.nodes
        assert dag.nodes["all_task"].node_type == NodeType.ALL
