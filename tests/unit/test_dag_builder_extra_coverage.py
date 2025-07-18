"""
Extra tests for DAG builder to achieve 100% coverage.
"""

from unittest.mock import MagicMock, patch

import pytest

from fast_dag import DAG
from fast_dag.core.exceptions import InvalidNodeError, MissingConnectionError
from fast_dag.core.types import NodeType


class TestDAGBuilderExtraCoverage:
    """Test DAG builder edge cases for 100% coverage"""

    def test_connect_with_none_source_name(self):
        """Test connect when source node has no name (line 92)"""
        dag = DAG("test")

        # Create a mock node object with name=None
        node1 = MagicMock()
        node1.name = None

        @dag.node
        def target() -> int:
            return 0

        with pytest.raises(
            InvalidNodeError, match="Source and target nodes must have names"
        ):
            dag.connect(node1, "target")

    def test_connect_with_none_target_name(self):
        """Test connect when target node has no name (line 92)"""
        dag = DAG("test")

        @dag.node
        def source() -> int:
            return 42

        # Create a mock node object with name=None
        node2 = MagicMock()
        node2.name = None

        with pytest.raises(
            InvalidNodeError, match="Source and target nodes must have names"
        ):
            dag.connect("source", node2)

    def test_auto_detect_input_error_no_params(self):
        """Test _auto_detect_input when target has no parameters (line 140)"""
        dag = DAG("test")

        @dag.node
        def source() -> int:
            return 42

        @dag.node
        def target() -> int:  # No input params
            return 0

        with pytest.raises(
            MissingConnectionError, match="Target node 'target' has no input parameters"
        ):
            dag.connect("source", "target")

    def test_auto_detect_input_matching_output_name(self):
        """Test _auto_detect_input matching by output name (line 146)"""
        dag = DAG("test")

        @dag.node
        def source() -> int:
            return 42

        @dag.node
        def target(result: int) -> int:  # param name matches output name
            return result * 2

        # Should auto-detect 'result' as input
        dag.connect("source", "target")
        assert dag.nodes["target"].input_connections["result"][0].name == "source"

    def test_auto_detect_input_partial_match(self):
        """Test _auto_detect_input with partial parameter name match (line 151)"""
        dag = DAG("test")

        @dag.node
        def source() -> int:
            return 42

        @dag.node
        def target(my_result_value: int) -> int:  # 'result' is in param name
            return my_result_value * 2

        # Should match 'my_result_value' because it contains 'result'
        dag.connect("source", "target", output="result")
        assert (
            dag.nodes["target"].input_connections["my_result_value"][0].name == "source"
        )

    def test_auto_detect_input_exception_handling(self):
        """Test _auto_detect_input exception handling (line 156-157)"""
        dag = DAG("test")

        @dag.node
        def source() -> int:
            return 42

        # Mock a target node that raises exception when getting signature
        target_node = MagicMock()
        target_node.name = "target"
        target_node.func = MagicMock()

        # Make inspect.signature raise an exception
        with (
            patch("inspect.signature", side_effect=Exception("signature error")),
            pytest.raises(
                MissingConnectionError, match="Could not auto-detect input parameter"
            ),
        ):
            dag._auto_detect_input(target_node, dag.nodes["source"], "result")

    def test_disconnect_with_none_source_name(self):
        """Test disconnect when source node has no name (lines 172-176)"""
        dag = DAG("test")

        # Create a mock node object with name=None
        node1 = MagicMock()
        node1.name = None

        @dag.node
        def target() -> int:
            return 0

        with pytest.raises(
            InvalidNodeError, match="Source and target nodes must have names"
        ):
            dag.disconnect(node1, "target")

    def test_disconnect_with_none_target_name(self):
        """Test disconnect when target node has no name (lines 172-176)"""
        dag = DAG("test")

        @dag.node
        def source() -> int:
            return 42

        # Create a mock node object with name=None
        node2 = MagicMock()
        node2.name = None

        with pytest.raises(
            InvalidNodeError, match="Source and target nodes must have names"
        ):
            dag.disconnect("source", node2)

    def test_disconnect_existing_connection(self):
        """Test disconnect removes existing connections (lines 183-206)"""
        dag = DAG("test")

        @dag.node
        def source() -> int:
            return 42

        @dag.node
        def target(x: int) -> int:
            return x * 2

        # First connect them
        dag.connect("source", "target", input="x")

        # Verify connection exists
        assert "result" in dag.nodes["source"].output_connections
        assert "x" in dag.nodes["target"].input_connections

        # Now disconnect
        dag.disconnect("source", "target")

        # Verify connection is removed
        assert "result" not in dag.nodes["source"].output_connections
        assert "x" not in dag.nodes["target"].input_connections

    def test_disconnect_specific_output(self):
        """Test disconnect with specific output (lines 191-199)"""
        dag = DAG("test")

        @dag.node
        def multi_source() -> tuple[int, str]:
            return (42, "hello")

        @dag.node
        def target1(x: int) -> int:
            return x * 2

        @dag.node
        def target2(msg: str) -> str:
            return msg.upper()

        # Connect both outputs
        dag.connect("multi_source", "target1", output="0", input="x")
        dag.connect("multi_source", "target2", output="1", input="msg")

        # Disconnect only output "0"
        dag.disconnect("multi_source", "target1", output="0")

        # Verify only output "0" is disconnected
        assert "0" not in dag.nodes["multi_source"].output_connections
        assert "1" in dag.nodes["multi_source"].output_connections
        assert "x" not in dag.nodes["target1"].input_connections
        assert "msg" in dag.nodes["target2"].input_connections

    def test_rshift_operator(self):
        """Test __rshift__ operator (line 212)"""
        dag1 = DAG("dag1")
        dag2 = DAG("dag2")

        # Test >> operator
        result = dag1 >> dag2
        assert result is dag1  # Should return self

    def test_node_decorator_update_existing_cached_node(self):
        """Test @dag.node updating existing cached node (lines 241-251)"""
        dag = DAG("test")

        # Define function first
        def compute(x: int) -> int:
            return x * x

        # First add as cached node
        dag.cached_node(cache_ttl=60)(compute)

        # Then update with @dag.node - use the same function object
        dag.node(description="Updated description", retry=3)(compute)

        node = dag.nodes["compute"]
        assert node.cached is True
        assert node.cache_ttl == 60
        assert node.description == "Updated description"
        assert node.retry == 3

    def test_node_decorator_inputs_update(self):
        """Test @dag.node updating inputs (line 241)"""
        dag = DAG("test")

        def compute(x: int, y: int) -> int:
            return x + y

        # First add without specifying inputs
        dag.add_node("compute", compute)

        # Then update with specific inputs - use same function object
        dag.node(name="compute", inputs=["a", "b"])(compute)

        assert dag.nodes["compute"].inputs == ["a", "b"]

    def test_node_decorator_outputs_update(self):
        """Test @dag.node updating outputs (line 243)"""
        dag = DAG("test")

        def compute() -> tuple[int, str]:
            return (42, "hello")

        # First add without specifying outputs
        dag.add_node("compute", compute)

        # Then update with specific outputs - use same function object
        dag.node(name="compute", outputs=["num", "msg"])(compute)

        assert dag.nodes["compute"].outputs == ["num", "msg"]

    def test_node_decorator_timeout_update(self):
        """Test @dag.node updating timeout (line 251)"""
        dag = DAG("test")

        def slow_task() -> int:
            return 42

        # First add without timeout
        dag.add_node("slow_task", slow_task)

        # Then update with timeout - use same function object
        dag.node(name="slow_task", timeout=5.0)(slow_task)

        assert dag.nodes["slow_task"].timeout == 5.0

    def test_cached_node_with_caching_attributes(self):
        """Test @dag.node with function having caching attributes (lines 271-273)"""
        dag = DAG("test")

        def my_func() -> int:
            return 42

        # Simulate caching attributes
        my_func._cached = True
        my_func._cache_backend = "disk"
        my_func._cache_ttl = 120

        # Add as regular node with the decorated function
        dag.node()(my_func)

        node = dag.nodes["my_func"]
        assert node.cached is True
        assert node.cache_backend == "disk"
        assert node.cache_ttl == 120

    def test_select_decorator_without_args(self):
        """Test @dag.select without parentheses (line 342)"""
        dag = DAG("test")

        @dag.select
        def route(data: dict) -> str:
            return data.get("route", "default")

        assert "route" in dag.nodes
        assert dag.nodes["route"].node_type == NodeType.SELECT

    def test_any_node_alias_decorator(self):
        """Test @dag.any_node alias (line 384)"""
        dag = DAG("test")

        @dag.any_node(name="custom_any")
        def process(data: int) -> int:
            return data * 2

        assert "custom_any" in dag.nodes
        assert dag.nodes["custom_any"].node_type == NodeType.ANY

    def test_all_node_alias_decorator(self):
        """Test @dag.all_node alias (line 426)"""
        dag = DAG("test")

        @dag.all_node(name="custom_all")
        def combine(a: int, b: int) -> int:
            return a + b

        assert "custom_all" in dag.nodes
        assert dag.nodes["custom_all"].node_type == NodeType.ALL

    def test_cached_node_update_existing_in_dag(self):
        """Test @dag.cached_node updating existing node (lines 460-464)"""
        dag = DAG("test")

        # Define function first
        def process(x: int) -> int:
            return x * 2

        # First add regular node
        dag.node()(process)

        # Then make it cached - use same function object
        dag.cached_node(cache_ttl=300, backend="disk")(process)

        node = dag.nodes["process"]
        assert node.cached is True
        assert node.cache_ttl == 300
        assert node.cache_backend == "disk"

    def test_node_decorator_retry_delay_update(self):
        """Test @dag.node updating retry_delay (line 249)"""
        dag = DAG("test")

        def unreliable_task() -> int:
            return 42

        # First add with retry but no retry_delay
        dag.add_node("unreliable_task", unreliable_task, retry=3)

        # Then update with retry_delay - use same function object
        dag.node(name="unreliable_task", retry_delay=1.5)(unreliable_task)

        assert dag.nodes["unreliable_task"].retry == 3
        assert dag.nodes["unreliable_task"].retry_delay == 1.5

    def test_select_decorator_with_args(self):
        """Test @dag.select with parentheses (line 342)"""
        dag = DAG("test")

        @dag.select(name="router", outputs=["path1", "path2"])
        def route_data(data: dict) -> str:
            return data.get("route", "path1")

        assert "router" in dag.nodes
        assert dag.nodes["router"].node_type == NodeType.SELECT
        assert dag.nodes["router"].outputs == ["path1", "path2"]
