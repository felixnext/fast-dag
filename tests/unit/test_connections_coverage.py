"""
Tests for core.connections to improve coverage.
"""

from unittest.mock import MagicMock

import pytest

from fast_dag.core.connections import (
    ConditionalOutputProxy,
    InputCollection,
    InputProxy,
    NodeList,
    OutputCollection,
    OutputProxy,
)
from fast_dag.core.types import NodeType


class TestConnectionsCoverage:
    """Test connections comprehensively"""

    def test_conditional_output_proxy_init(self):
        """Test ConditionalOutputProxy initialization (lines 14-15)"""
        mock_node = MagicMock()
        mock_node.name = "test_node"

        proxy = ConditionalOutputProxy(mock_node, "on_true")

        assert proxy.node is mock_node
        assert proxy.output_name == "on_true"

    def test_conditional_output_proxy_rshift_list(self):
        """Test ConditionalOutputProxy >> operator with list (lines 19-22)"""
        mock_node = MagicMock()
        mock_node.name = "source"

        target1 = MagicMock()
        target1.name = "target1"
        target2 = MagicMock()
        target2.name = "target2"

        proxy = ConditionalOutputProxy(mock_node, "on_true")
        result = proxy >> [target1, target2]

        # Should connect to both targets
        assert mock_node.connect_to.call_count == 2
        mock_node.connect_to.assert_any_call(target1, output="on_true")
        mock_node.connect_to.assert_any_call(target2, output="on_true")
        assert result == [target1, target2]

    def test_conditional_output_proxy_rshift_single(self):
        """Test ConditionalOutputProxy >> operator with single node (lines 23-24)"""
        mock_node = MagicMock()
        mock_node.name = "source"
        mock_node.connect_to.return_value = "connected"

        target = MagicMock()
        target.name = "target"

        proxy = ConditionalOutputProxy(mock_node, "on_false")
        result = proxy >> target

        mock_node.connect_to.assert_called_once_with(target, output="on_false")
        assert result == "connected"

    def test_conditional_output_proxy_or(self):
        """Test ConditionalOutputProxy | operator (line 28)"""
        mock_node = MagicMock()
        mock_node.name = "source"
        mock_node.connect_to.return_value = "connected"

        target = MagicMock()
        target.name = "target"

        proxy = ConditionalOutputProxy(mock_node, "on_true")
        result = proxy | target

        mock_node.connect_to.assert_called_once_with(target, output="on_true")
        assert result == "connected"

    def test_conditional_output_proxy_connect_to(self):
        """Test ConditionalOutputProxy connect_to (line 32)"""
        mock_node = MagicMock()
        mock_node.name = "source"
        mock_node.connect_to.return_value = "connected"

        target = MagicMock()
        target.name = "target"

        proxy = ConditionalOutputProxy(mock_node, "on_false")
        result = proxy.connect_to(target, input="data")

        mock_node.connect_to.assert_called_once_with(
            target, output="on_false", input="data"
        )
        assert result == "connected"

    def test_output_proxy_init(self):
        """Test OutputProxy initialization (lines 39-40)"""
        mock_node = MagicMock()
        mock_node.name = "test_node"

        proxy = OutputProxy(mock_node, "result")

        assert proxy.node is mock_node
        assert proxy.output_name == "result"

    def test_output_proxy_connect_to_input_proxy(self):
        """Test OutputProxy connect_to InputProxy (lines 46-50)"""
        mock_node = MagicMock()
        mock_node.name = "source"
        mock_node.connect_to.return_value = "connected"

        target_node = MagicMock()
        target_node.name = "target"

        input_proxy = InputProxy(target_node, "data")
        output_proxy = OutputProxy(mock_node, "result")

        result = output_proxy.connect_to(input_proxy)

        mock_node.connect_to.assert_called_once_with(
            target_node, output="result", input="data"
        )
        assert result == "connected"

    def test_output_proxy_connect_to_node(self):
        """Test OutputProxy connect_to Node (lines 52-53)"""
        mock_node = MagicMock()
        mock_node.name = "source"
        mock_node.connect_to.return_value = "connected"

        target = MagicMock()
        target.name = "target"

        output_proxy = OutputProxy(mock_node, "result")
        result = output_proxy.connect_to(target, input="data")

        mock_node.connect_to.assert_called_once_with(
            target, output="result", input="data"
        )
        assert result == "connected"

    def test_output_proxy_rshift(self):
        """Test OutputProxy >> operator (line 57)"""
        mock_node = MagicMock()
        mock_node.name = "source"
        mock_node.connect_to.return_value = "connected"

        target = MagicMock()
        target.name = "target"

        output_proxy = OutputProxy(mock_node, "result")
        result = output_proxy >> target

        mock_node.connect_to.assert_called_once_with(
            target, output="result", input=None
        )
        assert result == "connected"

    def test_output_proxy_or(self):
        """Test OutputProxy | operator (line 61)"""
        mock_node = MagicMock()
        mock_node.name = "source"
        mock_node.connect_to.return_value = "connected"

        target = MagicMock()
        target.name = "target"

        output_proxy = OutputProxy(mock_node, "result")
        result = output_proxy | target

        mock_node.connect_to.assert_called_once_with(
            target, output="result", input=None
        )
        assert result == "connected"

    def test_input_proxy_init(self):
        """Test InputProxy initialization (lines 68-69)"""
        mock_node = MagicMock()
        mock_node.name = "test_node"

        proxy = InputProxy(mock_node, "data")

        assert proxy.node is mock_node
        assert proxy.input_name == "data"

    def test_output_collection_init(self):
        """Test OutputCollection initialization (lines 75-76)"""
        mock_node = MagicMock()
        mock_node.name = "test_node"

        collection = OutputCollection(mock_node)

        assert collection.node is mock_node

    def test_output_collection_getitem_valid(self):
        """Test OutputCollection __getitem__ with valid output (line 84)"""
        mock_node = MagicMock()
        mock_node.name = "test_node"
        mock_node.outputs = ["result", "error"]

        collection = OutputCollection(mock_node)
        proxy = collection["result"]

        assert isinstance(proxy, OutputProxy)
        assert proxy.node is mock_node
        assert proxy.output_name == "result"

    def test_output_collection_getitem_no_outputs(self):
        """Test OutputCollection __getitem__ when node has no outputs (line 80)"""
        mock_node = MagicMock()
        mock_node.name = "test_node"
        mock_node.outputs = None

        collection = OutputCollection(mock_node)
        proxy = collection["anything"]  # Should work when outputs is None

        assert isinstance(proxy, OutputProxy)
        assert proxy.output_name == "anything"

    def test_output_collection_getitem_invalid(self):
        """Test OutputCollection __getitem__ with invalid output (lines 81-83)"""
        mock_node = MagicMock()
        mock_node.name = "test_node"
        mock_node.outputs = ["result", "error"]

        collection = OutputCollection(mock_node)

        with pytest.raises(
            KeyError, match="Output 'missing' not found in node 'test_node'"
        ):
            _ = collection["missing"]

    def test_input_collection_init(self):
        """Test InputCollection initialization (lines 90-91)"""
        mock_node = MagicMock()
        mock_node.name = "test_node"

        collection = InputCollection(mock_node)

        assert collection.node is mock_node

    def test_input_collection_getitem_valid(self):
        """Test InputCollection __getitem__ with valid input (line 99)"""
        mock_node = MagicMock()
        mock_node.name = "test_node"
        mock_node.inputs = ["data", "config"]

        collection = InputCollection(mock_node)
        proxy = collection["data"]

        assert isinstance(proxy, InputProxy)
        assert proxy.node is mock_node
        assert proxy.input_name == "data"

    def test_input_collection_getitem_no_inputs(self):
        """Test InputCollection __getitem__ when node has no inputs (line 95)"""
        mock_node = MagicMock()
        mock_node.name = "test_node"
        mock_node.inputs = None

        collection = InputCollection(mock_node)
        proxy = collection["anything"]  # Should work when inputs is None

        assert isinstance(proxy, InputProxy)
        assert proxy.input_name == "anything"

    def test_input_collection_getitem_invalid(self):
        """Test InputCollection __getitem__ with invalid input (lines 96-98)"""
        mock_node = MagicMock()
        mock_node.name = "test_node"
        mock_node.inputs = ["data", "config"]

        collection = InputCollection(mock_node)

        with pytest.raises(
            KeyError, match="Input 'missing' not found in node 'test_node'"
        ):
            _ = collection["missing"]

    def test_node_list_rshift_multiple_to_any_node(self):
        """Test NodeList >> with multiple nodes to ANY node (lines 110-137)"""
        source1 = MagicMock()
        source1.name = "source1"
        source2 = MagicMock()
        source2.name = "source2"

        target = MagicMock()
        target.name = "target"
        target.node_type = NodeType.ANY
        target.inputs = ["data"]

        node_list = NodeList([source1, source2])
        result = node_list >> target

        # ANY nodes allow multiple connections
        source1.connect_to.assert_called_once_with(target, input="data")
        source2.connect_to.assert_called_once_with(target)
        assert result is target

    def test_node_list_rshift_too_many_sources(self):
        """Test NodeList >> with too many sources for regular node (lines 119-124)"""
        source1 = MagicMock()
        source1.name = "source1"
        source2 = MagicMock()
        source2.name = "source2"
        source3 = MagicMock()
        source3.name = "source3"

        target = MagicMock()
        target.name = "target"
        target.node_type = NodeType.STANDARD
        target.inputs = ["data1", "data2"]

        node_list = NodeList([source1, source2, source3])

        with pytest.raises(
            ValueError, match="Cannot connect 3 source nodes to node 'target'"
        ):
            _ = node_list >> target

    def test_node_list_rshift_matching_inputs(self):
        """Test NodeList >> with matching number of inputs (lines 130-135)"""
        source1 = MagicMock()
        source1.name = "source1"
        source2 = MagicMock()
        source2.name = "source2"

        target = MagicMock()
        target.name = "target"
        target.node_type = NodeType.STANDARD
        target.inputs = ["data1", "data2"]

        node_list = NodeList([source1, source2])
        result = node_list >> target

        # Each source connects to corresponding input
        source1.connect_to.assert_called_once_with(target, input="data1")
        source2.connect_to.assert_called_once_with(target, input="data2")
        assert result is target

    def test_node_list_rshift_no_target_inputs(self):
        """Test NodeList >> when target has no inputs (line 127)"""
        source1 = MagicMock()
        source1.name = "source1"

        target = MagicMock()
        target.name = "target"
        target.node_type = NodeType.STANDARD
        target.inputs = None

        node_list = NodeList([source1])
        result = node_list >> target

        # Should connect with default
        source1.connect_to.assert_called_once_with(target)
        assert result is target

    def test_node_list_or(self):
        """Test NodeList | operator (line 141)"""
        source1 = MagicMock()
        source1.name = "source1"

        target = MagicMock()
        target.name = "target"
        target.node_type = NodeType.STANDARD
        target.inputs = ["data"]

        node_list = NodeList([source1])
        result = node_list | target

        source1.connect_to.assert_called_once_with(target, input="data")
        assert result is target
