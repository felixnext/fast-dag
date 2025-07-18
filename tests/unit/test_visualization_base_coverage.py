"""
Tests for visualization base to achieve 100% coverage.
"""

from typing import Any

from fast_dag.visualization.base import VisualizationBackend, VisualizationOptions


class MockNode:
    """Mock node for testing"""

    def __init__(self, name: str, inputs=None, outputs=None, description=None):
        self.name = name
        self.inputs = inputs or []
        self.outputs = outputs or []
        self.description = description


class MockContext:
    """Mock context for testing"""

    def __init__(self, results=None, metadata=None):
        self.results = results or {}
        self.metadata = metadata or {}


class MockVisualizationBackend(VisualizationBackend):
    """Mock implementation of VisualizationBackend for testing"""

    def visualize_dag(self, dag: Any, **kwargs) -> str:  # noqa: ARG002
        return "mock_dag"

    def visualize_fsm(self, fsm: Any, **kwargs) -> str:  # noqa: ARG002
        return "mock_fsm"

    def save(self, content: str, filename: str, format: str = "png") -> None:
        pass


class TestVisualizationBaseCoverage:
    """Test visualization base to achieve 100% coverage"""

    def test_get_node_style_with_skipped_color(self):
        """Test get_node_style when node is skipped (lines 113-114)"""
        options = VisualizationOptions(show_results=True, skipped_color="#D3D3D3")
        backend = MockVisualizationBackend(options)

        node = MockNode("test_node")
        context = MockContext(results={}, metadata={})  # No results, no errors

        style = backend.get_node_style(node, context)

        # Should get skipped color since node is not in results and has no error
        assert style["fillcolor"] == "#D3D3D3"
        assert style["style"] == "filled"

    def test_get_node_style_with_success_color(self):
        """Test get_node_style when node is successful"""
        options = VisualizationOptions(show_results=True, success_color="#90EE90")
        backend = MockVisualizationBackend(options)

        node = MockNode("test_node")
        context = MockContext(results={"test_node": "success"})

        style = backend.get_node_style(node, context)

        # Should get success color since node is in results
        assert style["fillcolor"] == "#90EE90"
        assert style["style"] == "filled"

    def test_get_node_style_with_error_color(self):
        """Test get_node_style when node has error"""
        options = VisualizationOptions(show_results=True, error_color="#FFB6C1")
        backend = MockVisualizationBackend(options)

        node = MockNode("test_node")
        context = MockContext(results={}, metadata={"test_node_error": "some error"})

        style = backend.get_node_style(node, context)

        # Should get error color since node has error in metadata
        assert style["fillcolor"] == "#FFB6C1"
        assert style["style"] == "filled"

    def test_get_node_style_no_context(self):
        """Test get_node_style without context"""
        options = VisualizationOptions(show_results=True)
        backend = MockVisualizationBackend(options)

        node = MockNode("test_node")

        style = backend.get_node_style(node)

        # Should return empty style since no context
        assert style == {}

    def test_get_node_style_with_node_color(self):
        """Test get_node_style with default node color"""
        options = VisualizationOptions(node_color="#FFFFFF")
        backend = MockVisualizationBackend(options)

        node = MockNode("test_node")

        style = backend.get_node_style(node)

        # Should use node color when no results shown
        assert style["fillcolor"] == "#FFFFFF"
        assert style["style"] == "filled"

    def test_get_node_label_with_all_options(self):
        """Test get_node_label with all options enabled"""
        options = VisualizationOptions(
            show_inputs=True, show_outputs=True, show_description=True
        )
        backend = MockVisualizationBackend(options)

        node = MockNode(
            "test_node",
            inputs=["input1", "input2"],
            outputs=["output1"],
            description="This is a test node",
        )

        label = backend.get_node_label(node)

        assert "test_node" in label
        assert "in: input1, input2" in label
        assert "out: output1" in label
        assert "This is a test node" in label

    def test_get_node_label_with_long_description(self):
        """Test get_node_label with long description that gets truncated"""
        options = VisualizationOptions(show_description=True)
        backend = MockVisualizationBackend(options)

        long_description = "This is a very long description that should be truncated after 50 characters"
        node = MockNode("test_node", description=long_description)

        label = backend.get_node_label(node)

        # Should truncate after 50 characters
        assert "This is a very long description that should be tru..." in label
        assert len(long_description) > 50  # Verify our test description is indeed long

    def test_get_node_label_no_inputs_outputs(self):
        """Test get_node_label with no inputs/outputs"""
        options = VisualizationOptions(show_inputs=True, show_outputs=True)
        backend = MockVisualizationBackend(options)

        node = MockNode("test_node")  # No inputs/outputs

        label = backend.get_node_label(node)

        # Should only have node name
        assert label == "test_node"

    def test_visualization_options_defaults(self):
        """Test default values in VisualizationOptions"""
        options = VisualizationOptions()

        assert options.direction == "TB"
        assert options.node_shape == "box"
        assert options.show_results is False
        assert options.success_color == "#90EE90"
        assert options.error_color == "#FFB6C1"
        assert options.skipped_color == "#D3D3D3"
        assert options.show_inputs is True
        assert options.show_outputs is True
        assert options.show_types is False
        assert options.show_description is False
