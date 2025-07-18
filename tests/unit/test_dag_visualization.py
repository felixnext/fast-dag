"""Unit tests for DAG visualization functionality."""

from unittest.mock import MagicMock, patch

import pytest

from fast_dag import DAG
from fast_dag.core.types import ConditionalReturn, SelectReturn


class TestDAGVisualization:
    """Test DAG visualization methods."""

    def test_visualize_default_backend(self):
        """Test visualization with default backend."""
        dag = DAG("test")

        @dag.node
        def a() -> int:
            return 1

        @dag.node
        def b(x: int) -> int:
            return x + 1

        dag.connect("a", "b", input="x")

        # Mock the visualizer
        with patch("fast_dag.dag.visualization.MermaidBackend") as mock_viz:
            mock_instance = MagicMock()
            mock_viz.return_value = mock_instance
            mock_instance.visualize_dag.return_value = "graph TD"

            result = dag.visualize()

            mock_viz.assert_called_once()
            mock_instance.visualize_dag.assert_called_once_with(dag)
            assert result == "graph TD"

    def test_visualize_mermaid_backend(self):
        """Test visualization with Mermaid backend."""
        dag = DAG("pipeline")

        @dag.node
        def process() -> str:
            return "done"

        with patch("fast_dag.dag.visualization.MermaidBackend") as mock_viz:
            mock_instance = MagicMock()
            mock_viz.return_value = mock_instance
            mock_instance.visualize_dag.return_value = "graph LR"

            result = dag.visualize(backend="mermaid", direction="LR")

            mock_instance.visualize_dag.assert_called_once_with(dag, direction="LR")
            assert result == "graph LR"

    def test_visualize_graphviz_backend(self):
        """Test visualization with Graphviz backend."""
        dag = DAG("workflow")

        @dag.node
        def start() -> int:
            return 0

        with patch("fast_dag.dag.visualization.GraphvizBackend") as mock_viz:
            mock_instance = MagicMock()
            mock_viz.return_value = mock_instance
            mock_instance.visualize_dag.return_value = "digraph G {"

            result = dag.visualize(backend="graphviz", show_types=False)

            mock_viz.assert_called_once()
            mock_instance.visualize_dag.assert_called_once_with(dag, show_types=False)
            assert result == "digraph G {"

    def test_visualize_invalid_backend(self):
        """Test visualization with invalid backend."""
        dag = DAG("test")

        with pytest.raises(ValueError, match="Unknown visualization backend"):
            dag.visualize(backend="invalid")

    def test_visualize_with_execution_results(self):
        """Test visualization after execution."""
        dag = DAG("executed")

        @dag.node
        def compute() -> int:
            return 42

        # Execute the DAG
        result = dag.run()
        assert result == 42

        # Mock visualizer to verify it receives executed context
        with patch("fast_dag.dag.visualization.MermaidBackend") as mock_viz:
            mock_instance = MagicMock()
            mock_viz.return_value = mock_instance

            dag.visualize(show_results=True)

            # Check that visualize_dag was called with a context
            call_args = mock_instance.visualize_dag.call_args
            assert len(call_args[0]) == 1  # DAG argument
            assert "context" in call_args[1]
            assert call_args[1]["context"] is not None

    def test_visualize_conditional_workflow(self):
        """Test visualization of conditional nodes."""
        dag = DAG("conditional")

        @dag.node
        def check(x: int) -> ConditionalReturn:
            return ConditionalReturn(condition=x > 0, value=x)

        @dag.node
        def positive(x: int) -> str:
            return "positive"

        @dag.node
        def negative(x: int) -> str:
            return "negative"

        dag.connect("check", "positive", output="on_true", input="x")
        dag.connect("check", "negative", output="on_false", input="x")

        # Test visualization captures conditional structure
        with patch("fast_dag.dag.visualization.MermaidBackend") as mock_viz:
            mock_instance = MagicMock()
            mock_viz.return_value = mock_instance

            dag.visualize()

            # Verify DAG has the expected structure
            assert len(dag.nodes) == 3
            assert "check" in dag.nodes
            assert dag.nodes["check"].outputs == ["on_true", "on_false"]

    def test_visualize_select_workflow(self):
        """Test visualization of select nodes."""
        dag = DAG("select_flow")

        @dag.node
        def route(x: int) -> SelectReturn:
            if x < 0:
                return SelectReturn(branch="negative", value=x)
            elif x == 0:
                return SelectReturn(branch="zero", value=x)
            else:
                return SelectReturn(branch="positive", value=x)

        @dag.node
        def handle_negative(x: int) -> str:
            return "neg"

        @dag.node
        def handle_zero(x: int) -> str:
            return "zero"

        @dag.node
        def handle_positive(x: int) -> str:
            return "pos"

        dag.connect("route", "handle_negative", output="negative", input="x")
        dag.connect("route", "handle_zero", output="zero", input="x")
        dag.connect("route", "handle_positive", output="positive", input="x")

        # Ensure visualization works with select nodes
        with patch("fast_dag.dag.visualization.MermaidBackend") as mock_viz:
            mock_instance = MagicMock()
            mock_viz.return_value = mock_instance
            mock_instance.visualize_dag.return_value = "graph TD"

            result = dag.visualize()
            assert result == "graph TD"

    def test_visualize_nested_workflow(self):
        """Test visualization of nested workflows."""
        # Create inner DAG
        inner = DAG("inner")

        @inner.node
        def double(x: int) -> int:
            return x * 2

        # Create outer DAG
        outer = DAG("outer")

        @outer.node
        def start() -> int:
            return 5

        # Add inner DAG as node
        outer.add_dag("process", inner)
        outer.connect("start", "process", input="x")

        @outer.node
        def finish(x: int) -> str:
            return f"Result: {x}"

        outer.connect("process", "finish", input="x")

        with patch("fast_dag.dag.visualization.MermaidBackend") as mock_viz:
            mock_instance = MagicMock()
            mock_viz.return_value = mock_instance

            outer.visualize(show_nested=True)

            # Verify nested structure is handled
            assert len(outer.nodes) == 3
            assert "process" in outer.nodes

    def test_visualize_empty_dag(self):
        """Test visualization of empty DAG."""
        dag = DAG("empty")

        with patch("fast_dag.dag.visualization.MermaidBackend") as mock_viz:
            mock_instance = MagicMock()
            mock_viz.return_value = mock_instance
            mock_instance.visualize_dag.return_value = "graph TD\n  empty[Empty DAG]"

            result = dag.visualize()
            assert "Empty DAG" in result

    def test_visualize_complex_dag(self):
        """Test visualization of complex DAG with multiple paths."""
        dag = DAG("complex")

        @dag.node
        def input1() -> int:
            return 1

        @dag.node
        def input2() -> int:
            return 2

        @dag.node
        def process1(a: int, b: int) -> int:
            return a + b

        @dag.node
        def process2(x: int) -> int:
            return x * 2

        @dag.node
        def final(p1: int, p2: int) -> int:
            return p1 + p2

        # Create complex connections
        dag.connect("input1", "process1", input="a")
        dag.connect("input2", "process1", input="b")
        dag.connect("input1", "process2", input="x")
        dag.connect("process1", "final", input="p1")
        dag.connect("process2", "final", input="p2")

        with patch("fast_dag.dag.visualization.MermaidBackend") as mock_viz:
            mock_instance = MagicMock()
            mock_viz.return_value = mock_instance

            dag.visualize()

            # Verify complex structure
            assert len(dag.nodes) == 5
            # Check that final node has two incoming connections
            final_node = dag.nodes["final"]
            assert len(final_node.inputs) == 2

    def test_visualize_with_custom_options(self):
        """Test visualization with custom options."""
        dag = DAG("custom")

        @dag.node(description="Start node")
        def start() -> int:
            return 1

        # Test various option combinations
        with patch("fast_dag.dag.visualization.MermaidBackend") as mock_viz:
            mock_instance = MagicMock()
            mock_viz.return_value = mock_instance

            dag.visualize(
                show_types=False,
                show_descriptions=True,
                highlight_path=["start"],
                custom_option="test",
            )

            # Verify options are passed through
            call_kwargs = mock_instance.visualize_dag.call_args[1]
            assert call_kwargs["show_types"] is False
            assert call_kwargs["show_descriptions"] is True
            assert call_kwargs["highlight_path"] == ["start"]
            assert call_kwargs["custom_option"] == "test"
