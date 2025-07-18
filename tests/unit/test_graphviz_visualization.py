"""
Comprehensive unit tests for Graphviz visualization to improve coverage.
"""

import builtins
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from fast_dag import DAG, FSM, Context, FSMContext, FSMReturn
from fast_dag.core.node import Node
from fast_dag.core.types import NodeType


class TestGraphvizVisualization:
    """Test Graphviz visualization backend"""

    @pytest.fixture
    def setup_viz(self):
        """Setup visualization modules"""
        try:
            from fast_dag.visualization import VisualizationOptions
            from fast_dag.visualization.graphviz import GraphvizBackend

            return GraphvizBackend, VisualizationOptions
        except ImportError:
            pytest.skip("Visualization dependencies not installed")

    def test_dag_with_all_node_types(self, setup_viz):
        """Test DAG visualization with all node types"""
        GraphvizBackend, VisualizationOptions = setup_viz

        dag = DAG("all_node_types")

        # Standard node
        @dag.node
        def standard() -> int:
            return 1

        # Conditional node
        @dag.condition
        def check(x: int) -> bool:
            return x > 0

        # Select node
        @dag.select
        def selector(condition: str) -> int:
            return {"a": 1, "b": 2}.get(condition, 0)

        # ANY node
        @dag.any
        def any_node(inputs: list[int]) -> int:
            return sum(inputs)

        # ALL node
        @dag.all
        def all_node(inputs: list[int]) -> int:
            return sum(inputs)

        # Nested DAG node
        nested_dag = DAG("nested")

        @nested_dag.node
        def nested_task() -> int:
            return 10

        dag.add_node(
            Node(
                func=lambda: nested_dag.run(),
                name="nested_dag_node",
                node_type=NodeType.DAG,
            )
        )

        # Nested FSM node
        nested_fsm = FSM("nested_fsm")

        @nested_fsm.state(initial=True, terminal=True)
        def fsm_state() -> FSMReturn:
            return FSMReturn(stop=True, value=20)

        dag.add_node(
            Node(
                func=lambda: nested_fsm.run(),
                name="nested_fsm_node",
                node_type=NodeType.FSM,
            )
        )

        # Create backend and visualize
        backend = GraphvizBackend()
        options = VisualizationOptions(font_size=12)
        dot_code = backend.visualize_dag(dag, options=options)

        # Check all node shapes
        assert "shape=box" in dot_code  # standard
        assert "shape=diamond" in dot_code  # conditional
        assert "shape=trapezium" in dot_code  # select
        assert "shape=ellipse" in dot_code  # any/all
        assert "shape=box3d" in dot_code  # nested DAG
        assert "shape=doublecircle" in dot_code  # nested FSM
        assert "fontsize=12" in dot_code  # font size option

    def test_dag_with_execution_context(self, setup_viz):
        """Test DAG visualization with execution context"""
        GraphvizBackend, VisualizationOptions = setup_viz

        dag = DAG("with_context")

        @dag.node
        def success() -> int:
            return 42

        @dag.node
        def failure() -> int:
            raise ValueError("Failed")

        @dag.node
        def skipped() -> int:
            return 0

        # Create context with results
        context = Context()
        context.set_result("success", 42)
        # For error nodes, the context might track them differently
        context.metrics["nodes_failed"] = 1
        context.metadata["error_nodes"] = ["failure"]
        # skipped has no result

        dag.context = context

        # Create backend and visualize
        backend = GraphvizBackend()
        dot_code = backend.visualize_dag(dag)

        # Should have node styling based on context
        assert '"success"' in dot_code
        assert '"failure"' in dot_code
        assert '"skipped"' in dot_code

    def test_dag_with_edge_labels(self, setup_viz):
        """Test DAG visualization with custom edge labels"""
        GraphvizBackend, VisualizationOptions = setup_viz

        dag = DAG("edge_labels")

        @dag.node
        def source() -> dict[str, int]:
            return {"out1": 1, "out2": 2}

        @dag.node
        def target1(in1: int) -> int:
            return in1

        @dag.node
        def target2(in2: int) -> int:
            return in2

        # Connect with custom outputs/inputs
        dag.connect("source", "target1", output="out1", input="in1")
        dag.connect("source", "target2", output="out2", input="in2")

        # Create backend and visualize with edge labels
        backend = GraphvizBackend()
        options = VisualizationOptions(show_inputs=True, edge_color="blue")
        dot_code = backend.visualize_dag(dag, options=options)

        # Check edge labels
        assert "out1 → in1" in dot_code
        assert "out2 → in2" in dot_code
        assert 'color="blue"' in dot_code

    def test_dag_without_custom_edge_labels(self, setup_viz):
        """Test DAG visualization with default edge connections"""
        GraphvizBackend, VisualizationOptions = setup_viz

        dag = DAG("default_edges")

        @dag.node
        def source() -> int:
            return 1

        @dag.node
        def target(data: int) -> int:
            return data * 2

        # Connect with defaults (result -> data)
        dag.nodes["source"] >> dag.nodes["target"]

        # Create backend and visualize
        backend = GraphvizBackend()
        options = VisualizationOptions(show_inputs=True)
        dot_code = backend.visualize_dag(dag, options=options)

        # Should not have edge labels for default connections
        assert "→" not in dot_code

    def test_dag_with_node_color_option(self, setup_viz):
        """Test DAG visualization with node color option"""
        GraphvizBackend, VisualizationOptions = setup_viz

        dag = DAG("colored_nodes")

        @dag.node
        def task() -> int:
            return 1

        # Create backend and visualize with node color
        backend = GraphvizBackend()
        options = VisualizationOptions(node_color="lightblue")
        dot_code = backend.visualize_dag(dag, options=options)

        # Check node color
        assert 'fillcolor="lightblue"' in dot_code
        assert 'style="filled"' in dot_code

    def test_fsm_visualization_complete(self, setup_viz):
        """Test FSM visualization with all features"""
        GraphvizBackend, VisualizationOptions = setup_viz

        fsm = FSM("complete_fsm")

        @fsm.state(initial=True)
        def start() -> FSMReturn:
            return FSMReturn(next_state="processing")

        @fsm.state
        def processing() -> FSMReturn:
            return FSMReturn(next_state="done")

        @fsm.state(terminal=True)
        def done() -> FSMReturn:
            return FSMReturn(stop=True)

        # Add manual state transitions
        fsm.state_transitions = {
            "start": {"default": "processing", "error": "done"},
            "processing": {"success": "done", "retry": "processing"},
        }

        # Create backend and visualize
        backend = GraphvizBackend()
        options = VisualizationOptions(font_size=14, edge_color="red")
        dot_code = backend.visualize_fsm(fsm, options=options)

        # Check FSM specific elements
        assert "digraph FSM" in dot_code
        assert "__start" in dot_code  # Initial state arrow
        assert "shape=doublecircle" in dot_code  # Terminal state
        assert 'label="error"' in dot_code  # Conditional transition
        assert 'label="success"' in dot_code
        assert 'label="retry"' in dot_code
        assert 'color="red"' in dot_code
        assert "fontsize=14" in dot_code

    def test_fsm_with_execution_context(self, setup_viz):
        """Test FSM visualization with execution context"""
        GraphvizBackend, VisualizationOptions = setup_viz

        fsm = FSM("fsm_with_context")

        @fsm.state(initial=True)
        def state1() -> FSMReturn:
            return FSMReturn(next_state="state2")

        @fsm.state(terminal=True)
        def state2() -> FSMReturn:
            return FSMReturn(stop=True)

        # Create context
        context = FSMContext()
        fsm.context = context

        # Create backend and visualize
        backend = GraphvizBackend()
        options = VisualizationOptions(node_color="yellow")
        dot_code = backend.visualize_fsm(fsm, options=options)

        # Should apply node color
        assert 'fillcolor="yellow"' in dot_code

    def test_fsm_with_connections_only(self, setup_viz):
        """Test FSM visualization using connections instead of state_transitions"""
        GraphvizBackend, VisualizationOptions = setup_viz

        fsm = FSM("connection_fsm")
        fsm.state_transitions = {}  # Empty transitions

        # Create nodes with output connections
        node1 = Node(func=lambda: None, name="state1")
        node2 = Node(func=lambda: None, name="state2")
        node1.output_connections["result"] = [(node2, "input")]

        fsm.nodes = {"state1": node1, "state2": node2}

        # Create backend and visualize
        backend = GraphvizBackend()
        dot_code = backend.visualize_fsm(fsm)

        # Should show connection
        assert '"state1" -> "state2"' in dot_code

    def test_fsm_without_initial_state(self, setup_viz):
        """Test FSM visualization without initial state"""
        GraphvizBackend, VisualizationOptions = setup_viz

        fsm = FSM("no_initial")
        fsm.initial_state = None

        @fsm.state
        def state1() -> FSMReturn:
            return FSMReturn(stop=True)

        # Create backend and visualize
        backend = GraphvizBackend()
        dot_code = backend.visualize_fsm(fsm)

        # Should not have start node
        assert "__start" not in dot_code

    def test_save_dot_format(self, setup_viz):
        """Test saving visualization in DOT format"""
        GraphvizBackend, _ = setup_viz

        dag = DAG("save_test")

        @dag.node
        def task() -> int:
            return 1

        backend = GraphvizBackend()
        dot_code = backend.visualize_dag(dag)

        with tempfile.TemporaryDirectory() as tmpdir:
            filename = os.path.join(tmpdir, "test")
            backend.save(dot_code, filename, format="dot")

            # Check that .dot file was created
            dot_file = f"{filename}.dot"
            assert os.path.exists(dot_file)

            # Check content
            with open(dot_file) as f:
                content = f.read()
            assert content == dot_code

    @patch("subprocess.run")
    def test_save_png_with_subprocess(self, mock_run, setup_viz):
        """Test saving visualization as PNG using subprocess"""
        GraphvizBackend, _ = setup_viz

        dag = DAG("save_png")

        @dag.node
        def task() -> int:
            return 1

        backend = GraphvizBackend()
        dot_code = backend.visualize_dag(dag)

        with tempfile.TemporaryDirectory() as tmpdir:
            filename = os.path.join(tmpdir, "test")

            # Mock successful subprocess call
            mock_run.return_value = MagicMock(returncode=0)

            backend.save(dot_code, filename, format="png")

            # Check subprocess was called correctly
            mock_run.assert_called_once()
            call_args = mock_run.call_args[0][0]
            assert call_args[0] == "dot"
            assert call_args[1] == "-Tpng"
            assert call_args[2] == f"{filename}.dot"
            assert call_args[4] == f"{filename}.png"

    @patch("subprocess.run", side_effect=FileNotFoundError)
    def test_save_png_fallback_to_graphviz_module(self, _mock_run, setup_viz):
        """Test fallback to graphviz Python module when subprocess fails"""
        GraphvizBackend, _ = setup_viz

        dag = DAG("save_fallback")

        @dag.node
        def task() -> int:
            return 1

        backend = GraphvizBackend()
        dot_code = backend.visualize_dag(dag)

        with tempfile.TemporaryDirectory() as tmpdir:
            filename = os.path.join(tmpdir, "test")

            # Create a mock graphviz module
            mock_graphviz_module = MagicMock()
            mock_source = MagicMock()
            mock_graphviz_module.Source.return_value = mock_source

            # Patch sys.modules to inject our mock
            with patch.dict("sys.modules", {"graphviz": mock_graphviz_module}):
                backend.save(dot_code, filename, format="png")

                # Check graphviz module was used
                mock_graphviz_module.Source.assert_called_once_with(dot_code)
                mock_source.render.assert_called_once_with(
                    filename, format="png", cleanup=True
                )

    @patch("subprocess.run", side_effect=FileNotFoundError)
    def test_save_png_no_graphviz_module(self, _mock_run, setup_viz, capsys):
        """Test error message when neither subprocess nor graphviz module work"""
        GraphvizBackend, _ = setup_viz

        dag = DAG("save_error")

        @dag.node
        def task() -> int:
            return 1

        backend = GraphvizBackend()
        dot_code = backend.visualize_dag(dag)

        with tempfile.TemporaryDirectory() as tmpdir:
            filename = os.path.join(tmpdir, "test")

            # Store original
            original_import = builtins.__import__

            def mock_import(name, *args, **kwargs):
                if name == "graphviz":
                    raise ImportError("No module named 'graphviz'")
                return original_import(name, *args, **kwargs)

            # Temporarily replace __import__
            builtins.__import__ = mock_import
            try:
                backend.save(dot_code, filename, format="png")

                # Check warning message
                captured = capsys.readouterr()
                assert "Warning: Could not render Graphviz diagram" in captured.out
                assert f"DOT source saved to {filename}.dot" in captured.out
            finally:
                # Restore original import
                builtins.__import__ = original_import

    def test_fsm_with_duplicate_transitions(self, setup_viz):
        """Test FSM visualization avoids duplicate transitions"""
        GraphvizBackend, _ = setup_viz

        fsm = FSM("duplicate_test")

        # Create nodes
        node1 = Node(func=lambda: None, name="state1")
        node2 = Node(func=lambda: None, name="state2")

        # Add both state transition and connection
        fsm.state_transitions = {"state1": {"default": "state2"}}
        node1.output_connections["result"] = [(node2, "input")]

        fsm.nodes = {"state1": node1, "state2": node2}

        # Create backend and visualize
        backend = GraphvizBackend()
        dot_code = backend.visualize_fsm(fsm)

        # Count occurrences of the transition
        transition_count = dot_code.count('"state1" -> "state2"')
        # Should only appear once (state_transitions takes precedence)
        assert transition_count == 1
