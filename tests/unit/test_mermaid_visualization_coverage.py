"""
Tests for mermaid visualization to improve coverage.
"""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from fast_dag import DAG, FSM, Context, FSMContext, FSMReturn
from fast_dag.visualization.mermaid import MermaidBackend


class TestMermaidVisualizationCoverage:
    """Test mermaid visualization comprehensively"""

    def test_visualize_dag_with_different_node_types(self):
        """Test DAG visualization with different node types (lines 33, 35, 37, 41)"""
        dag = DAG("test_dag")

        # Standard node
        @dag.node
        def standard_node() -> int:
            return 1

        # Conditional node
        @dag.condition
        def conditional_node(x: int) -> bool:
            return x > 0

        # SELECT node (trapezoid)
        @dag.select
        def select_node(inputs: list[int]) -> int:
            return max(inputs)

        # ANY node (circle)
        @dag.any
        def any_node(inputs: list[int]) -> int:
            return sum(inputs)

        # ALL node (circle)
        @dag.all
        def all_node(inputs: list[int]) -> int:
            return sum(inputs)

        # Add nested DAG (subroutine shape)
        nested_dag = DAG("nested")

        @nested_dag.node
        def nested_task() -> int:
            return 42

        dag.add_dag("dag_node", nested_dag)

        # Add nested FSM (stadium shape)
        nested_fsm = FSM("nested_fsm")

        @nested_fsm.state(initial=True, terminal=True)
        def fsm_state(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value="done")

        dag.add_fsm("fsm_node", nested_fsm)

        # Create visualization
        backend = MermaidBackend()
        backend.options = MagicMock()
        backend.options.direction = "TD"
        backend.options.show_results = False
        backend.options.show_inputs = False

        result = backend.visualize_dag(dag)

        # Check that different node shapes are used
        assert "standard_node[" in result  # Rectangle
        assert "conditional_node{{" in result  # Diamond
        assert "select_node[/" in result  # Trapezoid
        assert "any_node((" in result  # Circle
        assert "all_node((" in result  # Circle
        assert "dag_node[[" in result  # Subroutine
        assert "fsm_node[(" in result  # Stadium

    def test_visualize_dag_with_results_and_styling(self):
        """Test DAG visualization with results and styling"""
        dag = DAG("styled_dag")

        @dag.node
        def task() -> int:
            return 42

        # Create context with results
        context = Context()
        context.results["task"] = 42

        # Create backend with styling
        backend = MermaidBackend()
        backend.options = MagicMock()
        backend.options.direction = "TD"
        backend.options.show_results = True
        backend.options.show_inputs = False

        # Mock the get_node_style method to return a style
        with patch.object(backend, "get_node_style") as mock_style:
            mock_style.return_value = {"fillcolor": "lightgreen"}

            result = backend.visualize_dag(dag, context=context)

            # Check that styling is applied
            assert "style task fill:lightgreen" in result

    def test_visualize_dag_with_edge_labels(self):
        """Test DAG visualization with edge labels"""
        dag = DAG("edge_labels")

        @dag.node
        def producer() -> dict[str, int]:
            return {"output1": 1, "output2": 2}

        @dag.node
        def consumer(custom_input: int) -> int:
            return custom_input * 2

        # Create non-default connection
        dag.connect("producer", "consumer", output="output1", input="custom_input")

        backend = MermaidBackend()
        backend.options = MagicMock()
        backend.options.direction = "TD"
        backend.options.show_results = False
        backend.options.show_inputs = True

        result = backend.visualize_dag(dag)

        # Check that edge label is shown
        assert "producer -->|output1 → custom_input| consumer" in result

    def test_visualize_fsm_with_options(self):
        """Test FSM visualization with options (line 73)"""
        fsm = FSM("test_fsm")

        @fsm.state(initial=True)
        def start(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="middle", value="start")

        @fsm.state
        def middle(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="end", value="middle")

        @fsm.state(terminal=True)
        def end(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value="end")

        backend = MermaidBackend()
        options = MagicMock()
        options.direction = "TB"

        result = backend.visualize_fsm(fsm, options=options)

        assert "stateDiagram-v2" in result
        assert backend.options == options

    def test_visualize_fsm_with_terminal_states(self):
        """Test FSM visualization with terminal states (line 92)"""
        fsm = FSM("terminal_fsm")

        @fsm.state(initial=True)
        def start(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="end", value="start")

        @fsm.state(terminal=True)
        def end(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value="end")

        backend = MermaidBackend()
        backend.options = MagicMock()

        result = backend.visualize_fsm(fsm)

        # Check initial state arrow
        assert "[*] --> start" in result
        # Check terminal state arrow
        assert "end --> [*]" in result

    def test_visualize_fsm_with_state_transitions(self):
        """Test FSM visualization with state transitions (lines 96-102)"""
        fsm = FSM("transition_fsm")

        @fsm.state(initial=True)
        def start(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="middle", value="start")

        @fsm.state
        def middle(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="end", value="middle")

        @fsm.state(terminal=True)
        def end(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value="end")

        # Add custom transitions
        fsm.add_transition("start", "end", condition="default")
        fsm.add_transition("middle", "start", condition="error")

        backend = MermaidBackend()
        backend.options = MagicMock()

        result = backend.visualize_fsm(fsm)

        # Check default transition
        assert "start --> end" in result
        # Check labeled transition
        assert "middle --> start : error" in result

    def test_visualize_fsm_with_node_connections(self):
        """Test FSM visualization with node connections (lines 108-116)"""
        fsm = FSM("connection_fsm")

        @fsm.state(initial=True)
        def start(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="end", value="start")

        @fsm.state(terminal=True)
        def end(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value="end")

        # Create a manual connection (not in state_transitions)
        fsm.nodes["start"].output_connections["result"] = [(fsm.nodes["end"], "input")]

        backend = MermaidBackend()
        backend.options = MagicMock()

        result = backend.visualize_fsm(fsm)

        # Should include connection that's not in state_transitions
        assert "start --> end" in result

    def test_visualize_fsm_skip_duplicate_connections(self):
        """Test FSM visualization skips duplicate connections"""
        fsm = FSM("duplicate_fsm")

        @fsm.state(initial=True)
        def start(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="end", value="start")

        @fsm.state(terminal=True)
        def end(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value="end")

        # Add both state transition and node connection
        fsm.add_transition("start", "end", condition="default")
        fsm.nodes["start"].output_connections["result"] = [(fsm.nodes["end"], "input")]

        backend = MermaidBackend()
        backend.options = MagicMock()

        result = backend.visualize_fsm(fsm)

        # Should only appear once
        lines = result.split("\n")
        start_to_end_lines = [line for line in lines if "start --> end" in line]
        assert len(start_to_end_lines) == 1

    def test_save_mermaid_source_only(self):
        """Test saving Mermaid source only"""
        backend = MermaidBackend()
        content = "graph TD\n    A --> B"

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "test"

            backend.save(content, str(filepath), format="mmd")

            # Check mermaid file was created
            mmd_file = Path(f"{filepath}.mmd")
            assert mmd_file.exists()
            assert mmd_file.read_text() == content

    def test_save_with_mermaid_cli_success(self):
        """Test saving with mermaid CLI success (lines 126-143)"""
        backend = MermaidBackend()
        content = "graph TD\n    A --> B"

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "test"

            # Mock successful subprocess call
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock()

                backend.save(content, str(filepath), format="png")

                # Check mermaid file was created
                mmd_file = Path(f"{filepath}.mmd")
                assert mmd_file.exists()
                assert mmd_file.read_text() == content

                # Check subprocess was called
                mock_run.assert_called_once()
                args = mock_run.call_args[0][0]
                assert args == [
                    "mmdc",
                    "-i",
                    f"{filepath}.mmd",
                    "-o",
                    f"{filepath}.png",
                ]

    def test_save_with_mermaid_cli_failure(self):
        """Test saving with mermaid CLI failure (lines 139-143)"""
        backend = MermaidBackend()
        content = "graph TD\n    A --> B"

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "test"

            # Mock failed subprocess call
            with (
                patch("subprocess.run") as mock_run,
                patch("builtins.print") as mock_print,
            ):
                mock_run.side_effect = FileNotFoundError("mmdc not found")

                backend.save(content, str(filepath), format="png")

                # Check warning was printed
                mock_print.assert_any_call(
                    "Warning: Could not render Mermaid diagram. Install mermaid-cli with 'npm install -g @mermaid-js/mermaid-cli'"
                )
                mock_print.assert_any_call(f"Mermaid source saved to {filepath}.mmd")

    def test_save_with_subprocess_error(self):
        """Test saving with subprocess error"""
        backend = MermaidBackend()
        content = "graph TD\n    A --> B"

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "test"

            # Mock subprocess CalledProcessError
            with (
                patch("subprocess.run") as mock_run,
                patch("builtins.print") as mock_print,
            ):
                import subprocess

                mock_run.side_effect = subprocess.CalledProcessError(1, "mmdc")

                backend.save(content, str(filepath), format="svg")

                # Check warning was printed
                mock_print.assert_called()

    def test_get_mermaid_style_no_style(self):
        """Test _get_mermaid_style with no style (line 154)"""
        backend = MermaidBackend()

        # Mock get_node_style to return None
        with patch.object(backend, "get_node_style", return_value=None):
            node = MagicMock()
            node.name = "test_node"
            context = MagicMock()

            result = backend._get_mermaid_style(node, context)
            assert result is None

    def test_get_mermaid_style_no_fillcolor(self):
        """Test _get_mermaid_style with style but no fillcolor (line 162)"""
        backend = MermaidBackend()

        # Mock get_node_style to return style without fillcolor
        with patch.object(
            backend, "get_node_style", return_value={"strokecolor": "red"}
        ):
            node = MagicMock()
            node.name = "test_node"
            context = MagicMock()

            result = backend._get_mermaid_style(node, context)
            assert result is None

    def test_get_mermaid_style_with_fillcolor(self):
        """Test _get_mermaid_style with fillcolor"""
        backend = MermaidBackend()

        # Mock get_node_style to return style with fillcolor
        with patch.object(
            backend, "get_node_style", return_value={"fillcolor": "blue"}
        ):
            node = MagicMock()
            node.name = "test_node"
            context = MagicMock()

            result = backend._get_mermaid_style(node, context)
            assert result == "    style test_node fill:blue"

    def test_sanitize_id(self):
        """Test ID sanitization"""
        backend = MermaidBackend()

        # Test various characters that need sanitization
        assert backend._sanitize_id("normal_id") == "normal_id"
        assert backend._sanitize_id("id with spaces") == "id_with_spaces"
        assert backend._sanitize_id("id-with-dashes") == "id_with_dashes"
        assert backend._sanitize_id("id.with.dots") == "id_with_dots"
        assert (
            backend._sanitize_id("complex id-with.mixed chars")
            == "complex_id_with_mixed_chars"
        )
