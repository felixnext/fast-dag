"""
Comprehensive tests for DAG visualization to improve coverage.
"""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from fast_dag import DAG


class TestDAGVisualizationMethods:
    """Test DAG visualization methods comprehensively"""

    def test_to_mermaid_method(self):
        """Test to_mermaid convenience method"""
        dag = DAG("test_dag")

        @dag.node
        def process() -> int:
            return 42

        with patch("fast_dag.dag.visualization.MermaidBackend") as mock_backend:
            mock_instance = MagicMock()
            mock_backend.return_value = mock_instance
            mock_instance.visualize_dag.return_value = "graph TD\n  process[process]"

            result = dag.to_mermaid(direction="LR", show_types=True)

            # Should call visualize with mermaid backend
            mock_instance.visualize_dag.assert_called_once_with(
                dag, direction="LR", show_types=True
            )
            assert result == "graph TD\n  process[process]"

    def test_to_graphviz_method(self):
        """Test to_graphviz convenience method"""
        dag = DAG("test_dag")

        @dag.node
        def task() -> str:
            return "done"

        with patch("fast_dag.dag.visualization.GraphvizBackend") as mock_backend:
            mock_instance = MagicMock()
            mock_backend.return_value = mock_instance
            mock_instance.visualize_dag.return_value = "digraph G { task }"

            result = dag.to_graphviz(rankdir="LR", node_attr={"shape": "box"})

            # Should call visualize with graphviz backend
            mock_instance.visualize_dag.assert_called_once_with(
                dag, rankdir="LR", node_attr={"shape": "box"}
            )
            assert result == "digraph G { task }"

    def test_save_visualization_mermaid(self):
        """Test saving visualization to file with mermaid"""
        dag = DAG("save_test")

        @dag.node
        def step1() -> int:
            return 1

        @dag.node
        def step2(x: int) -> int:
            return x + 1

        dag.connect("step1", "step2", input="x")

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "test.mmd"

            with patch("fast_dag.dag.visualization.MermaidBackend") as mock_backend:
                mock_instance = MagicMock()
                mock_backend.return_value = mock_instance
                mock_instance.visualize_dag.return_value = "graph TD\n  step1 --> step2"

                dag.save_visualization(str(filepath), backend="mermaid")

                # Check file was written
                assert filepath.exists()
                content = filepath.read_text()
                assert content == "graph TD\n  step1 --> step2"

    def test_save_visualization_graphviz(self):
        """Test saving visualization to file with graphviz"""
        dag = DAG("graphviz_save")

        @dag.node
        def node_a() -> int:
            return 10

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "test.dot"

            with patch("fast_dag.dag.visualization.GraphvizBackend") as mock_backend:
                mock_instance = MagicMock()
                mock_backend.return_value = mock_instance
                mock_instance.visualize_dag.return_value = "digraph G {\n  node_a;\n}"

                dag.save_visualization(
                    str(filepath), backend="graphviz", show_results=False, rankdir="TB"
                )

                # Verify backend was called correctly
                mock_instance.visualize_dag.assert_called_once_with(dag, rankdir="TB")

                # Check file was written
                assert filepath.exists()
                content = filepath.read_text()
                assert content == "digraph G {\n  node_a;\n}"

    def test_save_visualization_with_results(self):
        """Test saving visualization with execution results"""
        dag = DAG("results_save")

        @dag.node
        def compute() -> int:
            return 99

        # Execute to have results
        result = dag.run()
        assert result == 99

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "results.mmd"

            with patch("fast_dag.dag.visualization.MermaidBackend") as mock_backend:
                mock_instance = MagicMock()
                mock_backend.return_value = mock_instance
                mock_instance.visualize_dag.return_value = (
                    "graph TD\n  compute[compute: 99]"
                )

                dag.save_visualization(str(filepath), show_results=True)

                # Verify context was passed
                call_kwargs = mock_instance.visualize_dag.call_args[1]
                assert "context" in call_kwargs
                assert call_kwargs["context"] is not None

    def test_visualize_with_options_object(self):
        """Test visualization with options object that has show_results"""
        dag = DAG("options_test")

        @dag.node
        def process() -> str:
            return "processed"

        # Execute DAG
        dag.run()

        # Create mock options object
        class Options:
            show_results = True

        options = Options()

        with patch("fast_dag.dag.visualization.MermaidBackend") as mock_backend:
            mock_instance = MagicMock()
            mock_backend.return_value = mock_instance

            dag.visualize(options=options)

            # Should pass context when options.show_results is True
            call_kwargs = mock_instance.visualize_dag.call_args[1]
            assert "context" in call_kwargs
            assert call_kwargs["context"] is not None

    def test_get_visualization_info_basic(self):
        """Test getting visualization info for basic DAG"""
        dag = DAG("info_test", description="Test DAG for info")

        @dag.node
        def start() -> int:
            return 0

        @dag.node
        def process(x: int) -> int:
            return x + 1

        dag.connect("start", "process", input="x")

        info = dag.get_visualization_info()

        assert info["name"] == "info_test"
        assert info["description"] == "Test DAG for info"
        assert info["total_nodes"] == 2
        assert info["entry_points"] == ["start"]
        assert info["execution_order"] == ["start", "process"]
        assert info["node_types"]["standard"] == 2
        assert len(info["connections"]) == 1
        assert info["connections"][0] == {
            "source": "start",
            "target": "process",
            "output": "result",
            "input": "x",
        }

    def test_get_visualization_info_with_node_types(self):
        """Test visualization info with different node types"""
        dag = DAG("node_types_test")

        @dag.node
        def standard() -> int:
            return 1

        @dag.condition
        def check(x: int) -> bool:
            return x > 0

        @dag.any
        def any_node(inputs: list[int]) -> int:
            return sum(inputs)

        @dag.all
        def all_node(inputs: list[int]) -> int:
            return sum(inputs)

        # Create connections
        dag.connect("standard", "check", input="x")

        info = dag.get_visualization_info()

        assert info["node_types"]["standard"] == 1
        assert info["node_types"]["conditional"] == 1
        assert info["node_types"]["any"] == 1
        assert info["node_types"]["all"] == 1

    def test_get_visualization_info_nested_workflows(self):
        """Test visualization info with nested workflows"""
        # Create nested DAG
        nested_dag = DAG("nested_dag")

        @nested_dag.node
        def nested_task() -> int:
            return 10

        # Create nested FSM
        from fast_dag import FSM, FSMContext, FSMReturn

        nested_fsm = FSM("nested_fsm")

        @nested_fsm.state(initial=True, terminal=True)
        def state(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value="done")

        # Create main DAG
        main_dag = DAG("main")

        @main_dag.node
        def start() -> int:
            return 1

        # Add nested workflows
        main_dag.add_dag("dag_node", nested_dag)
        main_dag.add_fsm("fsm_node", nested_fsm)

        info = main_dag.get_visualization_info()

        assert len(info["nested_workflows"]) == 2
        assert info["nested_workflows"]["dag_node"] == {
            "type": "DAG",
            "name": "nested_dag",
            "nodes": 1,
        }
        assert info["nested_workflows"]["fsm_node"] == {
            "type": "FSM",
            "name": "nested_fsm",
            "nodes": 1,
        }

    def test_get_visualization_info_invalid_dag(self):
        """Test visualization info for invalid DAG"""
        dag = DAG("invalid")

        @dag.node
        def orphan() -> int:
            return 1

        @dag.node
        def disconnected() -> int:
            return 2

        # Don't connect the nodes - they remain disconnected
        # Mock is_valid property to return False
        with patch.object(type(dag), "is_valid", property(lambda _self: False)):
            info = dag.get_visualization_info()

            # execution_order should be None for invalid DAG
            assert info["execution_order"] is None

    def test_get_visualization_info_complex_connections(self):
        """Test visualization info with complex connections"""
        dag = DAG("complex")

        @dag.node
        def source() -> dict[str, int]:
            return {"a": 1, "b": 2}

        @dag.node
        def target1(x: int) -> int:
            return x

        @dag.node
        def target2(y: int) -> int:
            return y

        # Multiple outputs to different targets
        dag.connect("source", "target1", output="a", input="x")
        dag.connect("source", "target2", output="b", input="y")

        info = dag.get_visualization_info()

        assert len(info["connections"]) == 2

        # Find connections
        conn1 = next(c for c in info["connections"] if c["target"] == "target1")
        assert conn1["output"] == "a"
        assert conn1["input"] == "x"

        conn2 = next(c for c in info["connections"] if c["target"] == "target2")
        assert conn2["output"] == "b"
        assert conn2["input"] == "y"

    def test_visualize_no_context_with_show_results(self):
        """Test visualization with show_results but no context"""
        dag = DAG("no_context")

        @dag.node
        def task() -> int:
            return 1

        # Explicitly set context to None to test the branch
        dag.context = None

        with patch("fast_dag.dag.visualization.MermaidBackend") as mock_backend:
            mock_instance = MagicMock()
            mock_backend.return_value = mock_instance

            dag.visualize(show_results=True)

            # Should not pass context since it's None
            call_kwargs = mock_instance.visualize_dag.call_args[1]
            assert "context" not in call_kwargs

    def test_save_visualization_file_write_error(self):
        """Test save visualization with file write error"""
        dag = DAG("write_error")

        @dag.node
        def task() -> int:
            return 1

        with patch("fast_dag.dag.visualization.MermaidBackend") as mock_backend:
            mock_instance = MagicMock()
            mock_backend.return_value = mock_instance
            mock_instance.visualize_dag.return_value = "graph TD"

            # Mock file open to raise error
            with (
                patch("builtins.open", side_effect=OSError("Permission denied")),
                pytest.raises(IOError, match="Permission denied"),
            ):
                dag.save_visualization("/invalid/path/file.mmd")
