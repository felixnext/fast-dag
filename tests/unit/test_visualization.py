"""Test visualization functionality."""

import pytest

from fast_dag import DAG, FSM
from fast_dag.core.types import FSMReturn


class TestVisualizationBasic:
    """Test basic visualization functionality."""

    def test_visualization_requires_optional_deps(self):
        """Test that visualization raises error without deps."""
        dag = DAG("test")

        @dag.node
        def task() -> int:
            return 42

        # Check if visualization deps are installed
        try:
            import fast_dag.visualization  # noqa: F401

            # If we can import, skip this test
            pytest.skip("Visualization dependencies are installed")
        except ImportError:
            # Good, dependencies not installed, test the error
            pass

        # This should fail if visualization deps are not installed
        with pytest.raises(ImportError, match="optional dependencies"):
            dag.visualize()

    def test_dag_mermaid_visualization(self):
        """Test DAG Mermaid visualization."""
        try:
            import fast_dag.visualization  # noqa: F401
        except ImportError:
            pytest.skip("Visualization dependencies not installed")

        dag = DAG("pipeline")

        @dag.node
        def fetch() -> dict:
            return {"data": [1, 2, 3]}

        @dag.node
        def process(data: dict) -> list:
            return data["data"]

        @dag.node
        def aggregate(items: list) -> int:
            return sum(items)

        # Connect nodes
        dag.connect("fetch", "process", input="data")
        dag.connect("process", "aggregate", input="items")

        # Generate visualization
        mermaid_code = dag.visualize(backend="mermaid")

        # Check basic structure
        assert "graph TB" in mermaid_code
        assert "fetch" in mermaid_code
        assert "process" in mermaid_code
        assert "aggregate" in mermaid_code
        assert "-->" in mermaid_code

    def test_dag_graphviz_visualization(self):
        """Test DAG Graphviz visualization."""
        try:
            import fast_dag.visualization  # noqa: F401
        except ImportError:
            pytest.skip("Visualization dependencies not installed")

        dag = DAG("pipeline")

        @dag.node
        def a() -> int:
            return 1

        @dag.node
        def b() -> int:
            return 2

        dag.nodes["a"] >> dag.nodes["b"]

        # Generate visualization
        dot_code = dag.visualize(backend="graphviz")

        # Check basic structure
        assert "digraph DAG" in dot_code
        assert '"a"' in dot_code
        assert '"b"' in dot_code
        assert "->" in dot_code

    def test_fsm_mermaid_visualization(self):
        """Test FSM Mermaid visualization."""
        try:
            import fast_dag.visualization  # noqa: F401
        except ImportError:
            pytest.skip("Visualization dependencies not installed")

        fsm = FSM("traffic_light")

        @fsm.state(initial=True)
        def red():
            return FSMReturn(next_state="green")

        @fsm.state
        def green():
            return FSMReturn(next_state="yellow")

        @fsm.state
        def yellow():
            return FSMReturn(next_state="red")

        # Generate visualization
        mermaid_code = fsm.visualize(backend="mermaid")

        # Check basic structure
        assert "stateDiagram-v2" in mermaid_code
        assert "[*] --> red" in mermaid_code
        assert "red" in mermaid_code
        assert "green" in mermaid_code
        assert "yellow" in mermaid_code

    def test_conditional_node_visualization(self):
        """Test visualization of conditional nodes."""
        try:
            import fast_dag.visualization  # noqa: F401
        except ImportError:
            pytest.skip("Visualization dependencies not installed")

        dag = DAG("conditional_flow")

        @dag.node
        def start() -> int:
            return 10

        @dag.condition
        def check(x: int) -> bool:
            return x > 5

        @dag.node
        def if_true(x: int) -> str:
            return f"Large: {x}"

        @dag.node
        def if_false(x: int) -> str:
            return f"Small: {x}"

        # Connect nodes
        dag.connect("start", "check", input="x")
        dag.nodes["check"].on_true >> dag.nodes["if_true"]
        dag.nodes["check"].on_false >> dag.nodes["if_false"]

        # Generate visualization
        mermaid_code = dag.visualize(backend="mermaid")

        # Check that conditional node has diamond shape
        assert "check{" in mermaid_code  # Diamond shape in Mermaid

    def test_visualization_with_results(self):
        """Test visualization with execution results."""
        try:
            import fast_dag.visualization  # noqa: F401
            from fast_dag.visualization import VisualizationOptions
        except ImportError:
            pytest.skip("Visualization dependencies not installed")

        dag = DAG("pipeline")

        @dag.node
        def success() -> int:
            return 42

        @dag.node
        def failure() -> int:
            raise ValueError("Failed")

        # Run with continue on error
        dag.run(error_strategy="continue")

        # Visualize with results
        options = VisualizationOptions(show_results=True)
        mermaid_code = dag.visualize(options=options)

        # Should have styling for executed nodes
        assert "style success fill:" in mermaid_code
        assert "style failure fill:" in mermaid_code


class TestVisualizationOptions:
    """Test visualization options."""

    def test_custom_options(self):
        """Test custom visualization options."""
        try:
            import fast_dag.visualization  # noqa: F401
            from fast_dag.visualization import VisualizationOptions
        except ImportError:
            pytest.skip("Visualization dependencies not installed")

        dag = DAG("test")

        @dag.node
        def task(x: int, y: int) -> int:
            """Add two numbers."""
            return x + y

        # Test different options
        options = VisualizationOptions(
            direction="LR",
            show_inputs=True,
            show_outputs=True,
            show_description=True,
            node_color="#FFD700",
        )

        # Mermaid
        mermaid_code = dag.visualize(backend="mermaid", options=options)
        assert "graph LR" in mermaid_code
        assert "in: x, y" in mermaid_code
        assert "out: result" in mermaid_code
        assert "Add two numbers" in mermaid_code

        # Graphviz
        dot_code = dag.visualize(backend="graphviz", options=options)
        assert "rankdir=LR" in dot_code
        assert 'fillcolor="#FFD700"' in dot_code
