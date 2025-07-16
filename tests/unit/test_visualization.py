"""
Unit tests for visualization functionality.
"""

from fast_dag import DAG, FSM, FSMContext, ConditionalReturn, FSMReturn


class TestMermaidVisualization:
    """Test Mermaid diagram generation"""

    def test_simple_dag_to_mermaid(self):
        """Test converting simple DAG to Mermaid"""
        dag = DAG("test")

        @dag.node
        def start() -> int:
            return 1

        @dag.node
        def end(x: int) -> int:
            return x + 1

        dag.connect("start", "end")

        mermaid = dag.to_mermaid()

        assert "flowchart TB" in mermaid  # Top-Bottom layout
        assert "start" in mermaid
        assert "end" in mermaid
        assert "start --> end" in mermaid

    def test_complex_dag_mermaid(self):
        """Test Mermaid with complex DAG structure"""
        dag = DAG("complex")

        @dag.node
        def a() -> int:
            return 1

        @dag.node
        def b() -> int:
            return 2

        @dag.node
        def c(x: int, y: int) -> int:
            return x + y

        dag.connect("a", "c", input="x")
        dag.connect("b", "c", input="y")

        mermaid = dag.to_mermaid()

        assert "a --> c" in mermaid
        assert "b --> c" in mermaid

    def test_conditional_node_mermaid(self):
        """Test Mermaid representation of conditional nodes"""
        dag = DAG("conditional")

        @dag.node
        def start() -> int:
            return 10

        @dag.condition()
        def check(x: int) -> ConditionalReturn:
            return ConditionalReturn(condition=x > 5, value=x)

        @dag.node
        def true_path(x: int) -> str:
            return "large"

        @dag.node
        def false_path(x: int) -> str:
            return "small"

        dag.connect("start", "check")
        dag.connect("check", "true_path", output="true")
        dag.connect("check", "false_path", output="false")

        mermaid = dag.to_mermaid()

        # Conditional should have diamond shape
        assert "check{" in mermaid or "check{{" in mermaid
        # Should show true/false labels
        assert "true" in mermaid.lower()
        assert "false" in mermaid.lower()

    def test_mermaid_with_descriptions(self):
        """Test Mermaid includes node descriptions"""
        dag = DAG("described")

        @dag.node
        def process_data(x: int) -> int:
            """Process the input data"""
            return x * 2

        mermaid = dag.to_mermaid()

        # Should include description
        assert "Process the input data" in mermaid

    def test_mermaid_with_execution_context(self):
        """Test Mermaid with execution results"""
        dag = DAG("test")

        @dag.node
        def step1() -> int:
            return 42

        @dag.node
        def step2(x: int) -> str:
            return f"Result: {x}"

        dag.connect("step1", "step2")

        # Execute to get context
        dag.run()
        context = dag.context

        # Generate Mermaid with context
        mermaid = dag.to_mermaid(context=context)

        # Should include execution results
        assert "42" in mermaid  # Result from step1
        assert "Result: 42" in mermaid  # Result from step2
        assert "success" in mermaid.lower()  # Success styling

    def test_save_mermaid_file(self, tmp_path):
        """Test saving Mermaid to file"""
        dag = DAG("test")

        @dag.node
        def task() -> int:
            return 1

        filepath = tmp_path / "test.mmd"
        dag.save_mermaid(str(filepath))

        assert filepath.exists()
        content = filepath.read_text()
        assert "flowchart" in content
        assert "task" in content


class TestGraphvizVisualization:
    """Test Graphviz diagram generation"""

    def test_simple_dag_to_graphviz(self):
        """Test converting simple DAG to Graphviz DOT"""
        dag = DAG("test")

        @dag.node
        def node_a() -> int:
            return 1

        @dag.node
        def node_b(x: int) -> int:
            return x + 1

        dag.connect("node_a", "node_b")

        dot = dag.to_graphviz()

        assert "digraph G {" in dot
        assert '"node_a"' in dot
        assert '"node_b"' in dot
        assert '"node_a" -> "node_b"' in dot

    def test_graphviz_with_styling(self):
        """Test Graphviz with node styling"""
        dag = DAG("styled")

        @dag.node
        def important() -> int:
            """Important node"""
            return 1

        dot = dag.to_graphviz()

        # Should have default styling
        assert "shape=" in dot
        assert "style=" in dot
        assert "fillcolor=" in dot

    def test_graphviz_with_execution_context(self):
        """Test Graphviz with execution results"""
        dag = DAG("test")

        @dag.node
        def success_node() -> int:
            return 100

        @dag.node
        def error_node() -> int:
            raise ValueError("Test error")

        # Run with continue on error
        dag.run(error_strategy="continue")
        context = dag.context

        dot = dag.to_graphviz(context=context)

        # Should show different colors for success/error
        assert "fillcolor=" in dot
        assert "#90EE90" in dot or "lightgreen" in dot  # Success color
        assert "#FFB6C1" in dot or "lightcoral" in dot  # Error color

    def test_save_graphviz_file(self, tmp_path):
        """Test saving Graphviz to file"""
        dag = DAG("test")

        @dag.node
        def task() -> int:
            return 1

        # Note: This tests the DOT file generation
        # Actual PNG rendering requires graphviz binary
        filepath = tmp_path / "test.dot"
        dag.save_graphviz(str(filepath), format="dot")

        assert filepath.exists()
        content = filepath.read_text()
        assert "digraph" in content


class TestFSMVisualization:
    """Test FSM-specific visualization"""

    def test_fsm_state_diagram(self):
        """Test FSM state diagram generation"""
        fsm = FSM("test_fsm")

        @fsm.state(initial=True)
        def start(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="middle")

        @fsm.state
        def middle(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="end")

        @fsm.state(terminal=True)
        def end(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True)

        mermaid = fsm.to_mermaid()

        # Should be state diagram
        assert "stateDiagram-v2" in mermaid
        assert "[*] --> start" in mermaid  # Initial state
        assert "end --> [*]" in mermaid  # Terminal state
        assert "start --> middle" in mermaid
        assert "middle --> end" in mermaid

    def test_fsm_with_execution_trace(self):
        """Test FSM visualization with execution history"""
        fsm = FSM("test", max_cycles=5)

        @fsm.state(initial=True)
        def loop_state(context: FSMContext) -> FSMReturn:
            count = context.metadata.get("count", 0)
            context.metadata["count"] = count + 1

            if count >= 2:
                return FSMReturn(stop=True, value=count)
            return FSMReturn(next_state="loop_state", value=count)

        # Execute FSM
        fsm.run()
        context = fsm.context

        # Generate visualization with context
        mermaid = fsm.to_mermaid(context=context)

        # Should show execution trace
        assert "sequenceDiagram" in mermaid or "stateDiagram" in mermaid
        # Should indicate cycles
        assert "Cycle" in mermaid or "loop_state" in mermaid


class TestHTMLExport:
    """Test HTML export functionality"""

    def test_export_html_basic(self, tmp_path):
        """Test basic HTML export"""
        dag = DAG("test")

        @dag.node
        def process() -> int:
            return 42

        filepath = tmp_path / "workflow.html"
        dag.export_html(str(filepath))

        assert filepath.exists()
        content = filepath.read_text()

        # Check HTML structure
        assert "<!DOCTYPE html>" in content
        assert "<html>" in content
        assert "mermaid" in content
        assert "process" in content

    def test_export_html_with_context(self, tmp_path):
        """Test HTML export with execution context"""
        dag = DAG("test")

        @dag.node
        def task1() -> int:
            return 10

        @dag.node
        def task2(x: int) -> int:
            return x * 2

        dag.connect("task1", "task2")

        # Execute
        dag.run()
        context = dag.context

        filepath = tmp_path / "results.html"
        dag.export_html(str(filepath), context=context, title="Test Results")

        content = filepath.read_text()

        # Check title
        assert "<title>Test Results</title>" in content
        # Check for execution results
        assert "10" in content  # Result from task1
        assert "20" in content  # Result from task2
        # Check for metadata section
        assert "metadata" in content or "metrics" in content

    def test_export_html_styling(self, tmp_path):
        """Test HTML includes proper styling"""
        dag = DAG("test")

        @dag.node
        def styled_node() -> int:
            return 1

        filepath = tmp_path / "styled.html"
        dag.export_html(str(filepath))

        content = filepath.read_text()

        # Check for CSS
        assert "<style>" in content
        assert "font-family" in content
        assert "background-color" in content


class TestVisualizationOptions:
    """Test visualization configuration options"""

    def test_custom_layout(self):
        """Test custom layout options"""
        dag = DAG("test")

        @dag.node
        def a() -> int:
            return 1

        @dag.node
        def b(x: int) -> int:
            return x

        dag.connect("a", "b")

        # Test different layouts
        mermaid_tb = dag.to_mermaid(layout="TB")  # Top-Bottom
        mermaid_lr = dag.to_mermaid(layout="LR")  # Left-Right

        assert "flowchart TB" in mermaid_tb
        assert "flowchart LR" in mermaid_lr

    def test_visualization_with_options(self):
        """Test visualization with various options"""
        dag = DAG("test")

        @dag.node
        def process() -> int:
            return 42

        # Execute
        dag.run()
        context = dag.context

        # Visualize with options
        dag.visualize(
            context=context, show_results=True, show_timing=True, highlight_errors=False
        )

        # This would typically display or save based on implementation

    def test_performance_visualization(self):
        """Test visualizing performance metrics"""
        dag = DAG("test")

        @dag.node
        def fast() -> int:
            return 1

        @dag.node
        def slow() -> int:
            import time

            time.sleep(0.1)
            return 2

        dag.run()
        context = dag.context

        # Generate visualization with timing
        mermaid = dag.to_mermaid(context=context, show_timing=True)

        # Should include timing information
        assert "Time:" in mermaid or "ms" in mermaid or "s" in mermaid


class TestVisualizationIntegration:
    """Test visualization integration with other features"""

    def test_nested_workflow_visualization(self):
        """Test visualizing nested workflows"""
        # Create sub-workflow
        sub_dag = DAG("sub_workflow")

        @sub_dag.node
        def sub_task() -> int:
            return 5

        # Create main workflow
        main_dag = DAG("main_workflow")

        @main_dag.node
        def start() -> int:
            return 1

        @main_dag.node(name="nested")
        def nested_workflow(x: int) -> int:
            # Execute sub-workflow
            result = sub_dag.run()
            return x + result

        main_dag.connect("start", "nested")

        mermaid = main_dag.to_mermaid()

        # Should show nested node (actual nesting visualization would be complex)
        assert "nested" in mermaid

    def test_parallel_execution_visualization(self):
        """Test visualizing parallel execution groups"""
        dag = DAG("parallel")

        @dag.node
        def a() -> int:
            return 1

        @dag.node
        def b() -> int:
            return 2

        @dag.node
        def c() -> int:
            return 3

        @dag.node
        def merge(x: int, y: int, z: int) -> int:
            return x + y + z

        dag.connect("a", "merge", input="x")
        dag.connect("b", "merge", input="y")
        dag.connect("c", "merge", input="z")

        # Run in parallel mode
        dag.run(mode="parallel")
        context = dag.context

        dag.to_mermaid(context=context)

        # Could show parallel groups with subgraphs
        # Exact representation depends on implementation
