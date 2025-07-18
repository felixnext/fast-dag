"""Unit tests for nested workflow functionality."""

import pytest

from fast_dag import DAG, FSM
from fast_dag.core.context import Context
from fast_dag.core.types import FSMReturn, NodeType


class TestNestedTypes:
    """Test nested workflow node types."""

    def test_dag_node_type_exists(self):
        """Test that DAG node type exists."""
        assert hasattr(NodeType, "DAG")
        assert NodeType.DAG.value == "dag"

    def test_fsm_node_type_exists(self):
        """Test that FSM node type exists."""
        assert hasattr(NodeType, "FSM")
        assert NodeType.FSM.value == "fsm"


class TestDAGNode:
    """Test DAGNode wrapper functionality."""

    def test_create_dag_node(self):
        """Test creating a node that wraps a DAG."""
        # Create inner DAG
        inner_dag = DAG("inner")

        @inner_dag.node
        def double(x: int) -> int:
            return x * 2

        # Create DAGNode
        from fast_dag.core.nested import create_dag_node

        dag_node = create_dag_node(
            dag=inner_dag,
            name="double_workflow",
            inputs=["x"],
            outputs=["result"],
        )

        assert dag_node.name == "double_workflow"
        assert dag_node.node_type == NodeType.DAG
        assert dag_node.inputs == ["x"]
        assert dag_node.outputs == ["result"]

    def test_dag_node_execution(self):
        """Test executing a DAG node."""
        # Create inner DAG
        inner_dag = DAG("calculator")

        @inner_dag.node
        def add(a: int, b: int) -> int:
            return a + b

        @inner_dag.node
        def multiply(sum_val: int, factor: int) -> int:
            return sum_val * factor

        inner_dag.connect("add", "multiply", output="result", input="sum_val")

        # Create DAGNode
        from fast_dag.core.nested import create_dag_node

        dag_node = create_dag_node(
            dag=inner_dag,
            name="calc_workflow",
            inputs=["a", "b", "factor"],
            outputs=["result"],
        )

        # Execute the node
        result = dag_node.execute({"a": 3, "b": 4, "factor": 2})
        assert result == 14  # (3 + 4) * 2

    def test_dag_node_with_context_propagation(self):
        """Test that context is propagated to nested DAG."""
        # Create inner DAG
        inner_dag = DAG("inner")

        @inner_dag.node
        def get_from_context(context: Context) -> str:
            return context.metadata.get("shared_value", "not found")

        # Create DAGNode
        from fast_dag.core.nested import create_dag_node

        dag_node = create_dag_node(dag=inner_dag, name="context_test")

        # Execute with context
        context = Context()
        context.metadata["shared_value"] = "hello from parent"

        result = dag_node.execute({}, context=context)
        assert result == "hello from parent"


class TestFSMNode:
    """Test FSMNode wrapper functionality."""

    def test_create_fsm_node(self):
        """Test creating a node that wraps an FSM."""
        # Create inner FSM
        inner_fsm = FSM("inner")

        @inner_fsm.state(initial=True)
        def start() -> FSMReturn:
            return FSMReturn(next_state="end", value="done")

        @inner_fsm.state(terminal=True)
        def end() -> FSMReturn:
            return FSMReturn(stop=True, value="finished")

        # Create FSMNode
        from fast_dag.core.nested import create_fsm_node

        fsm_node = create_fsm_node(
            fsm=inner_fsm,
            name="state_machine",
            inputs=[],
            outputs=["result"],
        )

        assert fsm_node.name == "state_machine"
        assert fsm_node.node_type == NodeType.FSM
        assert fsm_node.outputs == ["result"]

    def test_fsm_node_execution(self):
        """Test executing an FSM node."""
        # Create inner FSM - simpler test that doesn't rely on auto-passing values
        inner_fsm = FSM("simple_fsm")

        @inner_fsm.state(initial=True)
        def process(value: int) -> FSMReturn:
            # Simple FSM that just doubles the value and stops
            return FSMReturn(stop=True, value=value * 2)

        # Create FSMNode
        from fast_dag.core.nested import create_fsm_node

        fsm_node = create_fsm_node(
            fsm=inner_fsm,
            name="simple_fsm",
            inputs=["value"],
            outputs=["result"],
        )

        # Execute the node
        result = fsm_node.execute({"value": 5})
        assert result == 10


class TestNestedWorkflowIntegration:
    """Test integration of nested workflows in parent workflows."""

    def test_dag_with_nested_dag(self):
        """Test a DAG containing another DAG as a node."""
        # Create inner DAG
        inner_dag = DAG("preprocessor")

        @inner_dag.node
        def normalize(data: list) -> list:
            return [x / 100 for x in data]

        @inner_dag.node
        def filter_outliers(data: list) -> list:
            return [x for x in data if 0 <= x <= 1]

        inner_dag.connect("normalize", "filter_outliers", input="data")

        # Create outer DAG
        outer_dag = DAG("pipeline")

        @outer_dag.node
        def load_data() -> list:
            return [10, 50, 150, 75, 200]

        # Add nested DAG as a node
        outer_dag.add_dag(
            "preprocess",
            inner_dag,
            inputs={"data": "data"},
            outputs={"result": "cleaned_data"},
        )

        @outer_dag.node
        def analyze(cleaned_data: list) -> dict:
            return {
                "count": len(cleaned_data),
                "mean": sum(cleaned_data) / len(cleaned_data) if cleaned_data else 0,
            }

        # Connect nodes
        outer_dag.connect("load_data", "preprocess", input="data")
        outer_dag.connect(
            "preprocess", "analyze", output="cleaned_data", input="cleaned_data"
        )

        # Execute
        result = outer_dag.run()
        assert result["count"] == 3  # Only 10, 50, 75 pass the filter
        assert 0 < result["mean"] < 1

    def test_dag_with_nested_fsm(self):
        """Test a DAG containing an FSM as a node."""
        # Create a simple FSM that processes a string
        text_fsm = FSM("text_processor")

        @text_fsm.state(initial=True)
        def process(text: str) -> FSMReturn:
            # Simple FSM that just uppercases the text and stops
            return FSMReturn(stop=True, value=text.upper())

        # Create outer DAG
        dag = DAG("text_pipeline")

        @dag.node
        def get_text() -> str:
            return "hello world"

        # Add FSM as a node
        dag.add_fsm(
            "uppercase",
            text_fsm,
            inputs={"text": "text"},
            outputs={"result": "upper_text"},
        )

        @dag.node
        def finalize(upper_text: str) -> str:
            return f"Processed: {upper_text}"

        # Connect nodes
        dag.connect("get_text", "uppercase", input="text")
        dag.connect("uppercase", "finalize", output="upper_text", input="upper_text")

        # Execute
        result = dag.run()
        assert result == "Processed: HELLO WORLD"

    def test_nested_dag_result_access(self):
        """Test accessing results from nested DAG nodes."""
        # Create inner DAG
        inner = DAG("inner")

        @inner.node
        def step1() -> int:
            return 10

        @inner.node
        def step2(x: int) -> int:
            return x * 2

        inner.connect("step1", "step2", input="x")

        # Create outer DAG
        outer = DAG("outer")

        outer.add_dag("nested", inner)

        # Run outer DAG
        outer.run()

        # Should be able to access nested results
        assert outer["nested"] == 20  # Final result
        assert outer["nested.step1"] == 10  # Intermediate result
        assert outer["nested.step2"] == 20  # Final step result

    def test_deep_nesting(self):
        """Test deeply nested workflows (3+ levels)."""
        # Level 3 - innermost
        level3 = DAG("level3")

        @level3.node
        def compute() -> int:
            return 42

        # Level 2
        level2 = DAG("level2")
        level2.add_dag("l3", level3)

        @level2.node
        def double(x: int) -> int:
            return x * 2

        level2.connect("l3", "double", input="x")

        # Level 1 - outermost
        level1 = DAG("level1")
        level1.add_dag("l2", level2)

        @level1.node
        def finalize(x: int) -> str:
            return f"Result: {x}"

        level1.connect("l2", "finalize", input="x")

        # Execute
        result = level1.run()
        assert result == "Result: 84"  # 42 * 2

    def test_error_propagation_in_nested_workflow(self):
        """Test that errors in nested workflows propagate correctly."""
        # Create inner DAG with error
        inner = DAG("inner")

        @inner.node
        def fail() -> None:
            raise ValueError("Inner error")

        # Create outer DAG
        outer = DAG("outer")
        outer.add_dag("nested", inner)

        # Execute should propagate error
        with pytest.raises(Exception, match="Inner error"):
            outer.run()

    def test_nested_workflow_with_caching(self):
        """Test that nested workflows can use caching."""
        call_count = 0

        # Create inner DAG with cached node
        inner = DAG("inner")

        @inner.cached_node
        def expensive_op(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        # Create outer DAG
        outer = DAG("outer")
        outer.add_dag("nested", inner, inputs={"x": "x"})

        # First run
        result1 = outer.run(x=5)
        assert result1 == 10
        assert call_count == 1

        # Second run should use cache
        result2 = outer.run(x=5)
        assert result2 == 10
        assert call_count == 1  # Not incremented
