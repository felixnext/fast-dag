"""
Comprehensive tests for DAG nested workflow functionality to improve coverage.
"""

from fast_dag import DAG, FSM, FSMContext, FSMReturn
from fast_dag.core.nested import DAGNode
from fast_dag.core.types import NodeType


class TestNestedWorkflows:
    """Test nested workflow functionality"""

    def test_add_dag_basic(self):
        """Test adding a DAG as a node"""
        # Create nested DAG
        nested_dag = DAG("nested_dag")

        @nested_dag.node
        def process(x: int) -> int:
            return x * 2

        # Create parent DAG
        parent_dag = DAG("parent")

        # Add nested DAG
        dag_node = parent_dag.add_dag("nested", nested_dag)

        assert dag_node.name == "nested"
        assert dag_node.node_type == NodeType.DAG
        assert hasattr(dag_node, "dag")
        assert dag_node.dag == nested_dag
        assert dag_node.description == "Nested DAG: nested_dag"

    def test_add_dag_with_auto_detect_inputs(self):
        """Test auto-detecting inputs from entry points"""
        # Create nested DAG with multiple entry points
        nested_dag = DAG("multi_entry")

        @nested_dag.node
        def entry1(a: int, b: int) -> int:
            return a + b

        @nested_dag.node
        def entry2(c: int) -> int:
            return c * 2

        @nested_dag.node
        def combine(x: int, y: int) -> int:
            return x + y

        nested_dag.connect("entry1", "combine", input="x")
        nested_dag.connect("entry2", "combine", input="y")

        # Create parent DAG
        parent_dag = DAG("parent")

        # Add nested DAG without specifying inputs - should auto-detect
        dag_node = parent_dag.add_dag("nested", nested_dag)

        # Should detect inputs from entry points
        assert set(dag_node.inputs) == {"a", "b", "c"}

    def test_add_dag_with_dict_inputs_outputs(self):
        """Test adding DAG with dict-based inputs/outputs mapping"""
        nested_dag = DAG("nested")

        @nested_dag.node
        def process(value: int) -> dict[str, int]:
            return {"result": value * 2, "original": value}

        parent_dag = DAG("parent")

        # Add with dict inputs/outputs to define mapping
        dag_node = parent_dag.add_dag(
            "nested",
            nested_dag,
            inputs={"parent_input": "value"},  # parent_input -> value
            outputs={"parent_output": "result", "parent_original": "original"},
        )

        # Should extract keys
        assert dag_node.inputs == ["parent_input"]
        assert dag_node.outputs == ["parent_output", "parent_original"]

    def test_add_dag_with_metadata(self):
        """Test adding DAG with custom metadata"""
        nested_dag = DAG("nested")

        @nested_dag.node
        def task() -> int:
            return 42

        parent_dag = DAG("parent")

        dag_node = parent_dag.add_dag(
            "nested",
            nested_dag,
            description="Custom nested DAG",
            metadata={"version": "1.0", "author": "test"},
        )

        assert dag_node.description == "Custom nested DAG"
        assert dag_node.metadata["version"] == "1.0"
        assert dag_node.metadata["author"] == "test"
        assert dag_node.metadata["node_name"] == "nested"  # Added automatically

    def test_add_dag_without_run_method(self):
        """Test adding DAG that doesn't have run method"""

        # Create a mock DAG without run method
        class MockDAG:
            name = "mock_dag"
            nodes = {}

            def get_entry_points(self):
                return []

        mock_dag = MockDAG()
        parent_dag = DAG("parent")

        # Should still work with lambda fallback
        dag_node = parent_dag.add_dag("mock", mock_dag)  # type: ignore
        assert dag_node.func is not None

    def test_add_fsm_basic(self):
        """Test adding an FSM as a node"""
        # Create nested FSM
        nested_fsm = FSM("nested_fsm")

        @nested_fsm.state(initial=True)
        def start(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="end", value="started")

        @nested_fsm.state(terminal=True)
        def end(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value="done")

        # Create parent DAG
        parent_dag = DAG("parent")

        # Add nested FSM
        fsm_node = parent_dag.add_fsm("state_machine", nested_fsm)

        assert fsm_node.name == "state_machine"
        assert fsm_node.node_type == NodeType.FSM
        assert hasattr(fsm_node, "fsm")
        assert fsm_node.fsm == nested_fsm
        assert fsm_node.description == "Nested FSM: nested_fsm"

    def test_add_fsm_with_dict_inputs_outputs(self):
        """Test adding FSM with dict-based inputs/outputs"""
        nested_fsm = FSM("nested")

        @nested_fsm.state(initial=True, terminal=True)
        def process(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value={"result": 100})

        parent_dag = DAG("parent")

        # Add with dict inputs/outputs
        fsm_node = parent_dag.add_fsm(
            "fsm",
            nested_fsm,
            inputs={"fsm_input": "data"},
            outputs={"fsm_output": "result"},
        )

        assert fsm_node.inputs == ["fsm_input"]
        assert fsm_node.outputs == ["fsm_output"]

    def test_add_fsm_without_run_method(self):
        """Test adding FSM without run method"""

        # Create a mock FSM without run method
        class MockFSM:
            name = "mock_fsm"

        mock_fsm = MockFSM()
        parent_dag = DAG("parent")

        # Should still work with lambda fallback
        fsm_node = parent_dag.add_fsm("mock", mock_fsm)  # type: ignore
        assert fsm_node.func is not None

    def test_add_workflow_with_dag(self):
        """Test add_workflow with a DAG"""
        nested_dag = DAG("nested")

        @nested_dag.node
        def task() -> str:
            return "dag_result"

        parent_dag = DAG("parent")

        # add_workflow should delegate to add_dag
        node = parent_dag.add_workflow("workflow", nested_dag)

        assert node.node_type == NodeType.DAG
        assert hasattr(node, "dag")

    def test_add_workflow_with_fsm(self):
        """Test add_workflow with an FSM"""
        nested_fsm = FSM("nested")

        @nested_fsm.state(initial=True, terminal=True)
        def state() -> FSMReturn:
            return FSMReturn(stop=True, value="fsm_result")

        parent_dag = DAG("parent")

        # add_workflow should delegate to add_fsm
        node = parent_dag.add_workflow("workflow", nested_fsm)

        assert node.node_type == NodeType.FSM
        assert hasattr(node, "fsm")

    def test_get_nested_workflows(self):
        """Test getting all nested workflows"""
        parent_dag = DAG("parent")

        # Add regular node
        @parent_dag.node
        def regular() -> int:
            return 1

        # Add nested DAG
        nested_dag = DAG("nested_dag")

        @nested_dag.node
        def dag_task() -> int:
            return 2

        parent_dag.add_dag("dag_node", nested_dag)

        # Add nested FSM
        nested_fsm = FSM("nested_fsm")

        @nested_fsm.state(initial=True, terminal=True)
        def fsm_state() -> FSMReturn:
            return FSMReturn(stop=True, value=3)

        parent_dag.add_fsm("fsm_node", nested_fsm)

        # Get nested workflows
        nested = parent_dag.get_nested_workflows()

        assert len(nested) == 2
        assert "dag_node" in nested
        assert "fsm_node" in nested
        assert nested["dag_node"] == nested_dag
        assert nested["fsm_node"] == nested_fsm
        # Regular node should not be included
        assert "regular" not in nested

    def test_get_nested_depth_single_level(self):
        """Test getting nesting depth with single level"""
        parent_dag = DAG("parent")

        # Add regular node
        @parent_dag.node
        def regular() -> int:
            return 1

        # Add nested DAG
        nested_dag = DAG("nested")

        @nested_dag.node
        def task() -> int:
            return 2

        parent_dag.add_dag("nested", nested_dag)

        # Single level nesting
        assert parent_dag.get_nested_depth() == 1

    def test_get_nested_depth_multiple_levels(self):
        """Test getting nesting depth with multiple levels"""
        # Create deeply nested structure
        level3_dag = DAG("level3")

        @level3_dag.node
        def deep_task() -> int:
            return 3

        level2_dag = DAG("level2")
        level2_dag.add_dag("level3", level3_dag)

        level1_dag = DAG("level1")
        level1_dag.add_dag("level2", level2_dag)

        parent_dag = DAG("parent")
        parent_dag.add_dag("level1", level1_dag)

        # Should have depth of 3
        assert parent_dag.get_nested_depth() == 3

    def test_get_nested_depth_with_fsm(self):
        """Test getting nesting depth with FSM"""
        # Create nested FSM with nested DAG
        nested_dag = DAG("nested_dag")

        @nested_dag.node
        def task() -> int:
            return 1

        nested_fsm = FSM("nested_fsm")
        # Mock the FSM to have nested workflows
        nested_fsm.add_dag = lambda _name, _dag: None  # type: ignore
        nested_fsm.nodes = {
            "dag_node": DAGNode(
                func=lambda: None,
                name="dag_node",
                dag=nested_dag,
                node_type=NodeType.DAG,
            )
        }
        nested_fsm.get_nested_depth = lambda: 1  # type: ignore

        parent_dag = DAG("parent")
        parent_dag.add_fsm("fsm", nested_fsm)

        # Should include FSM depth
        assert parent_dag.get_nested_depth() == 2

    def test_flatten_nested_workflows(self):
        """Test flattening nested workflows"""
        parent_dag = DAG("parent", metadata={"version": "1.0"})

        # Add regular node
        @parent_dag.node
        def process(x: int) -> int:
            return x * 2

        # Add nested DAG
        nested_dag = DAG("nested_dag")

        @nested_dag.node
        def nested_task(y: int) -> int:
            return y + 1

        parent_dag.add_dag("nested", nested_dag)

        # Add nested FSM
        nested_fsm = FSM("nested_fsm")

        @nested_fsm.state(initial=True, terminal=True)
        def state() -> FSMReturn:
            return FSMReturn(stop=True, value="done")

        # Mock flatten method for FSM
        nested_fsm.flatten_nested_workflows = lambda: {  # type: ignore
            "name": "nested_fsm",
            "type": "FSM",
            "nodes": {"state": {"name": "state", "type": "state"}},
            "nested_workflows": {},
        }

        parent_dag.add_fsm("fsm", nested_fsm)

        # Flatten structure
        flattened = parent_dag.flatten_nested_workflows()

        assert flattened["name"] == "parent"
        assert flattened["type"] == "DAG"

        # Check regular nodes
        assert "process" in flattened["nodes"]
        assert flattened["nodes"]["process"]["name"] == "process"
        assert flattened["nodes"]["process"]["function"] == "process"
        assert flattened["nodes"]["process"]["inputs"] == ["x"]

        # Check nested workflows
        assert "nested" in flattened["nested_workflows"]
        assert flattened["nested_workflows"]["nested"]["name"] == "nested_dag"
        assert "fsm" in flattened["nested_workflows"]

    def test_flatten_with_anonymous_function(self):
        """Test flattening with anonymous function node"""
        parent_dag = DAG("parent")

        # Add node with lambda function (anonymous)
        parent_dag.add_node("lambda_node", lambda x: x + 1)

        flattened = parent_dag.flatten_nested_workflows()

        # Lambda functions should show as <lambda>
        assert flattened["nodes"]["lambda_node"]["function"] == "<lambda>"

    def test_nested_dag_execution(self):
        """Test executing a nested DAG within parent DAG"""
        # Create nested DAG
        nested_dag = DAG("multiplier")

        @nested_dag.node
        def multiply(x: int) -> int:
            return x * 2

        # Create parent DAG
        parent_dag = DAG("parent")

        @parent_dag.node
        def start() -> int:
            return 5

        # Add nested DAG
        parent_dag.add_dag("multiplier", nested_dag, inputs=["x"])

        # Connect
        parent_dag.connect("start", "multiplier", input="x")

        # Execute
        result = parent_dag.run()
        assert result == 10  # 5 * 2

    def test_deeply_nested_execution(self):
        """Test executing deeply nested DAGs"""
        # Level 2 DAG
        level2 = DAG("add_one")

        @level2.node
        def add(x: int) -> int:
            return x + 1

        # Level 1 DAG
        level1 = DAG("multiply_and_add")

        @level1.node
        def multiply(x: int) -> int:
            return x * 2

        level1.add_dag("adder", level2)
        level1.connect("multiply", "adder", input="x")

        # Parent DAG
        parent = DAG("parent")

        @parent.node
        def start() -> int:
            return 3

        parent.add_dag("processor", level1)
        parent.connect("start", "processor", input="x")

        # Execute: 3 * 2 + 1 = 7
        result = parent.run()
        assert result == 7
