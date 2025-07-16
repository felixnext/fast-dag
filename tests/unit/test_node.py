"""
Unit tests for Node class and node creation functionality.
"""

import pytest
import asyncio
from typing import Any
from fast_dag import Node, NodeType, ConditionalReturn, FSMReturn


class TestNodeCreation:
    """Test Node creation and initialization"""

    def test_simple_node_creation(self, simple_functions):
        """Test creating a basic node"""
        func = simple_functions["simple"]
        node = Node(func)

        assert node.name == "simple_function"
        assert node.func == func
        assert node.node_type == NodeType.STANDARD
        assert node.description == "A simple function that doubles input"

    def test_node_with_custom_name(self, simple_functions):
        """Test creating node with custom name"""
        func = simple_functions["simple"]
        node = Node(func, name="custom_name")

        assert node.name == "custom_name"
        assert node.func == func

    def test_node_with_explicit_inputs_outputs(self, simple_functions):
        """Test creating node with explicit inputs/outputs"""
        func = simple_functions["add"]
        node = Node(func, inputs=["x", "y"], outputs=["sum"])

        assert node.inputs == ["x", "y"]
        assert node.outputs == ["sum"]

    def test_node_type_assignment(self, simple_functions):
        """Test assigning different node types"""
        func = simple_functions["simple"]

        # Standard node
        standard = Node(func, node_type=NodeType.STANDARD)
        assert standard.node_type == NodeType.STANDARD

        # Conditional node
        conditional = Node(func, node_type=NodeType.CONDITIONAL)
        assert conditional.node_type == NodeType.CONDITIONAL

        # FSM state node
        fsm = Node(func, node_type=NodeType.FSM_STATE)
        assert fsm.node_type == NodeType.FSM_STATE


class TestNodeIntrospection:
    """Test automatic function introspection"""

    def test_input_introspection(self, simple_functions):
        """Test automatic input detection from function signature"""
        # Single parameter
        node1 = Node(simple_functions["simple"])
        assert node1.inputs == ["x"]

        # Multiple parameters
        node2 = Node(simple_functions["add"])
        assert node2.inputs == ["a", "b"]

        # No parameters
        def no_params() -> int:
            return 42

        node3 = Node(no_params)
        assert node3.inputs == []

    def test_context_parameter_exclusion(self):
        """Test that 'context' parameter is excluded from inputs"""

        def with_context(data: str, context: Any) -> str:
            return data.upper()

        node = Node(with_context)
        assert node.inputs == ["data"]  # context should be excluded

    def test_output_introspection(self):
        """Test automatic output detection from return annotation"""

        # Simple return
        def returns_int() -> int:
            return 42

        node1 = Node(returns_int)
        assert node1.outputs == ["result"]

        # Dict return
        def returns_dict() -> dict:
            return {"key": "value"}

        node2 = Node(returns_dict)
        assert node2.outputs == ["result"]

        # Tuple return with names
        def returns_tuple() -> tuple[str, int]:
            return "hello", 42

        node3 = Node(returns_tuple, outputs=["text", "number"])
        assert node3.outputs == ["text", "number"]

        # No return annotation
        def no_annotation(x):
            return x

        node4 = Node(no_annotation)
        assert node4.outputs == ["result"]

    def test_description_from_docstring(self):
        """Test extracting description from docstring"""

        def with_docstring(x: int) -> int:
            """This is a test function.

            It has multiple lines.
            """
            return x

        node = Node(with_docstring)
        assert node.description == "This is a test function."

        # No docstring
        def no_docstring(x: int) -> int:
            return x

        node2 = Node(no_docstring)
        assert node2.description is None


class TestNodeValidation:
    """Test node validation functionality"""

    def test_validate_signature(self):
        """Test function signature validation"""

        # Valid function
        def valid_func(x: int) -> int:
            return x * 2

        Node(valid_func)
        # Should not raise any exception

        # Test with explicit inputs that don't match
        with pytest.raises(ValueError, match="signature mismatch"):
            Node(valid_func, inputs=["a", "b", "c"])

    def test_async_node_detection(self):
        """Test detection of async functions"""

        async def async_func(x: int) -> int:
            return x + 1

        node = Node(async_func)
        assert node.is_async is True

        def sync_func(x: int) -> int:
            return x + 1

        node2 = Node(sync_func)
        assert node2.is_async is False


class TestNodeConnections:
    """Test node connection tracking"""

    def test_initial_connections_empty(self, simple_functions):
        """Test that new nodes have no connections"""
        node = Node(simple_functions["simple"])

        assert node.input_connections == {}
        assert node.output_connections == {}

    def test_add_input_connection(self, simple_functions):
        """Test adding input connections"""
        node1 = Node(simple_functions["simple"], name="node1")
        node2 = Node(simple_functions["simple"], name="node2")

        # Add connection
        node2.add_input_connection("x", node1, "result")

        assert "x" in node2.input_connections
        assert node2.input_connections["x"] == (node1, "result")

    def test_add_output_connection(self, simple_functions):
        """Test adding output connections"""
        node1 = Node(simple_functions["simple"], name="node1")
        node2 = Node(simple_functions["simple"], name="node2")

        # Add connection
        node1.add_output_connection("result", node2, "x")

        assert "result" in node1.output_connections
        assert (node2, "x") in node1.output_connections["result"]

    def test_multiple_output_connections(self, simple_functions):
        """Test node with multiple output connections"""
        source = Node(simple_functions["simple"], name="source")
        target1 = Node(simple_functions["simple"], name="target1")
        target2 = Node(simple_functions["simple"], name="target2")

        # Connect to multiple targets
        source.add_output_connection("result", target1, "x")
        source.add_output_connection("result", target2, "x")

        assert len(source.output_connections["result"]) == 2
        assert (target1, "x") in source.output_connections["result"]
        assert (target2, "x") in source.output_connections["result"]


class TestSpecialNodeTypes:
    """Test special node types and returns"""

    def test_conditional_node(self):
        """Test conditional node with ConditionalReturn"""

        def condition_func(value: int) -> ConditionalReturn:
            return ConditionalReturn(condition=value > 0, value=value)

        node = Node(condition_func, node_type=NodeType.CONDITIONAL)
        assert node.node_type == NodeType.CONDITIONAL

        # Test conditional outputs
        node.outputs = ["true", "false"]  # Should have these outputs

    def test_fsm_state_node(self):
        """Test FSM state node with FSMReturn"""

        def state_func(context: Any) -> FSMReturn:
            return FSMReturn(next_state="next", value={"data": "test"}, stop=False)

        node = Node(state_func, node_type=NodeType.FSM_STATE)
        assert node.node_type == NodeType.FSM_STATE

    def test_node_metadata(self, simple_functions):
        """Test storing metadata in nodes"""
        node = Node(
            simple_functions["simple"], metadata={"author": "test", "version": "1.0"}
        )

        assert node.metadata["author"] == "test"
        assert node.metadata["version"] == "1.0"


class TestNodeExecution:
    """Test node execution functionality"""

    def test_execute_simple_node(self, simple_functions):
        """Test executing a simple node"""
        node = Node(simple_functions["simple"])
        result = node.execute({"x": 5})

        assert result == 10  # 5 * 2

    def test_execute_multi_input_node(self, simple_functions):
        """Test executing node with multiple inputs"""
        node = Node(simple_functions["add"])
        result = node.execute({"a": 3, "b": 4})

        assert result == 7

    def test_execute_with_context(self):
        """Test executing node that accepts context"""

        def with_context(value: int, context: Any) -> int:
            # Access previous result from context
            previous = context.get("previous", 0)
            return value + previous

        node = Node(with_context)

        # Create mock context
        from fast_dag import Context

        ctx = Context()
        ctx.set_result("previous", 10)

        result = node.execute({"value": 5}, context=ctx)
        assert result == 15  # 5 + 10

    def test_execute_conditional_node(self):
        """Test executing conditional node"""

        def check_positive(value: int) -> ConditionalReturn:
            return ConditionalReturn(condition=value > 0, value=value)

        node = Node(check_positive, node_type=NodeType.CONDITIONAL)

        # Positive case
        result = node.execute({"value": 5})
        assert isinstance(result, ConditionalReturn)
        assert result.condition is True
        assert result.value == 5

        # Negative case
        result = node.execute({"value": -5})
        assert isinstance(result, ConditionalReturn)
        assert result.condition is False
        assert result.value == -5

    @pytest.mark.asyncio
    async def test_execute_async_node(self):
        """Test executing async node"""

        async def async_add(a: int, b: int) -> int:
            await asyncio.sleep(0.01)
            return a + b

        node = Node(async_add)
        assert node.is_async is True

        # Execute async
        result = await node.execute_async({"a": 1, "b": 2})
        assert result == 3
