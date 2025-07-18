"""
Unit tests for DAG class and basic workflow functionality.
"""

import pytest

from fast_dag import DAG, ConditionalReturn, Node, ValidationError


class TestDAGCreation:
    """Test DAG creation and initialization"""

    def test_dag_creation(self):
        """Test creating a basic DAG"""
        dag = DAG("test_workflow")

        assert dag.name == "test_workflow"
        assert dag.nodes == {}
        assert dag.description is None
        assert dag.metadata == {}

    def test_dag_with_description(self):
        """Test creating DAG with description"""
        dag = DAG("workflow", description="Test workflow")

        assert dag.description == "Test workflow"

    def test_dag_with_metadata(self):
        """Test creating DAG with metadata"""
        dag = DAG("workflow", metadata={"version": "1.0", "author": "test"})

        assert dag.metadata["version"] == "1.0"
        assert dag.metadata["author"] == "test"


class TestNodeAddition:
    """Test adding nodes to DAG"""

    def test_add_node_decorator(self):
        """Test adding node with decorator"""
        dag = DAG("test")

        @dag.node
        def process(x: int) -> int:
            return x * 2

        assert "process" in dag.nodes
        assert dag.nodes["process"].name == "process"
        assert dag.nodes["process"].func.__name__ == "process"

    def test_add_node_decorator_with_params(self):
        """Test decorator with parameters"""
        dag = DAG("test")

        @dag.node(name="custom", inputs=["data"], outputs=["result"])
        def process(data: dict) -> dict:
            return {"processed": True}

        assert "custom" in dag.nodes
        assert dag.nodes["custom"].inputs == ["data"]
        assert dag.nodes["custom"].outputs == ["result"]

    def test_add_node_manual(self, simple_functions):
        """Test adding node manually"""
        dag = DAG("test")
        node = Node(simple_functions["simple"])

        dag.add_node(node)

        assert "simple_function" in dag.nodes
        assert dag.nodes["simple_function"] == node

    def test_add_conditional_node(self):
        """Test adding conditional node"""
        dag = DAG("test")

        @dag.condition()
        def check(value: int) -> ConditionalReturn:
            return ConditionalReturn(condition=value > 0, value=value)

        assert "check" in dag.nodes
        assert dag.nodes["check"].node_type.value == "conditional"
        assert dag.nodes["check"].outputs == ["true", "false"]

    def test_duplicate_node_name_error(self):
        """Test error when adding duplicate node names"""
        dag = DAG("test")

        @dag.node
        def process(x: int) -> int:
            return x

        # Try to add another node with same name
        with pytest.raises(ValueError, match="already exists"):

            @dag.node(name="process")
            def another(x: int) -> int:
                return x + 1

    def test_builder_pattern(self):
        """Test builder pattern for adding nodes"""
        dag = DAG("test")

        def func1(x: int) -> int:
            return x + 1

        def func2(x: int) -> int:
            return x * 2

        # Chain additions
        dag.add_node("step1", func1).add_node("step2", func2)

        assert "step1" in dag.nodes
        assert "step2" in dag.nodes


class TestDAGValidation:
    """Test DAG validation functionality"""

    def test_validate_simple_dag(self):
        """Test validating a simple valid DAG"""
        dag = DAG("test")

        @dag.node
        def start(x: int) -> int:
            return x

        @dag.node
        def end(y: int) -> int:
            return y * 2

        dag.connect("start", "end")

        errors = dag.validate()
        assert errors == []

    def test_validate_cycle_detection(self):
        """Test detecting cycles in DAG"""
        dag = DAG("test")

        @dag.node
        def a(x: int) -> int:
            return x

        @dag.node
        def b(x: int) -> int:
            return x

        @dag.node
        def c(x: int) -> int:
            return x

        # Create cycle: a -> b -> c -> a
        dag.connect("a", "b")
        dag.connect("b", "c")
        dag.connect("c", "a")

        errors = dag.validate()
        assert len(errors) > 0
        assert any("cycle" in str(e).lower() for e in errors)

        # is_acyclic should return False
        assert dag.is_acyclic() is False

    def test_validate_disconnected_nodes(self):
        """Test detecting disconnected nodes"""
        dag = DAG("test")

        @dag.node
        def connected1(x: int) -> int:
            return x

        @dag.node
        def connected2(x: int) -> int:
            return x

        @dag.node
        def orphan(x: int) -> int:
            return x

        dag.connect("connected1", "connected2")
        # orphan is not connected

        errors = dag.validate()
        assert any(
            "disconnected" in str(e).lower() or "orphan" in str(e).lower()
            for e in errors
        )

        assert dag.is_fully_connected() is False

    def test_validate_missing_conditional_branch(self):
        """Test detecting missing conditional branches"""
        dag = DAG("test")

        @dag.node
        def start(x: int) -> int:
            return x

        @dag.condition()
        def check(x: int) -> ConditionalReturn:
            return ConditionalReturn(condition=x > 0, value=x)

        @dag.node
        def positive_path(x: int) -> int:
            return x

        # Only connect true branch
        dag.connect("start", "check")
        dag.connect("check", "positive_path", output="true")
        # Missing false branch connection

        errors = dag.validate()
        assert any("false" in str(e).lower() for e in errors)

    def test_validate_or_raise(self):
        """Test validate_or_raise method"""
        dag = DAG("test")

        @dag.node
        def a(x: int) -> int:
            return x

        @dag.node
        def b(x: int) -> int:
            return x

        # Create cycle
        dag.connect("a", "b")
        dag.connect("b", "a")

        with pytest.raises(ValidationError):
            dag.validate_or_raise()

    def test_has_entry_points(self):
        """Test checking for entry points"""
        dag = DAG("test")

        @dag.node
        def entry(x: int) -> int:
            return x

        @dag.node
        def middle(x: int) -> int:
            return x

        dag.connect("entry", "middle")

        assert dag.has_entry_points() is True

        # Connect middle back to entry - no entry points
        dag.connect("middle", "entry")
        assert dag.has_entry_points() is False


class TestDAGConnections:
    """Test node connection functionality"""

    def test_simple_connection(self):
        """Test simple node connection"""
        dag = DAG("test")

        @dag.node
        def a(x: int) -> int:
            return x

        @dag.node
        def b(x: int) -> int:
            return x * 2

        dag.connect("a", "b")

        # Check connections were made
        assert "b" in [
            node.name for node, _ in dag.nodes["a"].output_connections.get("result", [])
        ]
        assert (
            dag.nodes["b"].input_connections.get("x", (None, None))[0] == dag.nodes["a"]
        )

    def test_connection_with_explicit_ports(self):
        """Test connection with explicit input/output names"""
        dag = DAG("test")

        @dag.node(outputs=["value", "status"])
        def producer() -> tuple[int, str]:
            return 42, "ok"

        @dag.node
        def consumer(data: int) -> int:
            return data + 1

        dag.connect("producer", "consumer", output="value", input="data")

        # Verify specific connection
        node_a = dag.nodes["producer"]
        node_b = dag.nodes["consumer"]
        assert (node_b, "data") in node_a.output_connections["value"]

    def test_can_connect_validation(self):
        """Test connection validation"""
        dag = DAG("test")

        @dag.node
        def int_producer() -> int:
            return 42

        @dag.node
        def str_consumer(text: str) -> str:
            return text.upper()

        # Should detect type mismatch
        assert dag.can_connect("int_producer", "str_consumer") is False

        @dag.node
        def int_consumer(value: int) -> int:
            return value * 2

        # Should allow compatible types
        assert dag.can_connect("int_producer", "int_consumer") is True

    def test_check_connection_issues(self):
        """Test getting connection issues"""
        dag = DAG("test")

        @dag.node
        def source() -> int:
            return 1

        @dag.node
        def target(text: str) -> str:
            return text

        # First connect the nodes
        dag.connect("source", "target")

        # Then check for connection issues
        dag.check_connection_issues()
        # With type checking in validation, we should get type mismatch issues
        errors = dag.validate(check_types=True)
        assert len(errors) > 0
        assert any("type" in str(error).lower() for error in errors)


class TestDAGProperties:
    """Test DAG computed properties"""

    def test_entry_points_detection(self):
        """Test automatic entry point detection"""
        dag = DAG("test")

        @dag.node
        def entry1() -> int:
            return 1

        @dag.node
        def entry2() -> int:
            return 2

        @dag.node
        def middle(x: int, y: int) -> int:
            return x + y

        dag.connect("entry1", "middle", input="x")
        dag.connect("entry2", "middle", input="y")

        entry_points = dag.entry_points
        assert len(entry_points) == 2
        assert "entry1" in entry_points
        assert "entry2" in entry_points

    def test_execution_order(self):
        """Test topological sort for execution order"""
        dag = DAG("test")

        @dag.node
        def a() -> int:
            return 1

        @dag.node
        def b(x: int) -> int:
            return x + 1

        @dag.node
        def c(x: int) -> int:
            return x + 2

        @dag.node
        def d(x: int, y: int) -> int:
            return x + y

        # a -> b -> d
        # a -> c -> d
        dag.connect("a", "b")
        dag.connect("a", "c")
        dag.connect("b", "d", input="x")
        dag.connect("c", "d", input="y")

        order = dag.execution_order

        # a must come before b and c
        assert order.index("a") < order.index("b")
        assert order.index("a") < order.index("c")

        # b and c must come before d
        assert order.index("b") < order.index("d")
        assert order.index("c") < order.index("d")

    def test_clear_cached_properties(self):
        """Test that cached properties update when DAG changes"""
        dag = DAG("test")

        @dag.node
        def a() -> int:
            return 1

        # Get initial entry points
        entries1 = dag.entry_points
        assert entries1 == ["a"]

        # Add another node
        @dag.node
        def b() -> int:
            return 2

        # Entry points should update
        entries2 = dag.entry_points
        assert sorted(entries2) == ["a", "b"]
