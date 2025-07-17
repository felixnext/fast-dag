"""Unit tests for serialization functionality."""

import pytest

from fast_dag import DAG, FSM
from fast_dag.core.node import Node
from fast_dag.core.types import FSMReturn, NodeType
from fast_dag.serialization import FunctionRegistry, SerializerRegistry


# Test functions that will be serialized
def add(a: int, b: int) -> int:
    """Add two numbers."""
    return a + b


def multiply(x: int, y: int) -> int:
    """Multiply two numbers."""
    return x * y


def process_data(data: dict) -> list:
    """Process some data."""
    return data.get("items", [])


class TestFunctionRegistry:
    """Test function registry functionality."""

    def setup_method(self):
        """Clear registry before each test."""
        FunctionRegistry.clear()

    def test_register_and_get_function(self):
        """Test registering and retrieving functions."""
        # Register function
        FunctionRegistry.register(add, "test_add")

        # Retrieve function
        func = FunctionRegistry.get("test_add")
        assert func is add
        assert func(2, 3) == 5

    def test_auto_discovery(self):
        """Test auto-discovery of functions from module path."""
        # Should be able to find function by full path
        func = FunctionRegistry.get("tests.unit.test_serialization.multiply")
        assert func.__name__ == "multiply"
        assert func(3, 4) == 12

    def test_function_not_found(self):
        """Test error when function not found."""
        with pytest.raises(ValueError, match="Function not found"):
            FunctionRegistry.get("nonexistent.function")

    def test_register_module(self):
        """Test registering all functions from a module."""
        FunctionRegistry.register_module("tests.unit.test_serialization")

        # Should have registered test functions
        funcs = FunctionRegistry.list_functions()
        assert any("test_serialization.add" in f for f in funcs)
        assert any("test_serialization.multiply" in f for f in funcs)


class TestSerializerRegistry:
    """Test serializer registry functionality."""

    def test_default_serializer(self):
        """Test that msgspec is registered as default."""
        # Skip if msgspec not installed
        try:
            import msgspec  # noqa: F401
        except ImportError:
            pytest.skip("msgspec not installed")

        serializer = SerializerRegistry.get()
        assert serializer is not None
        assert "msgspec" in SerializerRegistry.list()


class TestDAGSerialization:
    """Test DAG serialization/deserialization."""

    def setup_method(self):
        """Register test functions before each test."""
        FunctionRegistry.clear()
        FunctionRegistry.register(add)
        FunctionRegistry.register(multiply)
        FunctionRegistry.register(process_data)

    @pytest.mark.skipif("not HAS_MSGSPEC", reason="msgspec not installed")
    def test_serialize_simple_dag(self):
        """Test serializing a simple DAG."""
        try:
            import msgspec  # noqa: F401
        except ImportError:
            pytest.skip("msgspec not installed")

        # Create DAG
        dag = DAG("test_dag")

        def start() -> int:
            return 10

        def double(x: int) -> int:
            return x * 2

        # Register the functions first
        FunctionRegistry.register(start)
        FunctionRegistry.register(double)

        # Add to DAG
        dag.add_node("start", start)
        dag.add_node("double", double)

        # Connect nodes
        dag.connect("start", "double", input="x")

        # Serialize to JSON
        serializer = SerializerRegistry.get()
        json_data = serializer.serialize(dag, format="json")

        # Check it's valid JSON
        assert isinstance(json_data, str)
        assert '"name":"test_dag"' in json_data or '"name": "test_dag"' in json_data
        assert '"nodes"' in json_data
        assert '"connections"' in json_data

    @pytest.mark.skipif("not HAS_MSGSPEC", reason="msgspec not installed")
    def test_deserialize_dag(self):
        """Test deserializing a DAG."""
        try:
            import msgspec  # noqa: F401
        except ImportError:
            pytest.skip("msgspec not installed")

        # Create and serialize DAG
        dag = DAG("pipeline")
        dag.add_node("add", add)
        dag.add_node("multiply", multiply)
        dag.connect("add", "multiply", output="result", input="x")

        serializer = SerializerRegistry.get()
        json_data = serializer.serialize(dag, format="json")

        # Deserialize
        loaded_dag = serializer.deserialize(json_data, target_type=DAG, format="json")

        # Verify
        assert loaded_dag.name == "pipeline"
        assert len(loaded_dag.nodes) == 2
        assert "add" in loaded_dag.nodes
        assert "multiply" in loaded_dag.nodes

        # Test execution
        result = loaded_dag.run(a=2, b=3, y=4)
        assert result == 20  # (2 + 3) * 4

    @pytest.mark.skipif("not HAS_MSGSPEC", reason="msgspec not installed")
    def test_save_load_dag_json(self, tmp_path):
        """Test saving and loading DAG from JSON file."""
        try:
            import msgspec  # noqa: F401
        except ImportError:
            pytest.skip("msgspec not installed")

        # Create DAG
        dag = DAG("save_test")
        dag.add_node("process", process_data)

        # Save to file
        filepath = tmp_path / "dag.json"
        dag.save(str(filepath))

        # Load from file
        loaded = DAG.load(str(filepath))

        # Verify
        assert loaded.name == "save_test"
        assert "process" in loaded.nodes

    @pytest.mark.skipif(
        "not HAS_MSGSPEC or not HAS_YAML", reason="msgspec/yaml not installed"
    )
    def test_save_load_dag_yaml(self, tmp_path):
        """Test saving and loading DAG from YAML file."""
        try:
            import msgspec  # noqa: F401
            import yaml  # noqa: F401
        except ImportError:
            pytest.skip("msgspec or yaml not installed")

        # Create DAG
        dag = DAG("yaml_test")
        dag.add_node("add", add)

        # Save to YAML
        filepath = tmp_path / "dag.yaml"
        dag.save(str(filepath), format="yaml")

        # Verify file exists and is YAML
        assert filepath.exists()
        content = filepath.read_text()
        assert "name: yaml_test" in content

        # Load from file
        loaded = DAG.load(str(filepath))
        assert loaded.name == "yaml_test"


class TestFSMSerialization:
    """Test FSM serialization/deserialization."""

    @pytest.mark.skipif("not HAS_MSGSPEC", reason="msgspec not installed")
    def test_serialize_fsm(self):
        """Test serializing an FSM."""
        try:
            import msgspec  # noqa: F401
        except ImportError:
            pytest.skip("msgspec not installed")

        # Create FSM
        fsm = FSM("test_fsm")

        def start() -> FSMReturn:
            return FSMReturn(next_state="end", value="started")

        def end() -> FSMReturn:
            return FSMReturn(stop=True, value="ended")

        # Register functions
        FunctionRegistry.register(start)
        FunctionRegistry.register(end)

        # Add states (using state method to get proper node types)
        node1 = Node(func=start, name="start", node_type=NodeType.FSM_STATE)
        node2 = Node(func=end, name="end", node_type=NodeType.FSM_STATE)
        fsm.add_node(node1)
        fsm.add_node(node2)
        fsm.initial_state = "start"
        fsm.terminal_states.add("end")

        # Serialize
        serializer = SerializerRegistry.get()
        json_data = serializer.serialize(fsm, format="json")

        # Verify
        assert '"name":"test_fsm"' in json_data or '"name": "test_fsm"' in json_data
        assert (
            '"initial_state":"start"' in json_data
            or '"initial_state": "start"' in json_data
        )
        assert (
            '"terminal_states":["end"]' in json_data
            or '"terminal_states": ["end"]' in json_data
        )

    @pytest.mark.skipif("not HAS_MSGSPEC", reason="msgspec not installed")
    def test_deserialize_fsm(self):
        """Test deserializing an FSM."""
        try:
            import msgspec  # noqa: F401
        except ImportError:
            pytest.skip("msgspec not installed")

        # Create FSM
        fsm = FSM("machine", max_cycles=5)

        def state_a() -> FSMReturn:
            return FSMReturn(next_state="b", value="A")

        def state_b() -> FSMReturn:
            return FSMReturn(next_state="a", value="B")

        # Register and add states
        FunctionRegistry.register(state_a)
        FunctionRegistry.register(state_b)

        fsm.add_node("a", state_a)
        fsm.add_node("b", state_b)
        fsm.initial_state = "a"

        # Serialize and deserialize
        serializer = SerializerRegistry.get()
        data = serializer.serialize(fsm, format="json")
        loaded = serializer.deserialize(data, target_type=FSM, format="json")

        # Verify
        assert loaded.name == "machine"
        assert loaded.initial_state == "a"
        assert loaded.max_cycles == 5
        assert len(loaded.nodes) == 2

    @pytest.mark.skipif("not HAS_MSGSPEC", reason="msgspec not installed")
    def test_fsm_with_transitions(self, tmp_path):
        """Test FSM with explicit state transitions."""
        try:
            import msgspec  # noqa: F401
        except ImportError:
            pytest.skip("msgspec not installed")

        # Create FSM with transitions
        fsm = FSM("traffic")

        def red() -> FSMReturn:
            return FSMReturn(value="STOP")

        def green() -> FSMReturn:
            return FSMReturn(value="GO")

        FunctionRegistry.register(red)
        FunctionRegistry.register(green)

        fsm.add_node("red", red)
        fsm.add_node("green", green)
        fsm.initial_state = "red"
        fsm.add_transition("red", "green", "timer")
        fsm.add_transition("green", "red", "timer")

        # Save and load
        filepath = tmp_path / "traffic.json"
        fsm.save(str(filepath))
        loaded = FSM.load(str(filepath))

        # Verify transitions
        assert loaded.state_transitions["red"]["timer"] == "green"
        assert loaded.state_transitions["green"]["timer"] == "red"


# Module-level check for msgspec
try:
    import msgspec

    del msgspec  # Remove unused import warning

    HAS_MSGSPEC = True
except ImportError:
    HAS_MSGSPEC = False

try:
    import yaml

    del yaml  # Remove unused import warning

    HAS_YAML = True
except ImportError:
    HAS_YAML = False
