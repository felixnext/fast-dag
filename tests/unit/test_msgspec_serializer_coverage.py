"""
Tests for msgspec serializer to improve coverage.
"""

from unittest.mock import MagicMock, patch

import pytest

from fast_dag import DAG, FSM, Context, FSMContext, FSMReturn
from fast_dag.serialization.msgspec_serializer import MsgspecSerializer
from fast_dag.serialization.registry import FunctionRegistry


# Test helper functions
def sample_func(x: int) -> int:
    return x + 1


def sample_fsm_func(context: FSMContext) -> FSMReturn:
    return FSMReturn(stop=True, value="done")


# Register functions
FunctionRegistry.register(sample_func, "test_msgspec_serializer_coverage.sample_func")
FunctionRegistry.register(
    sample_fsm_func, "test_msgspec_serializer_coverage.sample_fsm_func"
)


class TestMsgspecSerializerCoverage:
    """Test msgspec serializer comprehensively"""

    def test_init_without_msgspec(self):
        """Test initialization when msgspec is not available (lines 28-29, 37)"""
        # Mock msgspec as not available
        with (
            patch("fast_dag.serialization.msgspec_serializer.HAS_MSGSPEC", False),
            pytest.raises(ImportError, match="msgspec is required for serialization"),
        ):
            MsgspecSerializer()

    def test_serialize_unknown_object_type(self):
        """Test serializing unknown object type (lines 51-54)"""
        serializer = MsgspecSerializer()

        # Try to serialize an unsupported object
        with pytest.raises(TypeError, match="Cannot serialize type"):
            serializer.serialize("not a DAG or FSM or Context")

    def test_serialize_context(self):
        """Test serializing Context (line 52)"""
        serializer = MsgspecSerializer()

        context = Context()
        context.results = {"test": "value"}
        context.metadata = {"version": "1.0"}
        context.metrics = {"time": 123}

        result = serializer.serialize(context, format="json")
        assert isinstance(result, str)
        assert "test" in result
        assert "value" in result

    def test_serialize_unknown_format(self):
        """Test serializing with unknown format"""
        serializer = MsgspecSerializer()
        dag = DAG("test")

        with pytest.raises(ValueError, match="Unknown format"):
            serializer.serialize(dag, format="unknown")

    def test_deserialize_msgpack_from_string(self):
        """Test deserializing msgpack from string (lines 82)"""
        serializer = MsgspecSerializer()

        # Create a DAG and serialize it to msgpack
        dag = DAG("test")
        dag.add_node("func", sample_func)

        msgpack_bytes = serializer.serialize(dag, format="msgpack")
        assert isinstance(msgpack_bytes, bytes)

        # Convert bytes to string and deserialize
        msgpack_str = msgpack_bytes.decode("latin-1")  # Use latin-1 to preserve bytes

        with patch(
            "fast_dag.serialization.msgspec_serializer.msgspec.msgpack.Decoder"
        ) as mock_decoder:
            mock_decoder_instance = MagicMock()
            mock_decoder.return_value = mock_decoder_instance
            mock_decoder_instance.decode.return_value = {
                "name": "test",
                "nodes": [],
                "connections": [],
            }

            serializer.deserialize(msgpack_str, DAG, format="msgpack")

            # Should convert string to bytes
            mock_decoder_instance.decode.assert_called_once()

    def test_deserialize_yaml_from_bytes(self):
        """Test deserializing YAML from bytes (lines 87)"""
        serializer = MsgspecSerializer()

        yaml_bytes = b"name: test\nnodes: []\nconnections: []"

        with patch(
            "fast_dag.serialization.msgspec_serializer.yaml.safe_load"
        ) as mock_load:
            mock_load.return_value = {"name": "test", "nodes": [], "connections": []}

            serializer.deserialize(yaml_bytes, DAG, format="yaml")

            # Should convert bytes to string
            mock_load.assert_called_once_with("name: test\nnodes: []\nconnections: []")

    def test_deserialize_unknown_format(self):
        """Test deserializing with unknown format (line 90)"""
        serializer = MsgspecSerializer()

        with pytest.raises(ValueError, match="Unknown format"):
            serializer.deserialize("data", DAG, format="unknown")

    def test_deserialize_context(self):
        """Test deserializing Context (lines 99-101)"""
        serializer = MsgspecSerializer()

        # Create and serialize a context first
        context = Context()
        context.results = {"test": "value"}
        context.metadata = {"version": "1.0"}
        context.metrics = {"time": 123}

        # Serialize the context
        serialized = serializer.serialize(context, format="json")

        # Deserialize it back
        result = serializer.deserialize(serialized, Context, format="json")

        assert isinstance(result, Context)
        assert result.results == context.results
        assert result.metadata == context.metadata
        assert result.metrics == context.metrics

    def test_deserialize_fsm_context(self):
        """Test deserializing FSMContext (lines 102-104)"""
        serializer = MsgspecSerializer()

        # Create and serialize an FSM context first
        fsm_context = FSMContext()
        fsm_context.results = {"test": "value"}
        fsm_context.metadata = {"version": "1.0"}
        fsm_context.metrics = {"time": 123}
        fsm_context.state_history = ["state1", "state2"]
        fsm_context.cycle_count = 2

        # Serialize the FSM context
        serialized = serializer._context_to_serializable_fsm(fsm_context)

        # Deserialize it back
        result = serializer._serializable_to_fsm_context(serialized)

        assert isinstance(result, FSMContext)
        assert result.results == fsm_context.results
        assert result.state_history == fsm_context.state_history
        assert result.cycle_count == fsm_context.cycle_count

    def test_deserialize_unknown_target_type(self):
        """Test deserializing to unknown target type (lines 105-106)"""
        serializer = MsgspecSerializer()

        with pytest.raises(TypeError, match="Cannot deserialize to type"):
            serializer.deserialize("{}", str, format="json")

    # This test is commented out due to complexity with node name auto-assignment
    # def test_dag_serialization_with_connection_without_target_name(self):
    #     """Test DAG serialization when target node has no name (line 239)"""
    #     # This scenario is difficult to create due to automatic name assignment

    def test_context_to_serializable(self):
        """Test _context_to_serializable method (line 250)"""
        serializer = MsgspecSerializer()

        context = Context()
        context.results = {"test": "value"}
        context.metadata = {"version": "1.0"}
        context.metrics = {"time": 123}

        result = serializer._context_to_serializable(context)

        assert result.results == context.results
        assert result.metadata == context.metadata
        assert result.metrics == context.metrics

    def test_serializable_to_context(self):
        """Test _serializable_to_context method (lines 258-262)"""
        serializer = MsgspecSerializer()

        from fast_dag.serialization.types import SerializableContext

        serializable = SerializableContext(
            results={"test": "value"},
            metadata={"version": "1.0"},
            metrics={"time": 123},
        )

        result = serializer._serializable_to_context(serializable)

        assert isinstance(result, Context)
        assert result.results == serializable.results
        assert result.metadata == serializable.metadata
        assert result.metrics == serializable.metrics

    def test_context_to_serializable_fsm(self):
        """Test _context_to_serializable_fsm method (line 268)"""
        serializer = MsgspecSerializer()

        fsm_context = FSMContext()
        fsm_context.results = {"test": "value"}
        fsm_context.metadata = {"version": "1.0"}
        fsm_context.metrics = {"time": 123}
        fsm_context.state_history = ["state1", "state2"]
        fsm_context.cycle_count = 2

        # Mock cycle_results with _by_state attribute
        mock_cycle_results = MagicMock()
        mock_cycle_results._by_state = {"state1": ["result1"]}
        fsm_context.cycle_results = mock_cycle_results

        result = serializer._context_to_serializable_fsm(fsm_context)

        assert result.results == fsm_context.results
        assert result.metadata == fsm_context.metadata
        assert result.metrics == fsm_context.metrics
        assert result.state_history == fsm_context.state_history
        assert result.cycle_count == fsm_context.cycle_count
        assert result.cycle_results == mock_cycle_results._by_state

    def test_context_to_serializable_fsm_no_by_state(self):
        """Test _context_to_serializable_fsm when cycle_results has no _by_state"""
        serializer = MsgspecSerializer()

        fsm_context = FSMContext()
        fsm_context.results = {"test": "value"}
        fsm_context.metadata = {"version": "1.0"}
        fsm_context.metrics = {"time": 123}
        fsm_context.state_history = ["state1", "state2"]
        fsm_context.cycle_count = 2

        # Mock cycle_results without _by_state attribute
        mock_cycle_results = MagicMock()
        del mock_cycle_results._by_state  # Remove the attribute
        fsm_context.cycle_results = mock_cycle_results

        result = serializer._context_to_serializable_fsm(fsm_context)

        assert result.cycle_results == {}

    def test_serializable_to_fsm_context(self):
        """Test _serializable_to_fsm_context method (lines 283-298)"""
        serializer = MsgspecSerializer()

        from fast_dag.serialization.types import SerializableFSMContext

        serializable = SerializableFSMContext(
            results={"test": "value"},
            metadata={"version": "1.0"},
            metrics={"time": 123},
            state_history=["state1", "state2"],
            cycle_results={"state1": ["result1"]},
            cycle_count=2,
        )

        result = serializer._serializable_to_fsm_context(serializable)

        assert isinstance(result, FSMContext)
        assert result.results == serializable.results
        assert result.metadata == serializable.metadata
        assert result.metrics == serializable.metrics
        assert result.state_history == serializable.state_history
        assert result.cycle_count == serializable.cycle_count

        # Check cycle_results was converted properly
        assert hasattr(result.cycle_results, "_by_state")
        assert result.cycle_results._by_state == serializable.cycle_results

    def test_serializable_to_fsm_context_non_dict_cycle_results(self):
        """Test _serializable_to_fsm_context with non-dict cycle_results"""
        serializer = MsgspecSerializer()

        from fast_dag.core.context import CycleResults
        from fast_dag.serialization.types import SerializableFSMContext

        # Create a CycleResults object
        cycle_results = CycleResults()
        cycle_results._by_state = {"state1": ["result1"]}

        # Create a simple string to represent non-dict cycle_results
        serializable = SerializableFSMContext(
            results={"test": "value"},
            metadata={"version": "1.0"},
            metrics={"time": 123},
            state_history=["state1", "state2"],
            cycle_results="not_a_dict",  # Not a dict
            cycle_count=2,
        )

        result = serializer._serializable_to_fsm_context(serializable)

        assert isinstance(result, FSMContext)
        assert result.cycle_results == "not_a_dict"

    def test_serialize_yaml_format(self):
        """Test YAML serialization"""
        serializer = MsgspecSerializer()

        dag = DAG("yaml_test")
        dag.add_node("func", sample_func)

        result = serializer.serialize(dag, format="yaml")

        assert isinstance(result, str)
        assert "yaml_test" in result
        assert "name:" in result

    def test_serialize_msgpack_format(self):
        """Test msgpack serialization"""
        serializer = MsgspecSerializer()

        dag = DAG("msgpack_test")
        dag.add_node("func", sample_func)

        result = serializer.serialize(dag, format="msgpack")

        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_deserialize_fsm(self):
        """Test FSM deserialization"""
        serializer = MsgspecSerializer()

        # Create FSM and serialize it
        fsm = FSM("test_fsm")
        fsm.add_node("func", sample_fsm_func)

        serialized = serializer.serialize(fsm, format="json")

        # Deserialize it back
        deserialized = serializer.deserialize(serialized, FSM, format="json")

        assert isinstance(deserialized, FSM)
        assert deserialized.name == "test_fsm"
        assert len(deserialized.nodes) == 1
