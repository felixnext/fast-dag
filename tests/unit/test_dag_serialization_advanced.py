"""Advanced unit tests for DAG serialization edge cases."""

import json

import pytest

from fast_dag import DAG
from fast_dag.core.exceptions import CycleError
from fast_dag.core.node import Node
from fast_dag.core.types import ConditionalReturn, NodeType
from fast_dag.serialization import FunctionRegistry


class TestDAGSerializationAdvanced:
    """Test advanced DAG serialization scenarios."""

    def setup_method(self):
        """Clear registry before each test."""
        FunctionRegistry.clear()

    def test_save_load_with_nonexistent_file(self, tmp_path):
        """Test loading from non-existent file."""
        filepath = tmp_path / "nonexistent.json"

        with pytest.raises(FileNotFoundError):
            DAG.load(str(filepath))

    def test_save_with_invalid_format(self, tmp_path):
        """Test saving with invalid format."""
        dag = DAG("test")

        @dag.node
        def process() -> int:
            return 1

        filepath = tmp_path / "test.invalid"

        with pytest.raises(ValueError, match="Unknown format"):
            dag.save(str(filepath), format="invalid")

    def test_save_permission_error(self, tmp_path):
        """Test save with permission error."""
        dag = DAG("test")

        @dag.node
        def func() -> int:
            return 1

        # Create read-only directory
        readonly_dir = tmp_path / "readonly"
        readonly_dir.mkdir()
        readonly_dir.chmod(0o444)

        filepath = readonly_dir / "dag.json"

        # Skip on Windows where permissions work differently
        import platform

        if platform.system() != "Windows":
            with pytest.raises(PermissionError):
                dag.save(str(filepath))

        # Cleanup
        readonly_dir.chmod(0o755)

    def test_load_corrupted_json(self, tmp_path):
        """Test loading corrupted JSON file."""
        filepath = tmp_path / "corrupted.json"
        filepath.write_text("{ invalid json }")

        # msgspec raises DecodeError instead of JSONDecodeError
        import msgspec

        with pytest.raises((ValueError, json.JSONDecodeError, msgspec.DecodeError)):
            DAG.load(str(filepath))

    def test_serialize_with_lambda_functions(self, tmp_path):
        """Test serialization fails gracefully with lambda functions."""
        dag = DAG("lambda_dag")

        # Lambda functions can't be serialized
        dag.add_node("lambda", lambda x: x + 1)

        # Should fail when trying to serialize
        filepath = tmp_path / "lambda.json"
        with pytest.raises(ValueError, match="Lambda functions cannot be serialized"):
            dag.save(str(filepath))

    def test_serialize_complex_metadata(self, tmp_path):
        """Test serialization with complex metadata."""
        dag = DAG(
            "metadata_test",
            metadata={
                "version": "1.0.0",
                "author": "test",
                "tags": ["ml", "pipeline"],
                "config": {"batch_size": 32, "learning_rate": 0.001},
            },
        )

        def process(data: dict) -> dict:
            return data

        FunctionRegistry.register(process)
        dag.add_node("process", process)

        # Save and load
        filepath = tmp_path / "metadata.json"
        dag.save(str(filepath))
        loaded = DAG.load(str(filepath))

        # Verify metadata preserved
        assert loaded.metadata["version"] == "1.0.0"
        assert loaded.metadata["tags"] == ["ml", "pipeline"]
        assert loaded.metadata["config"]["batch_size"] == 32

    def test_serialize_node_with_all_fields(self, tmp_path):
        """Test serialization of node with all optional fields."""
        dag = DAG("full_node")

        def complex_func(x: int, y: int = 10) -> dict:
            """Complex function with defaults."""
            return {"sum": x + y}

        FunctionRegistry.register(complex_func)

        node = Node(
            func=complex_func,
            name="complex",
            inputs=["x", "y"],
            outputs=["result"],
            description="Complex node",
            node_type=NodeType.STANDARD,
            retry=3,
            retry_delay=2.0,
            timeout=30.0,
            metadata={"priority": "high", "team": "ml"},
        )

        dag.add_node(node)

        # Save and load
        filepath = tmp_path / "full_node.json"
        dag.save(str(filepath))
        loaded = DAG.load(str(filepath))

        # Verify all fields preserved
        loaded_node = loaded.nodes["complex"]
        assert loaded_node.name == "complex"
        assert loaded_node.description == "Complex node"
        assert loaded_node.retry == 3
        assert loaded_node.retry_delay == 2.0
        assert loaded_node.timeout == 30.0
        assert loaded_node.metadata["priority"] == "high"

    def test_serialize_conditional_workflow(self, tmp_path):
        """Test serialization of conditional workflow."""
        dag = DAG("conditional")

        def check(x: int) -> ConditionalReturn:
            return ConditionalReturn(condition=x > 0, value=x)

        def positive(x: int) -> str:
            return "pos"

        def negative(x: int) -> str:
            return "neg"

        # Register all functions
        FunctionRegistry.register(check)
        FunctionRegistry.register(positive)
        FunctionRegistry.register(negative)

        # Build workflow
        dag.add_node(
            "check", check, outputs=["true", "false"], node_type=NodeType.CONDITIONAL
        )
        dag.add_node("positive", positive)
        dag.add_node("negative", negative)

        dag.connect("check", "positive", output="true", input="x")
        dag.connect("check", "negative", output="false", input="x")

        # Save and load
        filepath = tmp_path / "conditional.json"
        dag.save(str(filepath))
        loaded = DAG.load(str(filepath))

        # Verify structure
        assert len(loaded.nodes) == 3
        check_node = loaded.nodes["check"]
        assert check_node.outputs == ["true", "false"]

        # Test execution
        result = loaded.run(x=5)
        assert result == "pos"

    def test_serialize_with_special_characters(self, tmp_path):
        """Test serialization with special characters in names."""
        dag = DAG("special-chars_test!")

        def func() -> str:
            return "test"

        FunctionRegistry.register(func, "special/func")
        dag.add_node("node-with-dash", func)

        filepath = tmp_path / "special.json"
        dag.save(str(filepath))

        # Verify JSON structure
        content = json.loads(filepath.read_text())
        assert content["name"] == "special-chars_test!"
        assert any(n["name"] == "node-with-dash" for n in content["nodes"])

    def test_serialize_empty_dag(self, tmp_path):
        """Test serialization of empty DAG."""
        dag = DAG("empty")

        filepath = tmp_path / "empty.json"
        dag.save(str(filepath))
        loaded = DAG.load(str(filepath))

        assert loaded.name == "empty"
        assert len(loaded.nodes) == 0

    def test_serialize_disconnected_nodes(self, tmp_path):
        """Test serialization of DAG with disconnected nodes."""
        dag = DAG("disconnected")

        def node1() -> int:
            return 1

        def node2() -> int:
            return 2

        FunctionRegistry.register(node1)
        FunctionRegistry.register(node2)

        dag.add_node("node1", node1)
        dag.add_node("node2", node2)

        filepath = tmp_path / "disconnected.json"
        dag.save(str(filepath))
        loaded = DAG.load(str(filepath))

        assert len(loaded.nodes) == 2
        assert "node1" in loaded.nodes
        assert "node2" in loaded.nodes

    def test_serialize_circular_reference_prevention(self):
        """Test that circular references are prevented."""
        dag = DAG("circular")

        def a(y: int = 0) -> int:
            return y + 1

        def b(x: int) -> int:
            return x + 1

        FunctionRegistry.register(a)
        FunctionRegistry.register(b)

        dag.add_node("a", a)
        dag.add_node("b", b)

        # This should work
        dag.connect("a", "b", input="x")

        # This should fail (would create cycle)
        with pytest.raises(CycleError):
            dag.connect("b", "a")
            # Force validation to check for cycles
            dag.validate_or_raise()

    def test_load_with_missing_function(self, tmp_path):
        """Test loading DAG when function is not in registry."""
        dag = DAG("missing_func")

        def temp_func() -> int:
            return 1

        FunctionRegistry.register(temp_func, "temp.func")
        dag.add_node("node", temp_func)

        filepath = tmp_path / "missing.json"
        dag.save(str(filepath))

        # Clear registry to simulate missing function
        FunctionRegistry.clear()

        with pytest.raises(ValueError, match="Function not found"):
            DAG.load(str(filepath))

    def test_serialize_with_custom_node_types(self, tmp_path):
        """Test serialization with various node types."""
        dag = DAG("node_types")

        def standard() -> int:
            return 1

        def conditional(x: int) -> ConditionalReturn:
            return ConditionalReturn(condition=x > 0, value=x)

        # Register functions
        FunctionRegistry.register(standard)
        FunctionRegistry.register(conditional)

        # Add nodes with different types
        dag.add_node(Node(func=standard, name="std", node_type=NodeType.STANDARD))

        dag.add_node(
            Node(func=conditional, name="cond", node_type=NodeType.CONDITIONAL)
        )

        filepath = tmp_path / "types.json"
        dag.save(str(filepath))
        loaded = DAG.load(str(filepath))

        assert loaded.nodes["std"].node_type == NodeType.STANDARD
        assert loaded.nodes["cond"].node_type == NodeType.CONDITIONAL

    def test_msgpack_serialization(self, tmp_path):
        """Test msgpack format serialization."""
        pytest.importorskip("msgspec")

        dag = DAG("msgpack_test")

        def process(data: bytes) -> bytes:
            return data + b"_processed"

        FunctionRegistry.register(process)
        dag.add_node("process", process)

        filepath = tmp_path / "test.msgpack"
        dag.save(str(filepath), format="msgpack")

        # Verify it's binary format
        content = filepath.read_bytes()
        assert isinstance(content, bytes)

        # Load and verify
        loaded = DAG.load(str(filepath), format="msgpack")
        assert loaded.name == "msgpack_test"

    @pytest.mark.skipif("not HAS_YAML", reason="yaml not installed")
    def test_yaml_serialization_multiline(self, tmp_path):
        """Test YAML serialization with multiline strings."""
        pytest.importorskip("yaml")

        dag = DAG(
            "yaml_multi",
            description="""This is a
multi-line
description""",
        )

        def process() -> str:
            """This function has
            a multiline docstring
            for testing."""
            return "done"

        FunctionRegistry.register(process)
        dag.add_node("process", process)

        filepath = tmp_path / "multi.yaml"
        dag.save(str(filepath), format="yaml")

        content = filepath.read_text()
        # Just check that multiline content is preserved in some form
        assert "multi-line" in content

        loaded = DAG.load(str(filepath))
        assert "multi-line" in loaded.description

    def test_concurrent_save_load(self, tmp_path):
        """Test concurrent save/load operations."""
        import threading

        dag = DAG("concurrent")

        def func() -> int:
            return 1

        FunctionRegistry.register(func)
        dag.add_node("func", func)

        filepath = tmp_path / "concurrent.json"
        errors = []

        def save_dag():
            try:
                dag.save(str(filepath))
            except Exception as e:
                errors.append(e)

        def load_dag():
            try:
                if filepath.exists():
                    DAG.load(str(filepath))
            except Exception as e:
                errors.append(e)

        # Run multiple save/load operations concurrently
        threads = []
        for _i in range(5):
            t1 = threading.Thread(target=save_dag)
            t2 = threading.Thread(target=load_dag)
            threads.extend([t1, t2])
            t1.start()
            t2.start()

        for t in threads:
            t.join()

        # Should handle concurrent access gracefully
        # Allow FileNotFoundError and decode errors from concurrent access
        assert len(errors) == 0 or all(
            isinstance(e, FileNotFoundError) or "truncated" in str(e).lower()
            for e in errors
        )


# Module-level check
try:
    import yaml  # noqa: F401

    HAS_YAML = True
except ImportError:
    HAS_YAML = False
