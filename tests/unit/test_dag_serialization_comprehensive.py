"""
Comprehensive tests for DAG serialization to improve coverage.
"""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from fast_dag import DAG
from fast_dag.core.node import Node
from fast_dag.core.types import NodeType
from fast_dag.serialization.registry import FunctionRegistry


# Module-level functions that can be properly serialized (not test functions)
def sample_process(x: int) -> int:
    return x + 1


def sample_task() -> str:
    return "hello"


def sample_step1() -> int:
    return 10


def sample_step2(x: int) -> int:
    return x * 2


def sample_yaml_process(data: dict) -> dict:
    return {"result": data.get("value", 0) * 2}


def sample_compute() -> float:
    return 2.718


def sample_unknown_process() -> str:
    return "done"


# Register all functions
FunctionRegistry.register(
    sample_process, "test_dag_serialization_comprehensive.sample_process"
)
FunctionRegistry.register(
    sample_task, "test_dag_serialization_comprehensive.sample_task"
)
FunctionRegistry.register(
    sample_step1, "test_dag_serialization_comprehensive.sample_step1"
)
FunctionRegistry.register(
    sample_step2, "test_dag_serialization_comprehensive.sample_step2"
)
FunctionRegistry.register(
    sample_yaml_process, "test_dag_serialization_comprehensive.sample_yaml_process"
)
FunctionRegistry.register(
    sample_compute, "test_dag_serialization_comprehensive.sample_compute"
)
FunctionRegistry.register(
    sample_unknown_process,
    "test_dag_serialization_comprehensive.sample_unknown_process",
)


class TestDAGSerializationMethods:
    """Test DAG serialization methods"""

    def test_save_with_auto_format_detection(self):
        """Test save with automatic format detection from file extension"""
        dag = DAG("test_dag", description="Test DAG")

        @dag.node
        def process(x: int) -> int:
            return x * 2

        with tempfile.TemporaryDirectory() as tmpdir:
            # Test JSON format
            json_path = Path(tmpdir) / "test.json"
            dag.save(str(json_path))
            assert json_path.exists()
            content = json_path.read_text()
            assert "test_dag" in content

            # Test YAML format
            yaml_path = Path(tmpdir) / "test.yaml"
            dag.save(str(yaml_path))
            assert yaml_path.exists()
            content = yaml_path.read_text()
            assert "test_dag" in content

            # Test YML extension
            yml_path = Path(tmpdir) / "test.yml"
            dag.save(str(yml_path))
            assert yml_path.exists()

            # Test msgpack format
            msgpack_path = Path(tmpdir) / "test.msgpack"
            dag.save(str(msgpack_path))
            assert msgpack_path.exists()
            # msgpack is binary
            assert msgpack_path.read_bytes()

            # Test unknown extension defaults to JSON
            unknown_path = Path(tmpdir) / "test.unknown"
            dag.save(str(unknown_path))
            assert unknown_path.exists()
            content = unknown_path.read_text()
            assert "test_dag" in content

    def test_save_with_explicit_format(self):
        """Test save with explicit format specification"""
        dag = DAG("test_dag")

        @dag.node
        def task() -> int:
            return 1

        with tempfile.TemporaryDirectory() as tmpdir:
            # Save as YAML with .json extension
            path = Path(tmpdir) / "test.json"
            dag.save(str(path), format="yaml")
            content = path.read_text()
            # Should be YAML format despite .json extension
            assert "name: test_dag" in content or "name:\n  test_dag" in content

    def test_save_binary_format(self):
        """Test saving in binary format (msgpack)"""
        dag = DAG("binary_dag")

        @dag.node
        def compute() -> float:
            return 3.14

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test.msgpack"
            dag.save(str(path), format="msgpack")

            # Check it's binary data
            data = path.read_bytes()
            assert isinstance(data, bytes)
            assert len(data) > 0

    def test_load_with_auto_format_detection(self):
        """Test load with automatic format detection"""
        dag = DAG("load_test")
        dag.add_node("process", sample_process)

        with tempfile.TemporaryDirectory() as tmpdir:
            # Test each format
            for ext, _format_name in [
                (".json", "json"),
                (".yaml", "yaml"),
                (".yml", "yaml"),
                (".msgpack", "msgpack"),
            ]:
                path = Path(tmpdir) / f"test{ext}"
                dag.save(str(path))

                # Load without format specification
                loaded = DAG.load(str(path))
                assert loaded.name == "load_test"
                assert len(loaded.nodes) == 1

    def test_load_file_not_found(self):
        """Test load with non-existent file"""
        with pytest.raises(FileNotFoundError, match="File not found"):
            DAG.load("/non/existent/file.json")

    def test_load_with_explicit_format(self):
        """Test load with explicit format specification"""
        dag = DAG("explicit_format")
        dag.add_node("task", sample_task)

        with tempfile.TemporaryDirectory() as tmpdir:
            # Save as JSON
            path = Path(tmpdir) / "test.dat"
            dag.save(str(path), format="json")

            # Load with explicit format
            loaded = DAG.load(str(path), format="json")
            assert loaded.name == "explicit_format"

    def test_to_dict_and_from_dict(self):
        """Test dictionary conversion methods"""
        dag = DAG("dict_test", description="Dictionary test")
        dag.metadata["version"] = "1.0"
        dag.add_node("step1", sample_step1)
        dag.add_node("step2", sample_step2)
        dag.connect("step1", "step2", input="x")

        # Convert to dict
        dag_dict = dag.to_dict()
        assert isinstance(dag_dict, dict)
        assert dag_dict["name"] == "dict_test"
        assert dag_dict["description"] == "Dictionary test"
        assert dag_dict["metadata"]["version"] == "1.0"
        assert len(dag_dict["nodes"]) == 2

        # Create from dict
        loaded = DAG.from_dict(dag_dict)
        assert loaded.name == "dict_test"
        assert loaded.description == "Dictionary test"
        assert loaded.metadata["version"] == "1.0"
        assert len(loaded.nodes) == 2

    def test_to_yaml_and_from_yaml(self):
        """Test YAML conversion methods"""
        dag = DAG("yaml_test", description="YAML test")
        dag.add_node("process", sample_yaml_process)

        # Convert to YAML
        yaml_str = dag.to_yaml()
        assert isinstance(yaml_str, str)
        assert "yaml_test" in yaml_str
        assert "YAML test" in yaml_str

        # Create from YAML
        loaded = DAG.from_yaml(yaml_str)
        assert loaded.name == "yaml_test"
        assert loaded.description == "YAML test"
        assert len(loaded.nodes) == 1

    def test_to_yaml_with_bytes_result(self):
        """Test to_yaml when serializer returns bytes"""
        dag = DAG("bytes_yaml")

        @dag.node
        def task() -> int:
            return 42

        # Mock serializer to return bytes
        with patch("fast_dag.serialization.SerializerRegistry.get") as mock_get:
            mock_serializer = MagicMock()
            mock_serializer.serialize.return_value = b"name: bytes_yaml\n"
            mock_get.return_value = mock_serializer

            yaml_str = dag.to_yaml()
            assert isinstance(yaml_str, str)
            assert yaml_str == "name: bytes_yaml\n"

    def test_export_config_comprehensive(self):
        """Test comprehensive configuration export"""
        dag = DAG("config_export", description="Export test")
        dag.metadata["author"] = "test"

        # Add various node types
        @dag.node
        def standard(x: int) -> int:
            """Standard node"""
            return x

        @dag.condition
        def check(x: int) -> bool:
            """Conditional node"""
            return x > 0

        @dag.any
        def any_node(inputs: list[int]) -> int:
            """ANY node"""
            return sum(inputs)

        # Add node with retry and timeout
        def retry_func(x: int) -> int:
            return x + 1

        node = Node(
            func=retry_func,
            name="retry_node",
            inputs=["x"],
            outputs=["y"],
            description="Node with retry",
            node_type=NodeType.STANDARD,
            retry=3,
            retry_delay=1.0,
            timeout=30.0,
            metadata={"custom": "value"},
        )
        dag.add_node(node)

        # Create connections
        dag.connect("standard", "check", input="x")
        dag.nodes["check"].on_true >> dag.nodes["any_node"]
        dag.connect("retry_node", "any_node")

        # Export config
        config = dag.export_config()

        assert config["name"] == "config_export"
        assert config["description"] == "Export test"
        assert config["metadata"]["author"] == "test"

        # Check nodes
        assert len(config["nodes"]) == 4
        assert "standard" in config["nodes"]
        assert "check" in config["nodes"]
        assert "any_node" in config["nodes"]
        assert "retry_node" in config["nodes"]

        # Check node details
        retry_config = config["nodes"]["retry_node"]
        assert retry_config["name"] == "retry_node"
        assert retry_config["inputs"] == ["x"]
        assert retry_config["outputs"] == ["y"]
        assert retry_config["description"] == "Node with retry"
        assert retry_config["node_type"] == "standard"
        assert retry_config["retry"] == 3
        assert retry_config["retry_delay"] == 1.0
        assert retry_config["timeout"] == 30.0
        assert retry_config["metadata"]["custom"] == "value"

        # Check connections
        assert len(config["connections"]) >= 3
        connections = config["connections"]

        # Find specific connections
        standard_to_check = any(
            c["source"] == "standard" and c["target"] == "check" for c in connections
        )
        assert standard_to_check

    def test_export_config_with_complex_connections(self):
        """Test export config with complex output connections"""
        dag = DAG("complex_connections")

        @dag.node
        def source() -> dict[str, int]:
            return {"out1": 1, "out2": 2}

        @dag.node
        def target1(in1: int) -> int:
            return in1

        @dag.node
        def target2(in2: int) -> int:
            return in2

        # Multiple outputs to different inputs
        dag.connect("source", "target1", output="out1", input="in1")
        dag.connect("source", "target2", output="out2", input="in2")

        config = dag.export_config()
        connections = config["connections"]

        # Check both connections are exported correctly
        conn1 = next(c for c in connections if c["target"] == "target1")
        assert conn1["source"] == "source"
        assert conn1["output"] == "out1"
        assert conn1["input"] == "in1"

        conn2 = next(c for c in connections if c["target"] == "target2")
        assert conn2["source"] == "source"
        assert conn2["output"] == "out2"
        assert conn2["input"] == "in2"

    def test_serialization_with_string_result(self):
        """Test serialization when serializer returns string"""
        dag = DAG("string_result")

        @dag.node
        def task() -> str:
            return "result"

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test.json"

            # Mock serializer to return string
            with patch("fast_dag.serialization.SerializerRegistry.get") as mock_get:
                mock_serializer = MagicMock()
                mock_serializer.serialize.return_value = '{"name": "string_result"}'
                mock_get.return_value = mock_serializer

                dag.save(str(path))

                # Check file was written as text
                content = path.read_text()
                assert content == '{"name": "string_result"}'

    def test_load_msgpack_format(self):
        """Test loading msgpack format (binary)"""
        dag = DAG("msgpack_test")
        dag.add_node("compute", sample_compute)

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test.msgpack"
            dag.save(str(path))

            # Load msgpack
            loaded = DAG.load(str(path))
            assert loaded.name == "msgpack_test"

    def test_unknown_extension_with_load(self):
        """Test loading file with unknown extension defaults to JSON"""
        dag = DAG("unknown_ext")
        dag.add_node("process", sample_unknown_process)

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test.xyz"
            dag.save(str(path))  # Will save as JSON by default

            # Load without format should default to JSON
            loaded = DAG.load(str(path))
            assert loaded.name == "unknown_ext"
