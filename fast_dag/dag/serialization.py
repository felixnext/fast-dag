"""DAG serialization functionality."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from ..serialization import SerializerRegistry

if TYPE_CHECKING:
    from . import DAG


class DAGSerialization:
    """Mixin class for DAG serialization functionality."""

    # Type hints for attributes from DAG
    name: str
    description: str | None
    metadata: dict[str, Any]
    nodes: dict[str, Any]

    def save(self, filepath: str, *, format: str | None = None) -> None:
        """Save DAG to file.

        Args:
            filepath: Path to save file
            format: Serialization format (auto-detected from extension if None)
        """
        path = Path(filepath)

        # Auto-detect format from extension
        if format is None:
            extension = path.suffix.lower()
            if extension == ".json":
                format = "json"
            elif extension == ".yaml" or extension == ".yml":
                format = "yaml"
            elif extension == ".msgpack":
                format = "msgpack"
            else:
                format = "json"  # Default

        # Get serializer and save
        serializer = SerializerRegistry.get()
        data = serializer.serialize(self, format=format)

        # Write to file
        if isinstance(data, str):
            path.write_text(data)
        else:
            path.write_bytes(data)

    @classmethod
    def load(cls, filepath: str, *, format: str | None = None) -> DAG:
        """Load DAG from file.

        Args:
            filepath: Path to load file
            format: Serialization format (auto-detected from extension if None)

        Returns:
            Loaded DAG instance
        """
        path = Path(filepath)

        if not path.exists():
            raise FileNotFoundError(f"File not found: {filepath}")

        # Auto-detect format from extension
        if format is None:
            extension = path.suffix.lower()
            if extension == ".json":
                format = "json"
            elif extension == ".yaml" or extension == ".yml":
                format = "yaml"
            elif extension == ".msgpack":
                format = "msgpack"
            else:
                format = "json"  # Default

        # Load data
        data = path.read_bytes() if format == "msgpack" else path.read_text()

        # Get serializer and deserialize
        serializer = SerializerRegistry.get()
        return serializer.deserialize(data, target_type=cls, format=format)

    def to_dict(self) -> dict[str, Any]:
        """Convert DAG to dictionary representation.

        Returns:
            Dictionary representation of the DAG
        """
        serializer = SerializerRegistry.get()
        json_str = serializer.serialize(self, format="json")

        import json

        return json.loads(json_str)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DAG:
        """Create DAG from dictionary representation.

        Args:
            data: Dictionary representation

        Returns:
            DAG instance
        """
        import json

        json_str = json.dumps(data)

        serializer = SerializerRegistry.get()
        from . import DAG as DAGClass

        return serializer.deserialize(json_str, target_type=DAGClass, format="json")

    def to_yaml(self) -> str:
        """Convert DAG to YAML string.

        Returns:
            YAML string representation
        """
        serializer = SerializerRegistry.get()
        result = serializer.serialize(self, format="yaml")
        # Ensure we return a string
        if isinstance(result, bytes):
            return result.decode("utf-8")
        return result

    @classmethod
    def from_yaml(cls, yaml_str: str) -> DAG:
        """Create DAG from YAML string.

        Args:
            yaml_str: YAML string representation

        Returns:
            DAG instance
        """
        serializer = SerializerRegistry.get()
        from . import DAG as DAGClass

        return serializer.deserialize(yaml_str, target_type=DAGClass, format="yaml")

    def export_config(self) -> dict[str, Any]:
        """Export DAG configuration (structure without functions).

        Returns:
            Configuration dictionary
        """
        config: dict[str, Any] = {
            "name": self.name,
            "description": self.description,
            "metadata": self.metadata,
            "nodes": {},
            "connections": [],
        }

        # Export node configurations
        for node_name, node in self.nodes.items():
            config["nodes"][node_name] = {
                "name": node.name,
                "inputs": node.inputs,
                "outputs": node.outputs,
                "description": node.description,
                "node_type": node.node_type.value,
                "retry": node.retry,
                "retry_delay": node.retry_delay,
                "timeout": node.timeout,
                "metadata": node.metadata,
            }

        # Export connections
        for node_name, node in self.nodes.items():
            for output, connections in node.output_connections.items():
                for target_node, input_name in connections:
                    config["connections"].append(
                        {
                            "source": node_name,
                            "target": target_node.name,
                            "output": output,
                            "input": input_name,
                        }
                    )

        return config
