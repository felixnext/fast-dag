"""FSM serialization functionality."""

from typing import TYPE_CHECKING

from .visualization import FSMVisualization

if TYPE_CHECKING:
    from ..fsm import FSM


class FSMSerialization(FSMVisualization):
    """FSM serialization functionality."""

    def save(self, filepath: str, *, format: str | None = None) -> None:
        """Save the FSM to a file.

        Args:
            filepath: File path to save to
            format: Serialization format (json, yaml, msgpack)
        """
        from ..serialization import SerializerRegistry

        # Determine format from filename if not provided
        if format is None:
            if filepath.endswith(".json"):
                format = "json"
            elif filepath.endswith(".yaml") or filepath.endswith(".yml"):
                format = "yaml"
            elif filepath.endswith(".msgpack") or filepath.endswith(".msgpk"):
                format = "msgpack"
            else:
                format = "json"  # Default to JSON

        ser = SerializerRegistry.get()  # Get default serializer
        data = ser.serialize(self, format=format)

        # Write to file
        if format == "msgpack":
            mode = "wb"
            if isinstance(data, str):
                data = data.encode("utf-8")
        else:
            mode = "w"
            if isinstance(data, bytes):
                data = data.decode("utf-8")

        with open(filepath, mode) as f:
            f.write(data)

    @classmethod
    def load(
        cls, filename: str, format: str | None = None, serializer: str | None = None
    ) -> "FSM":
        """Load an FSM from a file.

        Args:
            filename: File path to load from
            format: Serialization format (auto-detected if None)
            serializer: Serializer name (defaults to registered default)

        Returns:
            Loaded FSM instance
        """
        from ..serialization import SerializerRegistry

        # Auto-detect format from extension
        if format is None:
            if filename.endswith(".json"):
                format = "json"
            elif filename.endswith(".yaml") or filename.endswith(".yml"):
                format = "yaml"
            elif filename.endswith(".msgpack") or filename.endswith(".mp"):
                format = "msgpack"
            else:
                format = "json"  # Default

        # Read file
        mode = "rb" if format == "msgpack" else "r"

        with open(filename, mode) as f:
            data = f.read()

        # Deserialize
        ser = SerializerRegistry.get(serializer)
        return ser.deserialize(data, target_type=cls, format=format)
