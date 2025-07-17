"""Base serialization interfaces and registry."""

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Serializer(Protocol):
    """Protocol for serializers."""

    def serialize(self, obj: Any, format: str = "json") -> bytes | str:
        """Serialize an object to the specified format."""
        ...

    def deserialize(
        self, data: bytes | str, target_type: type, format: str = "json"
    ) -> Any:
        """Deserialize data to the target type."""
        ...


class SerializerRegistry:
    """Registry for serializers."""

    _serializers: dict[str, Serializer] = {}
    _default: str | None = None

    @classmethod
    def register(cls, name: str, serializer: Serializer, default: bool = False) -> None:
        """Register a serializer."""
        cls._serializers[name] = serializer
        if default or cls._default is None:
            cls._default = name

    @classmethod
    def get(cls, name: str | None = None) -> Serializer:
        """Get a serializer by name."""
        if name is None:
            name = cls._default
        if name is None:
            raise ValueError("No default serializer configured")
        if name not in cls._serializers:
            raise ValueError(f"Unknown serializer: {name}")
        return cls._serializers[name]

    @classmethod
    def list(cls) -> list[str]:
        """List available serializers."""
        return list(cls._serializers.keys())
