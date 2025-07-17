"""Serialization support for fast-dag workflows."""

from .base import Serializer, SerializerRegistry
from .msgspec_serializer import MsgspecSerializer
from .registry import FunctionRegistry
from .types import SerializableDAG, SerializableNode

__all__ = [
    "Serializer",
    "SerializerRegistry",
    "MsgspecSerializer",
    "FunctionRegistry",
    "SerializableDAG",
    "SerializableNode",
]

# Register msgspec serializer as default if available
try:
    serializer = MsgspecSerializer()
    SerializerRegistry.register("msgspec", serializer, default=True)
except ImportError:
    pass
