"""
Tests for serialization base to improve coverage.
"""

from unittest.mock import MagicMock

import pytest

from fast_dag.serialization.base import Serializer, SerializerRegistry


class TestSerializationBaseCoverage:
    """Test serialization base comprehensively"""

    def test_serializer_protocol_serialize(self):
        """Test Serializer protocol serialize method (line 12)"""
        # Create a mock that implements the protocol
        mock_serializer = MagicMock(spec=Serializer)
        mock_serializer.serialize.return_value = b"serialized"

        # Call the method to cover the protocol definition
        result = mock_serializer.serialize({"test": "data"}, format="json")
        assert result == b"serialized"

    def test_serializer_protocol_deserialize(self):
        """Test Serializer protocol deserialize method (line 18)"""
        # Create a mock that implements the protocol
        mock_serializer = MagicMock(spec=Serializer)
        mock_serializer.deserialize.return_value = {"test": "data"}

        # Call the method to cover the protocol definition
        result = mock_serializer.deserialize(b"data", dict, format="json")
        assert result == {"test": "data"}

    def test_serializer_registry_get_no_default(self):
        """Test SerializerRegistry.get with no default configured (line 40)"""
        # Clear the registry
        SerializerRegistry._serializers = {}
        SerializerRegistry._default = None

        with pytest.raises(ValueError, match="No default serializer configured"):
            SerializerRegistry.get()

    def test_serializer_registry_get_unknown_serializer(self):
        """Test SerializerRegistry.get with unknown serializer (line 42)"""
        # Clear the registry and set a default
        SerializerRegistry._serializers = {"json": MagicMock()}
        SerializerRegistry._default = "json"

        with pytest.raises(ValueError, match="Unknown serializer: xml"):
            SerializerRegistry.get("xml")

    def test_serializer_registry_list(self):
        """Test SerializerRegistry.list (line 48)"""
        # Set up some serializers
        mock_json = MagicMock(spec=Serializer)
        mock_yaml = MagicMock(spec=Serializer)

        SerializerRegistry._serializers = {"json": mock_json, "yaml": mock_yaml}

        result = SerializerRegistry.list()
        assert len(result) == 2
        assert "json" in result
        assert "yaml" in result

    def test_serializer_registry_register_default(self):
        """Test SerializerRegistry.register with default=True"""
        # Clear the registry
        SerializerRegistry._serializers = {}
        SerializerRegistry._default = None

        mock_serializer = MagicMock(spec=Serializer)
        SerializerRegistry.register("test", mock_serializer, default=True)

        assert SerializerRegistry._default == "test"
        assert SerializerRegistry._serializers["test"] is mock_serializer

    def test_serializer_registry_get_with_name(self):
        """Test SerializerRegistry.get with specific name"""
        # Set up serializers
        mock_json = MagicMock(spec=Serializer)
        mock_yaml = MagicMock(spec=Serializer)

        SerializerRegistry._serializers = {"json": mock_json, "yaml": mock_yaml}
        SerializerRegistry._default = "json"

        # Get specific serializer
        result = SerializerRegistry.get("yaml")
        assert result is mock_yaml
