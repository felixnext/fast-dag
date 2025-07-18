"""
Tests for FSM serialization to improve coverage.
"""

from unittest.mock import MagicMock, mock_open, patch

from fast_dag import FSM
from fast_dag.serialization import SerializerRegistry


class TestFSMSerializationCoverage:
    """Test FSM serialization comprehensively"""

    def test_save_with_yaml_extension(self):
        """Test save with yaml extension (lines 27-28)"""
        fsm = FSM("test")

        @fsm.state(initial=True)
        def start() -> str:
            return "end"

        @fsm.state
        def end() -> None:
            pass

        # Mock serializer
        mock_serializer = MagicMock()
        mock_serializer.serialize.return_value = "yaml content"

        with (
            patch.object(SerializerRegistry, "get", return_value=mock_serializer),
            patch("builtins.open", mock_open()) as mock_file,
        ):
            fsm.save("test.yaml")

            # Check format was detected as yaml
            mock_serializer.serialize.assert_called_once_with(fsm, format="yaml")
            mock_file.assert_called_once_with("test.yaml", "w")
            mock_file().write.assert_called_once_with("yaml content")

    def test_save_with_yml_extension(self):
        """Test save with yml extension (lines 27-28)"""
        fsm = FSM("test")

        @fsm.state(initial=True)
        def start() -> str:
            return "end"

        @fsm.state
        def end() -> None:
            pass

        # Mock serializer
        mock_serializer = MagicMock()
        mock_serializer.serialize.return_value = "yaml content"

        with (
            patch.object(SerializerRegistry, "get", return_value=mock_serializer),
            patch("builtins.open", mock_open()) as mock_file,
        ):
            fsm.save("test.yml")

            # Check format was detected as yaml
            mock_serializer.serialize.assert_called_once_with(fsm, format="yaml")
            mock_file.assert_called_once_with("test.yml", "w")

    def test_save_with_msgpack_extension(self):
        """Test save with msgpack extension (lines 29-30)"""
        fsm = FSM("test")

        @fsm.state(initial=True)
        def start() -> str:
            return "end"

        @fsm.state
        def end() -> None:
            pass

        # Mock serializer
        mock_serializer = MagicMock()
        mock_serializer.serialize.return_value = b"msgpack bytes"

        with (
            patch.object(SerializerRegistry, "get", return_value=mock_serializer),
            patch("builtins.open", mock_open()) as mock_file,
        ):
            fsm.save("test.msgpack")

            # Check format was detected as msgpack
            mock_serializer.serialize.assert_called_once_with(fsm, format="msgpack")
            mock_file.assert_called_once_with("test.msgpack", "wb")
            mock_file().write.assert_called_once_with(b"msgpack bytes")

    def test_save_with_msgpk_extension(self):
        """Test save with msgpk extension (lines 29-30)"""
        fsm = FSM("test")

        @fsm.state(initial=True)
        def start() -> str:
            return "end"

        @fsm.state
        def end() -> None:
            pass

        # Mock serializer
        mock_serializer = MagicMock()
        mock_serializer.serialize.return_value = b"msgpack bytes"

        with (
            patch.object(SerializerRegistry, "get", return_value=mock_serializer),
            patch("builtins.open", mock_open()) as mock_file,
        ):
            fsm.save("test.msgpk")

            # Check format was detected as msgpack
            mock_serializer.serialize.assert_called_once_with(fsm, format="msgpack")
            mock_file.assert_called_once_with("test.msgpk", "wb")

    def test_save_default_to_json(self):
        """Test save defaults to json for unknown extensions (lines 31-32)"""
        fsm = FSM("test")

        @fsm.state(initial=True)
        def start() -> str:
            return "end"

        @fsm.state
        def end() -> None:
            pass

        # Mock serializer
        mock_serializer = MagicMock()
        mock_serializer.serialize.return_value = "json content"

        with (
            patch.object(SerializerRegistry, "get", return_value=mock_serializer),
            patch("builtins.open", mock_open()) as mock_file,
        ):
            fsm.save("test.unknown")

            # Check format defaulted to json
            mock_serializer.serialize.assert_called_once_with(fsm, format="json")
            mock_file.assert_called_once_with("test.unknown", "w")

    def test_save_msgpack_string_to_bytes(self):
        """Test save converts string to bytes for msgpack (lines 40-41)"""
        fsm = FSM("test")

        @fsm.state(initial=True)
        def start() -> str:
            return "end"

        @fsm.state
        def end() -> None:
            pass

        # Mock serializer returning string instead of bytes
        mock_serializer = MagicMock()
        mock_serializer.serialize.return_value = "msgpack string"

        with (
            patch.object(SerializerRegistry, "get", return_value=mock_serializer),
            patch("builtins.open", mock_open()) as mock_file,
        ):
            fsm.save("test.msgpack")

            # Check string was converted to bytes
            mock_file().write.assert_called_once_with(b"msgpack string")

    def test_save_json_bytes_to_string(self):
        """Test save converts bytes to string for json (lines 44-45)"""
        fsm = FSM("test")

        @fsm.state(initial=True)
        def start() -> str:
            return "end"

        @fsm.state
        def end() -> None:
            pass

        # Mock serializer returning bytes instead of string
        mock_serializer = MagicMock()
        mock_serializer.serialize.return_value = b"json bytes"

        with (
            patch.object(SerializerRegistry, "get", return_value=mock_serializer),
            patch("builtins.open", mock_open()) as mock_file,
        ):
            fsm.save("test.json")

            # Check bytes were converted to string
            mock_file().write.assert_called_once_with("json bytes")

    def test_load_with_yaml_extension(self):
        """Test load with yaml extension (lines 70-71)"""
        # Mock file content
        yaml_content = "yaml content"

        # Mock serializer
        mock_serializer = MagicMock()
        mock_fsm = FSM("loaded")
        mock_serializer.deserialize.return_value = mock_fsm

        with (
            patch("builtins.open", mock_open(read_data=yaml_content)),
            patch.object(SerializerRegistry, "get", return_value=mock_serializer),
        ):
            result = FSM.load("test.yaml")

            # Check format was detected as yaml
            mock_serializer.deserialize.assert_called_once_with(
                yaml_content, target_type=FSM, format="yaml"
            )
            assert result == mock_fsm

    def test_load_with_yml_extension(self):
        """Test load with yml extension (lines 70-71)"""
        # Mock file content
        yaml_content = "yaml content"

        # Mock serializer
        mock_serializer = MagicMock()
        mock_fsm = FSM("loaded")
        mock_serializer.deserialize.return_value = mock_fsm

        with (
            patch("builtins.open", mock_open(read_data=yaml_content)),
            patch.object(SerializerRegistry, "get", return_value=mock_serializer),
        ):
            result = FSM.load("test.yml")

            # Check format was detected as yaml
            mock_serializer.deserialize.assert_called_once_with(
                yaml_content, target_type=FSM, format="yaml"
            )
            assert result == mock_fsm

    def test_load_with_msgpack_extension(self):
        """Test load with msgpack extension (lines 72-73)"""
        # Mock file content
        msgpack_content = b"msgpack bytes"

        # Mock serializer
        mock_serializer = MagicMock()
        mock_fsm = FSM("loaded")
        mock_serializer.deserialize.return_value = mock_fsm

        with (
            patch("builtins.open", mock_open(read_data=msgpack_content)),
            patch.object(SerializerRegistry, "get", return_value=mock_serializer),
        ):
            result = FSM.load("test.msgpack")

            # Check format was detected as msgpack and file opened in binary mode
            mock_serializer.deserialize.assert_called_once_with(
                msgpack_content, target_type=FSM, format="msgpack"
            )
            assert result == mock_fsm

    def test_load_with_mp_extension(self):
        """Test load with mp extension (lines 72-73)"""
        # Mock file content
        msgpack_content = b"msgpack bytes"

        # Mock serializer
        mock_serializer = MagicMock()
        mock_fsm = FSM("loaded")
        mock_serializer.deserialize.return_value = mock_fsm

        with (
            patch("builtins.open", mock_open(read_data=msgpack_content)),
            patch.object(SerializerRegistry, "get", return_value=mock_serializer),
        ):
            result = FSM.load("test.mp")

            # Check format was detected as msgpack
            mock_serializer.deserialize.assert_called_once_with(
                msgpack_content, target_type=FSM, format="msgpack"
            )
            assert result == mock_fsm

    def test_load_default_to_json(self):
        """Test load defaults to json for unknown extensions (lines 74-75)"""
        # Mock file content
        json_content = "json content"

        # Mock serializer
        mock_serializer = MagicMock()
        mock_fsm = FSM("loaded")
        mock_serializer.deserialize.return_value = mock_fsm

        with (
            patch("builtins.open", mock_open(read_data=json_content)),
            patch.object(SerializerRegistry, "get", return_value=mock_serializer),
        ):
            result = FSM.load("test.unknown")

            # Check format defaulted to json
            mock_serializer.deserialize.assert_called_once_with(
                json_content, target_type=FSM, format="json"
            )
            assert result == mock_fsm
