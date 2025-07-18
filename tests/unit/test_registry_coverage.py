"""
Tests for registry to improve coverage.
"""

import pytest

from fast_dag.registry import FunctionRegistry


class TestRegistryCoverage:
    """Test registry comprehensively"""

    def test_register_with_metadata(self):
        """Test register with metadata (lines 42-43)"""
        registry = FunctionRegistry()

        @registry.register(name="test_func", version="1.0", author="test")
        def func():
            return 42

        assert "test_func" in registry
        assert registry.metadata["test_func"]["version"] == "1.0"
        assert registry.metadata["test_func"]["author"] == "test"

    def test_register_with_function_directly(self):
        """Test register with function passed directly (lines 46-49)"""
        registry = FunctionRegistry()

        def func():
            return 42

        # Call register with function directly (not as decorator)
        result = registry.register(func)

        assert result is func
        assert "func" in registry
        assert registry.get("func") is func

    def test_get_missing_function(self):
        """Test get with missing function (line 53)"""
        registry = FunctionRegistry()

        result = registry.get("missing")
        assert result is None

    def test_getitem_missing_function(self):
        """Test __getitem__ with missing function (lines 57-58)"""
        registry = FunctionRegistry()

        with pytest.raises(KeyError, match="Function 'missing' not found in registry"):
            _ = registry["missing"]

    def test_getitem_existing_function(self):
        """Test __getitem__ with existing function (line 59)"""
        registry = FunctionRegistry()

        @registry.register
        def func():
            return 42

        result = registry["func"]
        assert result is func

    def test_contains(self):
        """Test __contains__ (line 63)"""
        registry = FunctionRegistry()

        @registry.register
        def func():
            return 42

        assert "func" in registry
        assert "missing" not in registry

    def test_list_functions(self):
        """Test list_functions (line 67)"""
        registry = FunctionRegistry()

        @registry.register
        def func1():
            return 1

        @registry.register
        def func2():
            return 2

        functions = registry.list_functions()
        assert len(functions) == 2
        assert "func1" in functions
        assert "func2" in functions

    def test_clear(self):
        """Test clear (lines 71-72)"""
        registry = FunctionRegistry()

        @registry.register(name="test", version="1.0")
        def func():
            return 42

        assert "test" in registry
        assert "test" in registry.metadata

        registry.clear()

        assert "test" not in registry
        assert len(registry.functions) == 0
        assert len(registry.metadata) == 0

    def test_register_decorator_with_name(self):
        """Test register as decorator with custom name"""
        registry = FunctionRegistry()

        @registry.register(name="custom_name")
        def original_name():
            return 42

        assert "custom_name" in registry
        assert "original_name" not in registry
        assert registry["custom_name"]() == 42

    def test_register_multiple_functions(self):
        """Test registering multiple functions"""
        registry = FunctionRegistry()

        @registry.register
        def add(a: int, b: int) -> int:
            return a + b

        @registry.register
        def multiply(a: int, b: int) -> int:
            return a * b

        assert len(registry.list_functions()) == 2
        assert registry["add"](2, 3) == 5
        assert registry["multiply"](2, 3) == 6

    def test_register_same_name_overwrites(self):
        """Test that registering with same name overwrites previous"""
        registry = FunctionRegistry()

        @registry.register
        def func():
            return 1

        @registry.register(name="func")
        def func2():
            return 2

        assert registry["func"]() == 2  # Should be the second function
