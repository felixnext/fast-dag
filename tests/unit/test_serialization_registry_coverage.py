"""
Tests for serialization registry to achieve 100% coverage.
"""

import pytest

from fast_dag.serialization.registry import FunctionRegistry, get_function_path


class TestSerializationRegistryCoverage:
    """Test serialization registry to achieve 100% coverage"""

    def test_register_module_already_registered(self):
        """Test register_module when module is already registered (line 60)"""
        FunctionRegistry.clear()

        # Register a module first time
        FunctionRegistry.register_module("math")

        # Register the same module again - should return early
        FunctionRegistry.register_module("math")

        # Should still work without issues
        assert "math" in FunctionRegistry._modules

    def test_register_module_auto_discover_import_error(self):
        """Test register_module with auto_discover when import fails (lines 71-72)"""
        FunctionRegistry.clear()

        # Try to register a non-existent module
        FunctionRegistry.register_module("nonexistent_module_that_does_not_exist")

        # Should add to modules set even if import fails
        assert "nonexistent_module_that_does_not_exist" in FunctionRegistry._modules

    def test_get_function_path_with_lambda(self):
        """Test get_function_path with lambda function (lines 92-93)"""

        # Create a lambda function
        def lambda_func(x):
            return x + 1

        lambda_func.__name__ = "<lambda>"
        lambda_func.__qualname__ = "<lambda>"

        # Should raise ValueError for lambda functions
        with pytest.raises(ValueError, match="Lambda functions cannot be serialized"):
            get_function_path(lambda_func)

    def test_get_function_path_with_regular_function(self):
        """Test get_function_path with regular function (lines 88-95)"""

        def test_func():
            return "test"

        # Should return the full path
        path = get_function_path(test_func)
        assert path.endswith("test_func")
        assert "test_serialization_registry_coverage" in path

    def test_get_function_path_with_nested_function(self):
        """Test get_function_path with nested function"""

        def outer_func():
            def inner_func():
                return "inner"

            return inner_func

        inner = outer_func()
        path = get_function_path(inner)
        assert "outer_func.<locals>.inner_func" in path

    def test_register_module_with_auto_discover_false(self):
        """Test register_module with auto_discover=False"""
        FunctionRegistry.clear()

        # Register module without auto discovery
        FunctionRegistry.register_module("math", auto_discover=False)

        # Should add to modules set but not discover functions
        assert "math" in FunctionRegistry._modules

        # Should not have discovered math functions
        functions = FunctionRegistry.list_functions()
        math_functions = [f for f in functions if f.startswith("math.")]
        assert len(math_functions) == 0

    def test_register_module_with_auto_discover_true(self):
        """Test register_module with auto_discover=True"""
        FunctionRegistry.clear()

        # Create a test module with a function by defining it here
        import types

        test_module = types.ModuleType("test_module")
        test_module.test_function = lambda: "test"

        # Register module with auto discovery
        FunctionRegistry.register_module("math", auto_discover=True)

        # Should have executed the auto discovery code path
        assert "math" in FunctionRegistry._modules
