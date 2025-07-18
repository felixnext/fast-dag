"""
Tests for core introspection to improve coverage.
"""

from fast_dag.core.introspection import (
    get_function_inputs,
    get_function_name,
    get_function_outputs,
)


class TestIntrospectionCoverage:
    """Test introspection functions comprehensively"""

    def test_get_function_name_with_name_attribute(self):
        """Test get_function_name with __name__ attribute (line 36)"""

        def test_func():
            pass

        # Function should have __name__
        name = get_function_name(test_func)
        assert name == "test_func"

    def test_get_function_inputs_with_defaults(self):
        """Test get_function_inputs with default values (lines 56-57)"""

        def test_func(a, b=10, c=20):
            pass

        # Should get all parameters including those with defaults
        inputs = get_function_inputs(test_func)
        assert inputs == ["a", "b", "c"]

    def test_get_function_inputs_with_context(self):
        """Test get_function_inputs filtering context parameter (line 66)"""

        def test_func(x, context, y):
            pass

        # Should filter out 'context'
        inputs = get_function_inputs(test_func)
        assert inputs == ["x", "y"]
        assert "context" not in inputs

    def test_get_function_outputs_with_tuple_no_annotation(self):
        """Test get_function_outputs with tuple but no annotation (lines 92-96)"""

        def test_func():
            return (1, 2, 3)

        # Without annotations, should return default
        outputs = get_function_outputs(test_func)
        assert outputs == ["result"]

    def test_get_function_outputs_with_annotation_dict(self):
        """Test get_function_outputs with dict annotation (lines 109, 112)"""

        def test_func() -> dict[str, int]:
            return {"a": 1, "b": 2}

        # Dict return should give default output
        outputs = get_function_outputs(test_func)
        assert outputs == ["result"]

    def test_get_function_outputs_with_tuple_annotation(self):
        """Test get_function_outputs with tuple annotation (lines 117-118, 121-122)"""

        def test_func() -> tuple[int, str, float]:
            return (1, "test", 3.14)

        # Tuple with type args should generate output names
        outputs = get_function_outputs(test_func)
        assert outputs == ["output_0", "output_1", "output_2"]
