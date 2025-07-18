"""
Tests for node validation to improve coverage.
"""

from unittest.mock import patch

from fast_dag.core.node.validation import NodeValidator
from fast_dag.core.types import NodeType


class TestNodeValidationCoverage:
    """Test node validation comprehensively"""

    def test_validate_func_not_callable(self):
        """Test validate with non-callable func (line 19)"""

        # Create a mock that passes initial checks but isn't actually callable
        def test_func(x):
            pass

        # Create a NodeValidator instance
        validator = NodeValidator(
            name="test",
            func=test_func,
            inputs=["x"],
            outputs=["result"],
            node_type=NodeType.STANDARD,
        )

        # Mock callable() to return False for our test
        import builtins

        original_callable = builtins.callable

        def mock_callable(obj):
            if obj is test_func:
                return False
            return original_callable(obj)

        # Patch callable and test
        with patch("builtins.callable", mock_callable):
            errors = validator.validate()
            assert len(errors) == 1
            assert "func must be callable" in errors[0]

    def test_validate_no_outputs(self):
        """Test validate with no outputs (line 23)"""

        def test_func():
            return 42

        validator = NodeValidator(
            name="test",
            func=test_func,
            inputs=[],
            outputs=[],  # Empty outputs list
            node_type=NodeType.STANDARD,
        )

        errors = validator.validate()
        assert len(errors) == 1
        assert "must have at least one output" in errors[0]

    def test_validate_conditional_missing_outputs(self):
        """Test validate conditional node with missing outputs (line 42)"""

        def test_func():
            return True

        # Test with missing true/false outputs
        validator = NodeValidator(
            name="test_cond",
            func=test_func,
            inputs=[],
            outputs=["result", "other"],  # Missing true/false
            node_type=NodeType.CONDITIONAL,
        )

        errors = validator.validate()
        assert len(errors) == 1
        assert (
            "must have 'true' and 'false' or 'on_true' and 'on_false' outputs"
            in errors[0]
        )

    def test_validate_conditional_with_on_true_on_false(self):
        """Test validate conditional node with on_true/on_false outputs"""

        def test_func():
            return True

        # Test with on_true/on_false (alternative naming)
        validator = NodeValidator(
            name="test_cond",
            func=test_func,
            inputs=[],
            outputs=["on_true", "on_false"],
            node_type=NodeType.CONDITIONAL,
        )

        errors = validator.validate()
        assert len(errors) == 0  # Should be valid

    def test_validate_conditional_with_true_false(self):
        """Test validate conditional node with true/false outputs"""

        def test_func():
            return True

        # Test with true/false
        validator = NodeValidator(
            name="test_cond",
            func=test_func,
            inputs=[],
            outputs=["true", "false"],
            node_type=NodeType.CONDITIONAL,
        )

        errors = validator.validate()
        assert len(errors) == 0  # Should be valid

    def test_validate_multiple_errors(self):
        """Test validate with multiple errors"""

        # Create a function to avoid the initialization error
        def test_func(x):
            pass

        # Create a validator with multiple issues
        validator = NodeValidator(
            name="test",
            func=test_func,
            inputs=["x"],
            outputs=[],  # Empty outputs
            node_type=NodeType.STANDARD,
        )

        # Mock callable to return False
        import builtins

        original_callable = builtins.callable

        def mock_callable(obj):
            if obj is test_func:
                return False
            return original_callable(obj)

        # Test with multiple errors
        with patch("builtins.callable", mock_callable):
            errors = validator.validate()
            assert len(errors) == 2
            assert any("func must be callable" in e for e in errors)
            assert any("must have at least one output" in e for e in errors)
