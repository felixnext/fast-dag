"""
Test for FSM visualization import error coverage.
"""

import sys
from unittest.mock import patch

import pytest

from fast_dag import FSM


def test_visualize_import_error_handling():
    """Test visualize handles ImportError from visualization imports (lines 35-36)"""
    fsm = FSM("test")

    @fsm.state(initial=True)
    def start() -> str:
        return "end"

    @fsm.state
    def end() -> None:
        pass

    # Remove visualization module to force ImportError
    modules_to_remove = []
    for module in list(sys.modules.keys()):
        if "fast_dag.visualization" in module:
            modules_to_remove.append(module)

    for module in modules_to_remove:
        del sys.modules[module]

    # Patch the import to raise ImportError
    with (
        patch.dict("sys.modules", {"fast_dag.visualization": None}),
        pytest.raises(
            ImportError, match="Visualization requires optional dependencies"
        ),
    ):
        fsm.visualize()
