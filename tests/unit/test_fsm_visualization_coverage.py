"""
Tests for FSM visualization to improve coverage.
"""

from unittest.mock import MagicMock, patch

import pytest

from fast_dag import FSM


class TestFSMVisualizationCoverage:
    """Test FSM visualization comprehensively"""

    def test_visualize_import_error(self):
        """Test visualize with import error (lines 35-36)"""
        fsm = FSM("test")

        @fsm.state(initial=True)
        def start() -> str:
            return "end"

        @fsm.state
        def end() -> None:
            pass

        # Mock the import to raise ImportError
        with patch("fast_dag.fsm.visualization.FSMVisualization.visualize") as mock_viz:
            mock_viz.side_effect = ImportError(
                "Visualization requires optional dependencies. "
                "Install with: pip install fast-dag[viz]"
            )

            with pytest.raises(
                ImportError, match="Visualization requires optional dependencies"
            ):
                fsm.visualize()

    def test_visualize_graphviz_backend(self):
        """Test visualize with graphviz backend (lines 54-55)"""
        fsm = FSM("test")

        @fsm.state(initial=True)
        def start() -> str:
            return "end"

        @fsm.state
        def end() -> None:
            pass

        # Mock the visualization imports
        mock_graphviz = MagicMock()
        mock_graphviz.return_value.visualize_fsm.return_value = "digraph G { ... }"

        with patch.dict(
            "sys.modules",
            {
                "fast_dag.visualization": MagicMock(
                    GraphvizBackend=mock_graphviz,
                    MermaidBackend=MagicMock(),
                    VisualizationBackend=MagicMock,
                    VisualizationOptions=MagicMock,
                )
            },
        ):
            result = fsm.visualize(backend="graphviz")
            assert result == "digraph G { ... }"
            mock_graphviz.assert_called_once()

    def test_visualize_unknown_backend(self):
        """Test visualize with unknown backend (lines 56-57)"""
        fsm = FSM("test")

        @fsm.state(initial=True)
        def start() -> str:
            return "end"

        @fsm.state
        def end() -> None:
            pass

        # Mock the visualization imports
        with (
            patch.dict(
                "sys.modules",
                {
                    "fast_dag.visualization": MagicMock(
                        GraphvizBackend=MagicMock(),
                        MermaidBackend=MagicMock(),
                        VisualizationBackend=MagicMock,
                        VisualizationOptions=MagicMock,
                    )
                },
            ),
            pytest.raises(ValueError, match="Unknown backend: unknown"),
        ):
            fsm.visualize(backend="unknown")

    def test_visualize_with_filename(self):
        """Test visualize with filename to save (line 64)"""
        fsm = FSM("test")

        @fsm.state(initial=True)
        def start() -> str:
            return "end"

        @fsm.state
        def end() -> None:
            pass

        # Mock the visualization imports
        mock_mermaid = MagicMock()
        mock_viz_instance = MagicMock()
        mock_viz_instance.visualize_fsm.return_value = "stateDiagram-v2 ..."
        mock_mermaid.return_value = mock_viz_instance

        with patch.dict(
            "sys.modules",
            {
                "fast_dag.visualization": MagicMock(
                    GraphvizBackend=MagicMock(),
                    MermaidBackend=mock_mermaid,
                    VisualizationBackend=MagicMock,
                    VisualizationOptions=MagicMock,
                )
            },
        ):
            result = fsm.visualize(filename="test.png", format="png")

            # Check that save was called
            mock_viz_instance.save.assert_called_once_with(
                "stateDiagram-v2 ...", "test.png", "png"
            )
            assert result == "stateDiagram-v2 ..."
