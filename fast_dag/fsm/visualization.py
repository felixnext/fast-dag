"""FSM visualization functionality."""

from typing import Any

from .execution import FSMExecutor


class FSMVisualization(FSMExecutor):
    """FSM visualization functionality."""

    def visualize(
        self,
        *,
        backend: str = "mermaid",
        show_results: bool = False,
        **kwargs: Any,
    ) -> str:
        """Visualize the FSM as a state diagram.

        Args:
            backend: Visualization backend ("mermaid" or "graphviz")
            show_results: Whether to show execution results
            **kwargs: Additional visualization options (filename, format, options)

        Returns:
            String representation of the visualization
        """
        try:
            from ..visualization import (
                GraphvizBackend,
                MermaidBackend,
                VisualizationBackend,
                VisualizationOptions,
            )
        except ImportError as e:
            raise ImportError(
                "Visualization requires optional dependencies. "
                "Install with: pip install fast-dag[viz]"
            ) from e

        # Extract options from kwargs
        options = kwargs.get("options")
        filename = kwargs.get("filename")
        format = kwargs.get("format", "png")

        # Create options if not provided
        if options is None:
            options = VisualizationOptions()
            options.show_results = show_results

        # Select backend
        if backend.lower() == "mermaid":
            viz: VisualizationBackend = MermaidBackend(options)
        elif backend.lower() == "graphviz":
            viz = GraphvizBackend(options)
        else:
            raise ValueError(f"Unknown backend: {backend}")

        # Generate visualization
        content = viz.visualize_fsm(self)

        # Save if filename provided
        if filename:
            viz.save(content, filename, format)

        return content
