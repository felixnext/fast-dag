"""Base classes for visualization support."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class VisualizationOptions:
    """Options for visualization."""

    # Layout options
    direction: str = "TB"  # TB (top-bottom), LR (left-right), BT, RL
    node_shape: str = "box"  # box, ellipse, diamond, etc.

    # Styling options
    node_color: str | None = None
    edge_color: str | None = None
    font_size: int | None = None

    # Execution result visualization
    show_results: bool = False
    success_color: str = "#90EE90"  # Light green
    error_color: str = "#FFB6C1"  # Light red
    skipped_color: str = "#D3D3D3"  # Light gray

    # Content options
    show_inputs: bool = True
    show_outputs: bool = True
    show_types: bool = False
    show_description: bool = False


class VisualizationBackend(ABC):
    """Abstract base class for visualization backends."""

    def __init__(self, options: VisualizationOptions | None = None):
        """Initialize the backend with options."""
        self.options = options or VisualizationOptions()

    @abstractmethod
    def visualize_dag(self, dag: Any, **kwargs) -> str:
        """Generate visualization for a DAG.

        Args:
            dag: The DAG instance to visualize
            **kwargs: Additional options (e.g., context, options)

        Returns:
            String representation of the visualization
        """
        pass

    @abstractmethod
    def visualize_fsm(self, fsm: Any, **kwargs) -> str:
        """Generate visualization for an FSM.

        Args:
            fsm: The FSM instance to visualize
            **kwargs: Additional options (e.g., context, options)

        Returns:
            String representation of the visualization
        """
        pass

    @abstractmethod
    def save(self, content: str, filename: str, format: str = "png") -> None:
        """Save visualization to file.

        Args:
            content: The visualization content
            filename: Output filename (without extension)
            format: Output format (png, svg, pdf, etc.)
        """
        pass

    def get_node_label(self, node: Any) -> str:
        """Get label for a node based on options."""
        parts = [node.name or "unnamed"]

        if self.options.show_inputs and node.inputs:
            parts.append(f"in: {', '.join(node.inputs)}")

        if self.options.show_outputs and node.outputs:
            parts.append(f"out: {', '.join(node.outputs)}")

        if self.options.show_description and node.description:
            parts.append(
                node.description[:50] + "..."
                if len(node.description) > 50
                else node.description
            )

        return "\\n".join(parts)

    def get_node_style(self, node: Any, context: Any = None) -> dict[str, str]:
        """Get style attributes for a node."""
        style = {}

        if context and self.options.show_results:
            node_name = node.name
            if node_name in context.results:
                style["fillcolor"] = self.options.success_color
                style["style"] = "filled"
            elif (
                hasattr(context, "metadata")
                and f"{node_name}_error" in context.metadata
            ):
                style["fillcolor"] = self.options.error_color
                style["style"] = "filled"
            else:
                style["fillcolor"] = self.options.skipped_color
                style["style"] = "filled"

        if self.options.node_color and "fillcolor" not in style:
            style["fillcolor"] = self.options.node_color
            style["style"] = "filled"

        return style
