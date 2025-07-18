"""DAG visualization functionality."""

from typing import TYPE_CHECKING, Any

from ..core.context import Context
from ..core.node import Node
from ..visualization import GraphvizBackend, MermaidBackend

if TYPE_CHECKING:
    pass


class DAGVisualization:
    """Mixin class for DAG visualization functionality."""

    # Type hints for attributes from DAG
    name: str
    description: str | None
    nodes: dict[str, Node]
    context: Context | None

    def get_entry_points(self) -> list[str]:
        """Get entry points - defined in DAG."""
        raise NotImplementedError

    def get_execution_order(self) -> list[str]:
        """Get execution order - defined in DAG."""
        raise NotImplementedError

    @property
    def is_valid(self) -> bool:
        """Check if DAG is valid - defined in DAG."""
        raise NotImplementedError

    def visualize(
        self,
        *,
        backend: str = "mermaid",
        show_results: bool = False,
        **kwargs,
    ) -> str:
        """Visualize the DAG.

        Args:
            backend: Visualization backend ('mermaid' or 'graphviz')
            show_results: Whether to show execution results
            **kwargs: Additional options for the backend

        Returns:
            Visualization string
        """
        visualizer: MermaidBackend | GraphvizBackend
        if backend == "mermaid":
            visualizer = MermaidBackend()
        elif backend == "graphviz":
            visualizer = GraphvizBackend()
        else:
            raise ValueError(f"Unknown visualization backend: {backend}")

        # Pass context if showing results
        if show_results and self.context:
            kwargs["context"] = self.context
        elif (
            "options" in kwargs
            and hasattr(kwargs["options"], "show_results")
            and kwargs["options"].show_results
            and self.context
        ):
            # If options object is passed, check its show_results flag
            kwargs["context"] = self.context

        return visualizer.visualize_dag(self, **kwargs)

    def to_mermaid(self, **kwargs) -> str:
        """Generate Mermaid diagram.

        Args:
            **kwargs: Options for Mermaid backend

        Returns:
            Mermaid diagram string
        """
        return self.visualize(backend="mermaid", **kwargs)

    def to_graphviz(self, **kwargs) -> str:
        """Generate Graphviz diagram.

        Args:
            **kwargs: Options for Graphviz backend

        Returns:
            Graphviz diagram string
        """
        return self.visualize(backend="graphviz", **kwargs)

    def save_visualization(
        self,
        filepath: str,
        *,
        backend: str = "mermaid",
        show_results: bool = False,
        **kwargs,
    ) -> None:
        """Save visualization to file.

        Args:
            filepath: Path to save file
            backend: Visualization backend
            show_results: Whether to show execution results
            **kwargs: Additional options
        """
        viz_str = self.visualize(backend=backend, show_results=show_results, **kwargs)

        with open(filepath, "w") as f:
            f.write(viz_str)

    def get_visualization_info(self) -> dict[str, Any]:
        """Get information about the DAG for visualization.

        Returns:
            Dictionary with visualization information
        """
        info: dict[str, Any] = {
            "name": self.name,
            "description": self.description,
            "total_nodes": len(self.nodes),
            "entry_points": self.get_entry_points(),
            "execution_order": self.get_execution_order() if self.is_valid else None,
            "node_types": {},
            "connections": [],
            "nested_workflows": {},
        }

        # Count node types
        for node in self.nodes.values():
            node_type = node.node_type.value
            info["node_types"][node_type] = info["node_types"].get(node_type, 0) + 1

        # List connections
        for node_name, node in self.nodes.items():
            for output, connections in node.output_connections.items():
                for target_node, input_name in connections:
                    info["connections"].append(
                        {
                            "source": node_name,
                            "target": target_node.name,
                            "output": output,
                            "input": input_name,
                        }
                    )

        # List nested workflows
        for node_name, node in self.nodes.items():
            if hasattr(node, "dag"):
                info["nested_workflows"][node_name] = {
                    "type": "DAG",
                    "name": node.dag.name,
                    "nodes": len(node.dag.nodes),
                }
            elif hasattr(node, "fsm"):
                info["nested_workflows"][node_name] = {
                    "type": "FSM",
                    "name": node.fsm.name,
                    "nodes": len(node.fsm.nodes),
                }

        return info
