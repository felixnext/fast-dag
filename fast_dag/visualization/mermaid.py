"""Mermaid visualization backend."""

from typing import Any

from .base import VisualizationBackend


class MermaidBackend(VisualizationBackend):
    """Mermaid diagram visualization backend."""

    def visualize_dag(self, dag: Any, **kwargs) -> str:
        """Generate Mermaid diagram for a DAG."""
        # Extract options if provided
        if "options" in kwargs:
            self.options = kwargs["options"]

        # Extract context if provided
        context = kwargs.get("context")
        lines = ["graph " + self.options.direction]

        # Get node types for special rendering
        from ..core.types import NodeType

        # Add nodes
        for node_name, node in dag.nodes.items():
            label = self.get_node_label(node)
            node_id = self._sanitize_id(node_name)

            # Choose shape based on node type
            if node.node_type == NodeType.CONDITIONAL:
                lines.append(f"    {node_id}{{{{{label}}}}}")  # Diamond shape
            elif node.node_type == NodeType.SELECT:
                lines.append(f"    {node_id}[/{label}/]")  # Trapezoid shape
            elif node.node_type in (NodeType.ANY, NodeType.ALL):
                lines.append(f"    {node_id}(({label}))")  # Circle shape
            elif node.node_type == NodeType.DAG:
                lines.append(
                    f"    {node_id}[[{label}]]"
                )  # Subroutine shape (double rectangle)
            elif node.node_type == NodeType.FSM:
                lines.append(f"    {node_id}[({label})]")  # Stadium shape
            else:
                lines.append(f"    {node_id}[{label}]")  # Rectangle shape

            # Add styling if needed
            if context and self.options.show_results:
                style = self._get_mermaid_style(node, context)
                if style:
                    lines.append(f"    {style}")

        # Add edges
        for node_name, node in dag.nodes.items():
            node_id = self._sanitize_id(node_name)
            for output_name, connections in node.output_connections.items():
                for target_node, input_name in connections:
                    target_id = self._sanitize_id(target_node.name)

                    # Add edge label if not default
                    if (
                        output_name != "result" or input_name != "data"
                    ) and self.options.show_inputs:
                        edge_label = f"{output_name} → {input_name}"
                        lines.append(f"    {node_id} -->|{edge_label}| {target_id}")
                    else:
                        lines.append(f"    {node_id} --> {target_id}")

        return "\n".join(lines)

    def visualize_fsm(self, fsm: Any, **kwargs) -> str:
        """Generate Mermaid state diagram for an FSM."""
        # Extract options if provided
        if "options" in kwargs:
            self.options = kwargs["options"]

        # Extract context if provided
        kwargs.get("context")
        lines = ["stateDiagram-v2"]

        # Add states
        for node_name, node in fsm.nodes.items():
            label = self.get_node_label(node)
            node_id = self._sanitize_id(node_name)

            # Mark initial state
            if node_name == fsm.initial_state:
                lines.append(f"    [*] --> {node_id}")

            lines.append(f"    {node_id} : {label}")

            # Mark terminal states
            if node_name in fsm.terminal_states:
                lines.append(f"    {node_id} --> [*]")

        # Add transitions from state_transitions
        for from_state, transitions in fsm.state_transitions.items():
            from_id = self._sanitize_id(from_state)
            for condition, to_state in transitions.items():
                to_id = self._sanitize_id(to_state)
                if condition == "default":
                    lines.append(f"    {from_id} --> {to_id}")
                else:
                    lines.append(f"    {from_id} --> {to_id} : {condition}")

        # Add transitions from connections (for FSMs that use connections)
        for node_name, node in fsm.nodes.items():
            node_id = self._sanitize_id(node_name)
            for _output_name, connections in node.output_connections.items():
                for target_node, _input_name in connections:
                    target_id = self._sanitize_id(target_node.name)
                    # Don't duplicate if already in state_transitions
                    if (
                        node_name not in fsm.state_transitions
                        or target_node.name
                        not in fsm.state_transitions[node_name].values()
                    ):
                        lines.append(f"    {node_id} --> {target_id}")

        return "\n".join(lines)

    def save(self, content: str, filename: str, format: str = "png") -> None:
        """Save Mermaid diagram to file.

        For Mermaid, we save the diagram source and optionally render it.
        """
        # Save the Mermaid source
        with open(f"{filename}.mmd", "w") as f:
            f.write(content)

        if format != "mmd":
            # Try to render using mermaid CLI if available
            try:
                import subprocess

                subprocess.run(
                    ["mmdc", "-i", f"{filename}.mmd", "-o", f"{filename}.{format}"],
                    check=True,
                    capture_output=True,
                )
            except (subprocess.CalledProcessError, FileNotFoundError):
                print(
                    "Warning: Could not render Mermaid diagram. Install mermaid-cli with 'npm install -g @mermaid-js/mermaid-cli'"
                )
                print(f"Mermaid source saved to {filename}.mmd")

    def _sanitize_id(self, node_id: str) -> str:
        """Sanitize node ID for Mermaid."""
        # Replace spaces and special characters
        return node_id.replace(" ", "_").replace("-", "_").replace(".", "_")

    def _get_mermaid_style(self, node: Any, context: Any) -> str | None:
        """Get Mermaid style string for a node."""
        style = self.get_node_style(node, context)
        if not style:
            return None

        node_id = self._sanitize_id(node.name)
        if "fillcolor" in style:
            # Mermaid uses different syntax for styling
            color = style["fillcolor"]
            return f"    style {node_id} fill:{color}"

        return None
