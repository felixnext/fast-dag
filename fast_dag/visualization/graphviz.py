"""Graphviz visualization backend."""

from typing import Any

from .base import VisualizationBackend


class GraphvizBackend(VisualizationBackend):
    """Graphviz visualization backend."""

    def visualize_dag(self, dag: Any) -> str:
        """Generate Graphviz DOT notation for a DAG."""
        lines = ["digraph DAG {"]

        # Set graph attributes
        lines.append(f"    rankdir={self.options.direction};")
        if self.options.font_size:
            lines.append(f"    fontsize={self.options.font_size};")

        # Get node types for special rendering
        from ..core.types import NodeType

        # Add nodes
        for node_name, node in dag.nodes.items():
            label = self.get_node_label(node)
            attrs = []

            # Set shape based on node type
            if node.node_type == NodeType.CONDITIONAL:
                attrs.append("shape=diamond")
            elif node.node_type == NodeType.SELECT:
                attrs.append("shape=trapezium")
            elif node.node_type in (NodeType.ANY, NodeType.ALL):
                attrs.append("shape=ellipse")
            elif node.node_type == NodeType.DAG:
                attrs.append("shape=box3d")  # 3D box for nested DAG
            elif node.node_type == NodeType.FSM:
                attrs.append("shape=doublecircle")  # Double circle for FSM
            else:
                attrs.append("shape=box")

            # Add label
            attrs.append(f'label="{label}"')

            # Add styling
            if dag.context:
                style_dict = self.get_node_style(node, dag.context)
                for key, value in style_dict.items():
                    attrs.append(f'{key}="{value}"')
            elif self.options.node_color:
                attrs.append(f'fillcolor="{self.options.node_color}"')
                attrs.append('style="filled"')

            attrs_str = ", ".join(attrs)
            lines.append(f'    "{node_name}" [{attrs_str}];')

        # Add edges
        for node_name, node in dag.nodes.items():
            for output_name, connections in node.output_connections.items():
                for target_node, input_name in connections:
                    attrs = []

                    # Add edge label if not default
                    if (
                        output_name != "result" or input_name != "data"
                    ) and self.options.show_inputs:
                        edge_label = f"{output_name} → {input_name}"
                        attrs.append(f'label="{edge_label}"')

                    if self.options.edge_color:
                        attrs.append(f'color="{self.options.edge_color}"')

                    attrs_str = f" [{', '.join(attrs)}]" if attrs else ""
                    lines.append(
                        f'    "{node_name}" -> "{target_node.name}"{attrs_str};'
                    )

        lines.append("}")
        return "\n".join(lines)

    def visualize_fsm(self, fsm: Any) -> str:
        """Generate Graphviz DOT notation for an FSM."""
        lines = ["digraph FSM {"]

        # Set graph attributes
        lines.append(f"    rankdir={self.options.direction};")
        if self.options.font_size:
            lines.append(f"    fontsize={self.options.font_size};")

        # Add invisible start node for initial state
        if fsm.initial_state:
            lines.append('    __start [shape=none, label=""];')
            lines.append(f'    __start -> "{fsm.initial_state}";')

        # Add states
        for node_name, node in fsm.nodes.items():
            label = self.get_node_label(node)
            attrs = []

            # All FSM states are ellipses
            attrs.append("shape=ellipse")

            # Double circle for terminal states
            if node_name in fsm.terminal_states:
                attrs.append("shape=doublecircle")

            # Add label
            attrs.append(f'label="{label}"')

            # Add styling
            if hasattr(fsm, "context") and fsm.context:
                style_dict = self.get_node_style(node, fsm.context)
                for key, value in style_dict.items():
                    attrs.append(f'{key}="{value}"')
            elif self.options.node_color:
                attrs.append(f'fillcolor="{self.options.node_color}"')
                attrs.append('style="filled"')

            attrs_str = ", ".join(attrs)
            lines.append(f'    "{node_name}" [{attrs_str}];')

        # Add transitions from state_transitions
        for from_state, transitions in fsm.state_transitions.items():
            for condition, to_state in transitions.items():
                attrs = []
                if condition != "default":
                    attrs.append(f'label="{condition}"')

                if self.options.edge_color:
                    attrs.append(f'color="{self.options.edge_color}"')

                attrs_str = f" [{', '.join(attrs)}]" if attrs else ""
                lines.append(f'    "{from_state}" -> "{to_state}"{attrs_str};')

        # Add transitions from connections (for FSMs that use connections)
        for node_name, node in fsm.nodes.items():
            for _output_name, connections in node.output_connections.items():
                for target_node, _input_name in connections:
                    # Don't duplicate if already in state_transitions
                    if (
                        node_name not in fsm.state_transitions
                        or target_node.name
                        not in fsm.state_transitions[node_name].values()
                    ):
                        attrs = []

                        if self.options.edge_color:
                            attrs.append(f'color="{self.options.edge_color}"')

                        attrs_str = f" [{', '.join(attrs)}]" if attrs else ""
                        lines.append(
                            f'    "{node_name}" -> "{target_node.name}"{attrs_str};'
                        )

        lines.append("}")
        return "\n".join(lines)

    def save(self, content: str, filename: str, format: str = "png") -> None:
        """Save Graphviz diagram to file."""
        # Save the DOT source
        with open(f"{filename}.dot", "w") as f:
            f.write(content)

        if format != "dot":
            # Try to render using graphviz
            try:
                import subprocess

                subprocess.run(
                    [
                        "dot",
                        f"-T{format}",
                        f"{filename}.dot",
                        "-o",
                        f"{filename}.{format}",
                    ],
                    check=True,
                    capture_output=True,
                )
            except (subprocess.CalledProcessError, FileNotFoundError):
                # Try using the graphviz Python package
                try:
                    import graphviz

                    graph = graphviz.Source(content)
                    graph.render(filename, format=format, cleanup=True)
                except ImportError:
                    print(
                        "Warning: Could not render Graphviz diagram. Install graphviz with 'pip install graphviz' or 'brew install graphviz'"
                    )
                    print(f"DOT source saved to {filename}.dot")
