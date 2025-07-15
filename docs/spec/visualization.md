# Visualization

This document specifies the visualization capabilities of fast-dag, supporting both Mermaid and Graphviz output formats for workflow diagrams.

## Design Principles

1. **Multiple Formats**: Support popular diagram formats
2. **Execution Awareness**: Show runtime results and metrics
3. **Customizable**: Flexible styling and layout options
4. **Interactive**: Support for clickable, zoomable diagrams
5. **Export Ready**: High-quality output for documentation

## Architecture

### Visualization Protocol

```python
from typing import Protocol, Any
from dataclasses import dataclass

@dataclass
class NodeStyle:
    """Styling options for nodes"""
    shape: str = "box"
    color: str | None = None
    fill_color: str | None = None
    border_width: int = 1
    font_size: int = 12
    font_color: str = "black"

@dataclass
class EdgeStyle:
    """Styling options for edges"""
    style: str = "solid"  # solid, dashed, dotted
    color: str = "black"
    width: int = 1
    arrow_type: str = "normal"
    label: str | None = None

class Visualizer(Protocol):
    """Protocol for visualization implementations"""
    
    def render_dag(
        self,
        dag: DAG,
        node_styles: dict[str, NodeStyle] | None = None,
        edge_styles: dict[tuple[str, str], EdgeStyle] | None = None,
        layout: str = "auto"
    ) -> str:
        """Render DAG to diagram format"""
        ...
    
    def render_with_results(
        self,
        dag: DAG,
        context: Context,
        show_values: bool = True,
        show_timing: bool = True
    ) -> str:
        """Render DAG with execution results"""
        ...
```

## Mermaid Implementation

### Basic Mermaid Renderer

```python
class MermaidRenderer:
    """Generate Mermaid diagrams"""
    
    def render_dag(
        self,
        dag: DAG,
        node_styles: dict[str, NodeStyle] | None = None,
        edge_styles: dict[tuple[str, str], EdgeStyle] | None = None,
        layout: str = "TB"  # Top-Bottom
    ) -> str:
        """Generate Mermaid flowchart"""
        lines = [f"flowchart {layout}"]
        
        # Add nodes
        for name, node in dag.nodes.items():
            style = node_styles.get(name) if node_styles else None
            node_def = self._format_node(name, node, style)
            lines.append(f"    {node_def}")
        
        # Add edges
        for from_node, to_nodes in self._get_connections(dag):
            for to_node in to_nodes:
                edge_style = edge_styles.get((from_node, to_node)) if edge_styles else None
                edge_def = self._format_edge(from_node, to_node, edge_style)
                lines.append(f"    {edge_def}")
        
        return "\n".join(lines)
    
    def _format_node(self, name: str, node: Node, style: NodeStyle | None) -> str:
        """Format node definition"""
        # Escape special characters
        label = node.description or name
        label = label.replace('"', '\\"')
        
        # Determine shape
        if style and style.shape == "diamond":
            return f'{name}{{{{{label}}}}}'
        elif style and style.shape == "circle":
            return f'{name}(({label}))'
        else:
            return f'{name}["{label}"]'
    
    def _format_edge(
        self,
        from_node: str,
        to_node: str,
        style: EdgeStyle | None
    ) -> str:
        """Format edge definition"""
        if style and style.style == "dashed":
            arrow = "-.->
        elif style and style.style == "dotted":
            arrow = "-..->
        else:
            arrow = "-->"
        
        edge = f"{from_node} {arrow} {to_node}"
        
        if style and style.label:
            edge = f'{from_node} {arrow}|{style.label}| {to_node}'
        
        return edge
```

### Mermaid with Results

```python
class MermaidResultRenderer(MermaidRenderer):
    """Render with execution results"""
    
    def render_with_results(
        self,
        dag: DAG,
        context: Context,
        metrics: ExecutionMetrics | None = None,
        show_values: bool = True,
        show_timing: bool = True
    ) -> str:
        """Render DAG with execution results"""
        lines = ["flowchart TB"]
        
        # Add nodes with results
        for name, node in dag.nodes.items():
            lines.append(self._format_node_with_result(
                name, node, context, metrics, show_values, show_timing
            ))
        
        # Add edges
        for from_node, to_nodes in self._get_connections(dag):
            for to_node in to_nodes:
                lines.append(f"    {from_node} --> {to_node}")
        
        # Add legend
        if show_timing or show_values:
            lines.extend(self._generate_legend())
        
        # Add styles
        lines.extend(self._generate_styles(context, metrics))
        
        return "\n".join(lines)
    
    def _format_node_with_result(
        self,
        name: str,
        node: Node,
        context: Context,
        metrics: ExecutionMetrics | None,
        show_values: bool,
        show_timing: bool
    ) -> str:
        """Format node with execution info"""
        parts = [name]
        
        if show_values and name in context.results:
            value = str(context.results[name])[:50]
            parts.append(f"Result: {value}")
        
        if show_timing and metrics and name in metrics.node_times:
            time = metrics.node_times[name]
            parts.append(f"Time: {time:.2f}s")
        
        label = "<br/>".join(parts)
        
        # Determine style based on execution
        if name in context.results:
            if isinstance(context.results[name], Exception):
                return f'    {name}["{label}"]:::error'
            else:
                return f'    {name}["{label}"]:::success'
        else:
            return f'    {name}["{label}"]:::pending'
    
    def _generate_styles(self, context: Context, metrics: ExecutionMetrics | None) -> list[str]:
        """Generate CSS styles for states"""
        return [
            "",
            "    classDef success fill:#90EE90,stroke:#006400,stroke-width:2px;",
            "    classDef error fill:#FFB6C1,stroke:#8B0000,stroke-width:2px;",
            "    classDef pending fill:#D3D3D3,stroke:#696969,stroke-width:1px;",
            "    classDef running fill:#87CEEB,stroke:#0000CD,stroke-width:3px;"
        ]
```

## Graphviz Implementation

### Basic Graphviz Renderer

```python
class GraphvizRenderer:
    """Generate Graphviz DOT diagrams"""
    
    def __init__(self):
        self.graph_attrs = {
            "rankdir": "TB",
            "bgcolor": "white",
            "splines": "ortho"
        }
        self.node_attrs = {
            "shape": "box",
            "style": "rounded,filled",
            "fillcolor": "lightblue",
            "fontname": "Arial"
        }
        self.edge_attrs = {
            "arrowhead": "vee",
            "arrowsize": "0.8"
        }
    
    def render_dag(
        self,
        dag: DAG,
        node_styles: dict[str, NodeStyle] | None = None,
        edge_styles: dict[tuple[str, str], EdgeStyle] | None = None,
        layout: str = "dot"
    ) -> str:
        """Generate DOT format"""
        lines = ["digraph G {"]
        
        # Graph attributes
        for key, value in self.graph_attrs.items():
            lines.append(f'    {key}="{value}";')
        
        # Default node attributes
        node_attr_str = ", ".join(f'{k}="{v}"' for k, v in self.node_attrs.items())
        lines.append(f"    node [{node_attr_str}];")
        
        # Add nodes
        for name, node in dag.nodes.items():
            style = node_styles.get(name) if node_styles else None
            lines.append(self._format_node(name, node, style))
        
        # Add edges
        for from_node, connections in self._get_all_connections(dag):
            for to_node, output, input in connections:
                style = edge_styles.get((from_node, to_node)) if edge_styles else None
                lines.append(self._format_edge(from_node, to_node, output, input, style))
        
        lines.append("}")
        return "\n".join(lines)
    
    def _format_node(self, name: str, node: Node, style: NodeStyle | None) -> str:
        """Format node in DOT syntax"""
        attrs = []
        
        # Label
        label = node.description or name
        attrs.append(f'label="{label}"')
        
        # Apply custom style
        if style:
            if style.shape:
                attrs.append(f'shape="{style.shape}"')
            if style.fill_color:
                attrs.append(f'fillcolor="{style.fill_color}"')
            if style.color:
                attrs.append(f'color="{style.color}"')
            if style.font_size:
                attrs.append(f'fontsize="{style.font_size}"')
        
        attr_str = ", ".join(attrs)
        return f'    "{name}" [{attr_str}];'
```

### Advanced Graphviz Features

```python
class GraphvizAdvancedRenderer(GraphvizRenderer):
    """Advanced Graphviz features"""
    
    def render_hierarchical(self, dag: DAG) -> str:
        """Render with hierarchical layout"""
        lines = ["digraph G {"]
        lines.append('    rankdir="LR";')
        lines.append('    ranksep="1.0";')
        
        # Group nodes by level
        levels = self._compute_levels(dag)
        
        for level, nodes in enumerate(levels):
            lines.append(f"    subgraph level_{level} {{")
            lines.append(f'        rank="same";')
            for node in nodes:
                lines.append(f'        "{node}";')
            lines.append("    }")
        
        # Add all nodes and edges
        for name, node in dag.nodes.items():
            lines.append(self._format_node(name, node, None))
        
        # Add edges
        for from_node, to_nodes in self._get_connections(dag):
            for to_node in to_nodes:
                lines.append(f'    "{from_node}" -> "{to_node}";')
        
        lines.append("}")
        return "\n".join(lines)
    
    def render_with_subgraphs(self, dag: DAG, groups: dict[str, list[str]]) -> str:
        """Render with node groupings"""
        lines = ["digraph G {"]
        lines.append('    compound=true;')
        
        # Create subgraphs
        for group_name, node_names in groups.items():
            lines.append(f'    subgraph "cluster_{group_name}" {{')
            lines.append(f'        label="{group_name}";')
            lines.append('        style="rounded,filled";')
            lines.append('        fillcolor="lightgray";')
            
            for node_name in node_names:
                if node_name in dag.nodes:
                    node = dag.nodes[node_name]
                    lines.append(self._format_node(node_name, node, None))
            
            lines.append("    }")
        
        # Add ungrouped nodes
        grouped_nodes = set(sum(groups.values(), []))
        for name, node in dag.nodes.items():
            if name not in grouped_nodes:
                lines.append(self._format_node(name, node, None))
        
        # Add edges
        for from_node, to_nodes in self._get_connections(dag):
            for to_node in to_nodes:
                lines.append(f'    "{from_node}" -> "{to_node}";')
        
        lines.append("}")
        return "\n".join(lines)
```

## Export and Integration

### File Export

```python
class DiagramExporter:
    """Export diagrams to various formats"""
    
    def export_mermaid(self, dag: DAG, filepath: str) -> None:
        """Export Mermaid diagram"""
        renderer = MermaidRenderer()
        content = renderer.render_dag(dag)
        
        with open(filepath, "w") as f:
            f.write(content)
    
    def export_graphviz(
        self,
        dag: DAG,
        filepath: str,
        format: str = "png",
        engine: str = "dot"
    ) -> None:
        """Export Graphviz diagram to image"""
        import graphviz
        
        renderer = GraphvizRenderer()
        dot_content = renderer.render_dag(dag)
        
        # Create graph
        graph = graphviz.Source(dot_content, engine=engine)
        
        # Render to file
        output_path = filepath.rsplit(".", 1)[0]  # Remove extension
        graph.render(output_path, format=format, cleanup=True)
    
    def export_html(self, dag: DAG, filepath: str) -> None:
        """Export interactive HTML with Mermaid"""
        renderer = MermaidRenderer()
        mermaid_content = renderer.render_dag(dag)
        
        html_template = '''<!DOCTYPE html>
<html>
<head>
    <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
    <script>mermaid.initialize({startOnLoad:true});</script>
</head>
<body>
    <div class="mermaid">
{content}
    </div>
</body>
</html>'''
        
        html_content = html_template.format(content=mermaid_content)
        
        with open(filepath, "w") as f:
            f.write(html_content)
```

### Jupyter Integration

```python
class JupyterVisualizer:
    """Visualization for Jupyter notebooks"""
    
    @staticmethod
    def display_mermaid(dag: DAG) -> None:
        """Display Mermaid diagram in Jupyter"""
        from IPython.display import display, Markdown
        
        renderer = MermaidRenderer()
        content = renderer.render_dag(dag)
        
        # Wrap in Markdown with mermaid code block
        markdown_content = f"```mermaid\n{content}\n```"
        display(Markdown(markdown_content))
    
    @staticmethod
    def display_graphviz(dag: DAG) -> None:
        """Display Graphviz diagram in Jupyter"""
        import graphviz
        from IPython.display import display
        
        renderer = GraphvizRenderer()
        dot_content = renderer.render_dag(dag)
        
        graph = graphviz.Source(dot_content)
        display(graph)
```

## Styling and Themes

### Pre-defined Themes

```python
class VisualizationTheme:
    """Pre-defined visualization themes"""
    
    LIGHT = {
        "node_styles": {
            "default": NodeStyle(
                fill_color="lightblue",
                border_width=1,
                font_color="black"
            ),
            "conditional": NodeStyle(
                shape="diamond",
                fill_color="lightyellow"
            ),
            "error": NodeStyle(
                fill_color="lightcoral",
                border_width=2
            )
        },
        "edge_styles": {
            "default": EdgeStyle(color="gray"),
            "error_path": EdgeStyle(color="red", style="dashed")
        }
    }
    
    DARK = {
        "node_styles": {
            "default": NodeStyle(
                fill_color="#2c3e50",
                color="#ecf0f1",
                font_color="#ecf0f1"
            ),
            "conditional": NodeStyle(
                shape="diamond",
                fill_color="#f39c12",
                font_color="white"
            )
        },
        "edge_styles": {
            "default": EdgeStyle(color="#7f8c8d"),
        }
    }
    
    COLORBLIND_SAFE = {
        "node_styles": {
            "default": NodeStyle(fill_color="#1f77b4"),
            "success": NodeStyle(fill_color="#2ca02c"),
            "error": NodeStyle(fill_color="#d62728"),
            "warning": NodeStyle(fill_color="#ff7f0e")
        }
    }
```

### Custom Styling

```python
def style_by_performance(dag: DAG, metrics: ExecutionMetrics) -> dict[str, NodeStyle]:
    """Style nodes based on execution time"""
    styles = {}
    
    if not metrics.node_times:
        return styles
    
    # Find slowest nodes
    times = list(metrics.node_times.values())
    p90 = sorted(times)[int(len(times) * 0.9)]
    
    for node, time in metrics.node_times.items():
        if time >= p90:
            # Highlight slow nodes
            styles[node] = NodeStyle(
                fill_color="orange",
                border_width=3
            )
        else:
            styles[node] = NodeStyle(fill_color="lightgreen")
    
    return styles
```

## FSM Visualization

### State Machine Diagrams

```python
class FSMVisualizer:
    """Specialized FSM visualization"""
    
    def render_state_diagram(self, fsm: FSM) -> str:
        """Render as state diagram"""
        lines = ["stateDiagram-v2"]
        
        # Mark initial state
        if fsm.initial_state:
            lines.append(f"    [*] --> {fsm.initial_state}")
        
        # Add state transitions
        for from_state, transitions in fsm.state_transitions.items():
            for condition, to_state in transitions.items():
                label = condition if condition != "default" else ""
                if label:
                    lines.append(f"    {from_state} --> {to_state} : {label}")
                else:
                    lines.append(f"    {from_state} --> {to_state}")
        
        # Mark terminal states
        for terminal in fsm.terminal_states:
            lines.append(f"    {terminal} --> [*]")
        
        return "\n".join(lines)
    
    def render_execution_trace(
        self,
        fsm: FSM,
        context: FSMContext
    ) -> str:
        """Render execution history"""
        lines = ["sequenceDiagram"]
        lines.append("    participant FSM")
        
        for i, state in enumerate(context.state_history):
            lines.append(f"    Note over FSM: Cycle {i+1}")
            lines.append(f"    FSM->>FSM: Execute {state}")
            
            if state in context.cycle_results:
                result = context.cycle_results[state][i]
                lines.append(f"    Note right of FSM: Result: {str(result)[:30]}")
        
        return "\n".join(lines)
```

## Performance

1. **Lazy Rendering**: Only render when requested
2. **Caching**: Cache rendered diagrams
3. **Incremental Updates**: Update only changed nodes
4. **Async Export**: Non-blocking image generation
5. **Streaming**: Support large graph streaming