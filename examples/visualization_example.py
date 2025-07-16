"""Example demonstrating visualization capabilities."""

from fast_dag import DAG, FSM
from fast_dag.core.types import FSMReturn


def create_sample_dag():
    """Create a sample DAG for visualization."""
    dag = DAG("data_pipeline")

    @dag.node
    def fetch_data() -> dict:
        """Fetch data from source."""
        return {"items": [1, 2, 3, 4, 5]}

    @dag.node
    def validate(data: dict) -> dict:
        """Validate input data."""
        if not data.get("items"):
            raise ValueError("No items found")
        return data

    @dag.condition
    def has_enough_data(data: dict) -> bool:
        """Check if we have enough data."""
        return len(data.get("items", [])) >= 3

    @dag.node
    def process_large(data: dict) -> int:
        """Process large dataset."""
        return sum(data["items"]) * 2

    @dag.node
    def process_small(data: dict) -> int:
        """Process small dataset."""
        return sum(data["items"])

    @dag.node
    def save_result(result: int) -> str:
        """Save the result."""
        return f"Result saved: {result}"

    # Connect the nodes
    dag.connect("fetch_data", "validate", input="data")
    dag.connect("validate", "has_enough_data", input="data")

    # Conditional branches
    dag.nodes["has_enough_data"].on_true >> dag.nodes["process_large"]
    dag.nodes["has_enough_data"].on_false >> dag.nodes["process_small"]

    # Both branches lead to save
    dag.connect("process_large", "save_result", input="result")
    dag.connect("process_small", "save_result", input="result")

    return dag


def create_sample_fsm():
    """Create a sample FSM for visualization."""
    fsm = FSM("traffic_light")

    @fsm.state(initial=True)
    def red() -> FSMReturn:
        """Red light - stop."""
        return FSMReturn(next_state="green", value="STOP")

    @fsm.state
    def green() -> FSMReturn:
        """Green light - go."""
        return FSMReturn(next_state="yellow", value="GO")

    @fsm.state
    def yellow() -> FSMReturn:
        """Yellow light - caution."""
        return FSMReturn(next_state="red", value="CAUTION")

    @fsm.state(terminal=True)
    def off() -> FSMReturn:
        """Light is off."""
        return FSMReturn(stop=True, value="OFF")

    # Add manual transition to turn off
    fsm.add_transition("red", "off", condition="power_off")
    fsm.add_transition("green", "off", condition="power_off")
    fsm.add_transition("yellow", "off", condition="power_off")

    return fsm


def main():
    """Run visualization examples."""
    # Example 1: Visualize DAG in Mermaid format
    print("=== DAG Visualization (Mermaid) ===")
    dag = create_sample_dag()
    mermaid_code = dag.visualize(backend="mermaid")
    print(mermaid_code)
    print()

    # Example 2: Visualize DAG in Graphviz format
    print("=== DAG Visualization (Graphviz) ===")
    graphviz_code = dag.visualize(backend="graphviz")
    print(graphviz_code)
    print()

    # Example 3: Visualize with execution results
    print("=== DAG with Results ===")
    dag.run()

    try:
        from fast_dag.visualization import VisualizationOptions

        options = VisualizationOptions(
            show_results=True, direction="LR", show_description=True
        )
        result_viz = dag.visualize(options=options)
        print(result_viz)
    except ImportError:
        print("Visualization extras not installed. Run: pip install fast-dag[viz]")
    print()

    # Example 4: Visualize FSM
    print("=== FSM Visualization (Mermaid) ===")
    fsm = create_sample_fsm()
    fsm_viz = fsm.visualize(backend="mermaid")
    print(fsm_viz)

    # Example 5: Save visualizations to files
    print("\n=== Saving Visualizations ===")
    # These will save as .mmd and .dot files
    # If tools are installed, they'll also render to PNG
    dag.visualize(filename="dag_example", format="png")
    print("Saved DAG visualization to dag_example.mmd")

    fsm.visualize(backend="graphviz", filename="fsm_example", format="svg")
    print("Saved FSM visualization to fsm_example.dot")


if __name__ == "__main__":
    main()
