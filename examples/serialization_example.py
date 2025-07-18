"""Example demonstrating serialization capabilities."""

from fast_dag import DAG, FSM
from fast_dag.core.types import FSMReturn
from fast_dag.serialization import FunctionRegistry


# Define some workflow functions
def fetch_data(source: str) -> dict:
    """Fetch data from a source."""
    print(f"Fetching data from {source}")
    return {"source": source, "items": [1, 2, 3, 4, 5]}


def validate_data(data: dict) -> dict:
    """Validate the fetched data."""
    if not data.get("items"):
        raise ValueError("No items in data")
    print(f"Validated {len(data['items'])} items")
    return data


def transform_data(data: dict) -> list:
    """Transform the data."""
    items = data["items"]
    transformed = [x * 2 for x in items]
    print(f"Transformed {len(transformed)} items")
    return transformed


def save_results(items: list, destination: str) -> str:
    """Save the results."""
    print(f"Saving {len(items)} items to {destination}")
    return f"Saved {len(items)} items to {destination}"


# FSM state functions
def idle_state() -> FSMReturn:
    """Idle state - waiting for job."""
    print("System idle, waiting for job...")
    return FSMReturn(next_state="processing", value={"status": "idle"})


def processing_state(job_id: str | None = None) -> FSMReturn:
    """Processing state - working on job."""
    print(f"Processing job: {job_id or 'unknown'}")
    # Simulate processing
    result = {"job_id": job_id, "result": "completed"}
    return FSMReturn(next_state="complete", value=result)


def complete_state(result: dict) -> FSMReturn:
    """Complete state - job done."""
    print(f"Job complete: {result}")
    return FSMReturn(next_state="idle", value={"status": "complete", "result": result})


def error_state(error: str) -> FSMReturn:
    """Error state - something went wrong."""
    print(f"Error occurred: {error}")
    return FSMReturn(next_state="idle", value={"status": "error", "message": error})


def create_etl_pipeline():
    """Create an ETL pipeline DAG."""
    dag = DAG("etl_pipeline", description="Extract, Transform, Load pipeline")

    # Add nodes
    dag.add_node("fetch", fetch_data)
    dag.add_node("validate", validate_data)
    dag.add_node("transform", transform_data)
    dag.add_node("save", save_results)

    # Connect nodes
    dag.connect("fetch", "validate", input="data")
    dag.connect("validate", "transform", input="data")
    dag.connect("transform", "save", input="items")

    return dag


def create_job_processor_fsm():
    """Create a job processor FSM."""
    fsm = FSM("job_processor", description="State machine for processing jobs")

    # Register states
    fsm.add_node("idle", idle_state)
    fsm.add_node("processing", processing_state)
    fsm.add_node("complete", complete_state)
    fsm.add_node("error", error_state)

    # Set initial state
    fsm.initial_state = "idle"

    # Add error transitions
    fsm.add_transition("processing", "error", "on_error")
    fsm.add_transition("error", "idle", "reset")

    return fsm


def main():
    """Run serialization examples."""
    # Register all functions for serialization
    print("=== Registering Functions ===")
    FunctionRegistry.register(fetch_data)
    FunctionRegistry.register(validate_data)
    FunctionRegistry.register(transform_data)
    FunctionRegistry.register(save_results)
    FunctionRegistry.register(idle_state)
    FunctionRegistry.register(processing_state)
    FunctionRegistry.register(complete_state)
    FunctionRegistry.register(error_state)

    # Example 1: Save and load DAG
    print("\n=== DAG Serialization Example ===")
    dag = create_etl_pipeline()

    # Save to different formats
    print("\nSaving DAG to different formats...")
    dag.save("etl_pipeline.json")
    print("Saved to etl_pipeline.json")

    try:
        dag.save("etl_pipeline.yaml", format="yaml")
        print("Saved to etl_pipeline.yaml")
    except ImportError:
        print("YAML support not installed. Run: pip install fast-dag[serialize]")

    try:
        dag.save("etl_pipeline.msgpack", format="msgpack")
        print("Saved to etl_pipeline.msgpack")
    except ImportError:
        print("msgpack support not installed. Run: pip install fast-dag[serialize]")

    # Load and execute
    print("\nLoading DAG from file...")
    loaded_dag = DAG.load("etl_pipeline.json")
    print(f"Loaded DAG: {loaded_dag.name}")
    print(f"Nodes: {list(loaded_dag.nodes.keys())}")

    # Execute the loaded DAG
    print("\nExecuting loaded DAG...")
    result = loaded_dag.run(source="database", destination="warehouse")
    print(f"Result: {result}")

    # Example 2: Save and load FSM
    print("\n\n=== FSM Serialization Example ===")
    fsm = create_job_processor_fsm()

    # Save FSM
    print("\nSaving FSM...")
    fsm.save("job_processor.json")
    print("Saved to job_processor.json")

    # Load FSM
    print("\nLoading FSM from file...")
    loaded_fsm = FSM.load("job_processor.json")
    print(f"Loaded FSM: {loaded_fsm.name}")
    print(f"Initial state: {loaded_fsm.initial_state}")
    print(f"States: {list(loaded_fsm.nodes.keys())}")

    # Run a few cycles
    print("\nRunning FSM for 3 cycles...")
    loaded_fsm.max_cycles = 3
    result = loaded_fsm.run(job_id="job-123")
    print(f"Final result: {result}")

    # Example 3: Demonstrate function registry
    print("\n\n=== Function Registry Example ===")
    print(f"Registered functions: {len(FunctionRegistry.list_functions())}")
    for func_name in sorted(FunctionRegistry.list_functions())[:5]:
        print(f"  - {func_name}")

    # Clean up
    import os

    for file in [
        "etl_pipeline.json",
        "etl_pipeline.yaml",
        "etl_pipeline.msgpack",
        "job_processor.json",
    ]:
        if os.path.exists(file):
            os.remove(file)
            print(f"\nCleaned up {file}")


if __name__ == "__main__":
    main()
