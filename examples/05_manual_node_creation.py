#!/usr/bin/env python3
"""
Manual Node Creation Example

Demonstrates creating workflows without decorators:
- Manual Node class instantiation
- Different connection methods
- Builder pattern
- Programmatic workflow generation
- Function registry
"""

import argparse
from fast_dag import DAG, Node, NodeType, ConditionalReturn


# Standalone functions (not decorated)
def fetch_data(source: str) -> dict:
    """Fetch data from source"""
    print(f"Fetching from {source}")
    return {"data": [1, 2, 3, 4, 5], "source": source}


def filter_data(data: dict, threshold: int) -> dict:
    """Filter data based on threshold"""
    print(f"Filtering with threshold {threshold}")
    filtered = [x for x in data["data"] if x > threshold]
    return {"filtered": filtered, "count": len(filtered)}


def aggregate_results(filtered: dict) -> dict:
    """Aggregate filtered results"""
    print("Aggregating results")
    data = filtered["filtered"]
    return {
        "sum": sum(data),
        "avg": sum(data) / len(data) if data else 0,
        "count": filtered["count"],
    }


def quality_check(results: dict) -> ConditionalReturn:
    """Check if results meet quality criteria"""
    print("Checking quality")
    passed = results["count"] >= 3
    return ConditionalReturn(condition=passed, value=results)


def report_success(results: dict) -> str:
    """Generate success report"""
    return f"Success! Processed {results['count']} items. Sum: {results['sum']}"


def report_failure(results: dict) -> str:
    """Generate failure report"""
    return f"Failed quality check. Only {results['count']} items passed filter."


def main():
    parser = argparse.ArgumentParser(description="Manual Node Creation Example")
    parser.add_argument(
        "--mode", choices=["manual", "builder", "registry"], default="manual"
    )
    parser.add_argument("--viz", action="store_true", help="Generate visualization")
    args = parser.parse_args()

    if args.mode == "manual":
        dag = create_manual_dag()
    elif args.mode == "builder":
        dag = create_builder_dag()
    else:  # registry
        dag = create_registry_dag()

    # Validate
    print(f"\nValidating {args.mode} workflow...")
    dag.validate_or_raise()
    print("✓ Workflow is valid")

    # Visualize
    if args.viz:
        dag.save_mermaid(f"manual_{args.mode}.mmd")
        print(f"✓ Saved diagram to manual_{args.mode}.mmd")

    # Execute
    print("\nExecuting workflow...")
    result = dag.run(source="database", threshold=2)
    print(f"\nResult: {result}")


def create_manual_dag() -> DAG:
    """Create DAG using manual node instantiation"""
    print("Creating DAG with manual node instantiation...")

    # Create workflow
    dag = DAG("manual_workflow")

    # Create nodes manually
    fetch_node = Node(
        func=fetch_data,
        name="fetch",
        inputs=["source"],
        outputs=["data"],
        description="Fetch data from source",
    )

    filter_node = Node(
        func=filter_data,
        name="filter",
        inputs=["data", "threshold"],
        outputs=["filtered"],
    )

    aggregate_node = Node(
        func=aggregate_results,
        name="aggregate",
        inputs=["filtered"],
        outputs=["results"],
    )

    # Conditional node
    check_node = Node(
        func=quality_check,
        name="check",
        inputs=["results"],
        outputs=["results"],
        node_type=NodeType.CONDITIONAL,
    )

    success_node = Node(
        func=report_success, name="success", inputs=["results"], outputs=["report"]
    )

    failure_node = Node(
        func=report_failure, name="failure", inputs=["results"], outputs=["report"]
    )

    # Add nodes to DAG
    dag.add_node(fetch_node)
    dag.add_node(filter_node)
    dag.add_node(aggregate_node)
    dag.add_node(check_node)
    dag.add_node(success_node)
    dag.add_node(failure_node)

    # Connect using explicit method
    dag.connect("fetch", "filter", output="data", input="data")
    dag.connect("filter", "aggregate", output="filtered", input="filtered")
    dag.connect("aggregate", "check", output="results", input="results")

    # Connect conditional branches
    dag.connect("check", "success", output="true", input="results")
    dag.connect("check", "failure", output="false", input="results")

    return dag


def create_builder_dag() -> DAG:
    """Create DAG using builder pattern and method chaining"""
    print("Creating DAG with builder pattern...")

    # Start with empty DAG
    dag = DAG("builder_workflow")

    # Use builder pattern with method chaining
    dag = (
        dag.add_node("fetch", fetch_data)
        .add_node("filter", filter_data)
        .add_node("aggregate", aggregate_results)
        .add_node("check", quality_check, node_type=NodeType.CONDITIONAL)
        .add_node("success", report_success)
        .add_node("failure", report_failure)
    )

    # Connect using method chaining and pipe operator
    # Method 1: Using connect_to
    dag.get_node("fetch").connect_to(dag.get_node("filter"))

    # Method 2: Using pipe operator
    dag.get_node("filter") | dag.get_node("aggregate") | dag.get_node("check")

    # Connect conditional outputs
    dag.get_node("check").on_true.connect_to(dag.get_node("success"))
    dag.get_node("check").on_false.connect_to(dag.get_node("failure"))

    return dag


def create_registry_dag() -> DAG:
    """Create DAG using function registry pattern"""
    print("Creating DAG with function registry...")

    from fast_dag import FunctionRegistry

    # Create registry
    registry = FunctionRegistry()

    # Register functions
    registry.register(fetch_data, "data_fetcher")
    registry.register(filter_data, "data_filter")
    registry.register(aggregate_results, "aggregator")
    registry.register(quality_check, "quality_checker")
    registry.register(report_success, "success_reporter")
    registry.register(report_failure, "failure_reporter")

    # Create DAG programmatically
    dag = DAG("registry_workflow")

    # Define workflow structure
    workflow_def = [
        {"name": "fetch", "func": "data_fetcher", "outputs": ["data"]},
        {"name": "filter", "func": "data_filter", "inputs": ["data", "threshold"]},
        {"name": "aggregate", "func": "aggregator"},
        {"name": "check", "func": "quality_checker", "type": "conditional"},
        {"name": "success", "func": "success_reporter"},
        {"name": "failure", "func": "failure_reporter"},
    ]

    # Create nodes from definitions
    for node_def in workflow_def:
        func = registry.get(node_def["func"])
        node_type = (
            NodeType.CONDITIONAL
            if node_def.get("type") == "conditional"
            else NodeType.STANDARD
        )

        node = Node(
            func=func,
            name=node_def["name"],
            inputs=node_def.get("inputs"),
            outputs=node_def.get("outputs"),
            node_type=node_type,
        )
        dag.add_node(node)

    # Define connections
    connections = [
        ("fetch", "filter"),
        ("filter", "aggregate"),
        ("aggregate", "check"),
        ("check", "success", "true"),
        ("check", "failure", "false"),
    ]

    # Apply connections
    for conn in connections:
        if len(conn) == 3:  # Conditional connection
            from_node, to_node, output = conn
            dag.connect(from_node, to_node, output=output)
        else:
            from_node, to_node = conn
            dag.connect(from_node, to_node)

    return dag


if __name__ == "__main__":
    main()
