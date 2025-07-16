#!/usr/bin/env python3
"""
Invalid DAG Example

Demonstrates various validation errors:
- Cyclic dependencies
- Missing connections
- Type mismatches
- Undefined nodes
"""

import argparse

from fast_dag import DAG, ConditionalReturn


def main():
    parser = argparse.ArgumentParser(description="Invalid DAG Examples")
    parser.add_argument(
        "--example",
        type=int,
        choices=[1, 2, 3, 4],
        default=1,
        help="Which invalid example to run",
    )
    args = parser.parse_args()

    if args.example == 1:
        example_cyclic_dependency()
    elif args.example == 2:
        example_disconnected_nodes()
    elif args.example == 3:
        example_type_mismatch()
    elif args.example == 4:
        example_missing_connections()


def example_cyclic_dependency():
    """Example 1: DAG with cycles"""
    print("Example 1: Cyclic Dependency")
    print("=" * 50)

    dag = DAG("cyclic_workflow")

    @dag.node
    def step_a(data: str) -> str:
        return f"A-{data}"

    @dag.node
    def step_b(data: str) -> str:
        return f"B-{data}"

    @dag.node
    def step_c(data: str) -> str:
        return f"C-{data}"

    # Create a cycle: A -> B -> C -> A
    step_a >> step_b >> step_c >> step_a

    print("Created cycle: A -> B -> C -> A")

    # Validate
    print("\nValidating...")
    errors = dag.validate()

    if errors:
        print("❌ Validation failed with errors:")
        for error in errors:
            print(f"  - {error}")

    # Try specific validations
    print(f"\nIs acyclic? {dag.is_acyclic()}")

    # Try to execute (will fail)
    try:
        dag.validate_or_raise()
        dag.run(data="test")
    except Exception as e:
        print(f"\n❌ Execution failed: {e}")


def example_disconnected_nodes():
    """Example 2: Disconnected graph"""
    print("\nExample 2: Disconnected Nodes")
    print("=" * 50)

    dag = DAG("disconnected_workflow")

    @dag.node
    def input_node(data: str) -> dict:
        return {"value": data}

    @dag.node
    def process_a(value: dict) -> str:
        return value["value"].upper()

    @dag.node
    def process_b(text: str) -> int:
        return len(text)

    @dag.node
    def orphan_node(number: int) -> str:
        """This node is not connected to anything"""
        return f"Result: {number}"

    # Only connect some nodes
    input_node >> process_a >> process_b
    # orphan_node is not connected!

    print("Graph structure:")
    print("  input_node -> process_a -> process_b")
    print("  orphan_node (disconnected)")

    # Validate
    print("\nValidating...")
    errors = dag.validate()

    if errors:
        print("❌ Validation failed with errors:")
        for error in errors:
            print(f"  - {error}")

    print(f"\nIs fully connected? {dag.is_fully_connected()}")
    print(f"Has entry points? {dag.has_entry_points()}")


def example_type_mismatch():
    """Example 3: Type mismatches between nodes"""
    print("\nExample 3: Type Mismatch")
    print("=" * 50)

    dag = DAG("type_mismatch_workflow")

    @dag.node(outputs=["number"])
    def generate_number(seed: int) -> int:
        """Outputs an integer"""
        return seed * 42

    @dag.node(inputs=["text"])
    def process_string(text: str) -> str:
        """Expects a string input"""
        return text.upper()

    @dag.node
    def final_step(result: str) -> dict:
        return {"final": result}

    # Connect incompatible types
    # generate_number outputs int, but process_string expects str
    generate_number >> process_string >> final_step

    print("Type mismatch:")
    print("  generate_number (outputs: int) -> process_string (expects: str)")

    # Validate
    print("\nValidating...")
    errors = dag.validate()

    if errors:
        print("❌ Validation failed with errors:")
        for error in errors:
            print(f"  - {error}")

    # Check specific connection
    can_connect = dag.can_connect("generate_number", "process_string")
    print(f"\nCan connect generate_number to process_string? {can_connect}")

    issues = dag.check_connection("generate_number", "process_string")
    if issues:
        print("Connection issues:")
        for issue in issues:
            print(f"  - {issue}")


def example_missing_connections():
    """Example 4: Conditional node with missing branches"""
    print("\nExample 4: Missing Conditional Branches")
    print("=" * 50)

    dag = DAG("incomplete_conditional")

    @dag.node
    def start(value: int) -> int:
        return value

    @dag.condition()
    def check_value(value: int) -> ConditionalReturn:
        """Conditional node"""
        return ConditionalReturn(condition=value > 0, value=value)

    @dag.node
    def handle_positive(value: int) -> str:
        return f"Positive: {value}"

    @dag.node
    def handle_negative(value: int) -> str:
        return f"Negative: {value}"

    @dag.node
    def finish(result: str) -> str:
        return f"Done: {result}"

    # Connect nodes but forget one branch
    start >> check_value
    check_value.on_true >> handle_positive >> finish
    # Forgot to connect: check_value.on_false >> handle_negative

    print("Missing connection:")
    print("  check_value.on_false is not connected!")

    # Validate
    print("\nValidating...")
    errors = dag.validate()

    if errors:
        print("❌ Validation failed with errors:")
        for error in errors:
            print(f"  - {error}")

    # Try to visualize (will show the missing connection)
    print("\nGenerating visualization to see missing connection...")
    dag.save_mermaid("invalid_conditional.mmd")
    print("Check invalid_conditional.mmd to see the incomplete graph")


if __name__ == "__main__":
    main()
