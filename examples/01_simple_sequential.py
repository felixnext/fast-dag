#!/usr/bin/env python3
"""
Simple Sequential DAG Example

A basic data processing pipeline that demonstrates:
- Decorator-based node creation
- Sequential connections
- Validation
- Visualization
- Different execution modes
"""

import argparse
import asyncio
from fast_dag import DAG, DAGRunner


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Simple Sequential DAG Example")
    parser.add_argument(
        "--mode",
        choices=["sync", "async", "parallel"],
        default="sync",
        help="Execution mode",
    )
    parser.add_argument("--viz", action="store_true", help="Generate visualization")
    args = parser.parse_args()

    # Create workflow
    pipeline = DAG("data_processing_pipeline")

    # Define nodes with decorators
    @pipeline.node
    def load_data(filename: str) -> dict:
        """Load data from file"""
        print(f"Loading data from {filename}")
        # Simulate data loading
        return {
            "records": [
                {"id": 1, "value": 100},
                {"id": 2, "value": 200},
                {"id": 3, "value": 300},
            ],
            "source": filename,
        }

    @pipeline.node
    def validate_data(data: dict) -> dict:
        """Validate data integrity"""
        print("Validating data...")
        records = data["records"]

        # Check for required fields
        valid_records = []
        for record in records:
            if "id" in record and "value" in record:
                valid_records.append(record)

        return {
            "valid_records": valid_records,
            "invalid_count": len(records) - len(valid_records),
            "source": data["source"],
        }

    @pipeline.node
    def transform_data(validated: dict) -> dict:
        """Transform and enrich data"""
        print("Transforming data...")
        records = validated["valid_records"]

        # Apply transformations
        transformed = []
        for record in records:
            transformed.append(
                {
                    "id": record["id"],
                    "value": record["value"],
                    "doubled": record["value"] * 2,
                    "category": "high" if record["value"] > 150 else "low",
                }
            )

        return {"transformed": transformed, "count": len(transformed)}

    @pipeline.node
    def generate_report(transformed: dict) -> str:
        """Generate summary report"""
        print("Generating report...")
        records = transformed["transformed"]

        total_value = sum(r["value"] for r in records)
        high_count = sum(1 for r in records if r["category"] == "high")

        report = f"""
Data Processing Report
=====================
Total Records: {transformed["count"]}
Total Value: {total_value}
High Value Records: {high_count}
Low Value Records: {transformed["count"] - high_count}
"""
        return report

    # Connect nodes sequentially
    load_data >> validate_data >> transform_data >> generate_report

    # Validate the workflow
    print("Validating workflow...")
    errors = pipeline.validate()
    if errors:
        print("Validation errors:")
        for error in errors:
            print(f"  - {error}")
        return
    print("✓ Workflow is valid")

    # Visualize if requested
    if args.viz:
        print("\nGenerating visualization...")
        # Save as Mermaid
        pipeline.save_mermaid("simple_sequential.mmd")
        print("✓ Saved Mermaid diagram to simple_sequential.mmd")

        # Save as PNG
        pipeline.save_graphviz("simple_sequential.png", format="png")
        print("✓ Saved PNG diagram to simple_sequential.png")

    # Execute based on mode
    print(f"\nExecuting in {args.mode} mode...")

    if args.mode == "sync":
        result = pipeline.run(filename="data.csv")
    elif args.mode == "async":
        result = asyncio.run(pipeline.run_async(filename="data.csv"))
    else:  # parallel
        runner = DAGRunner(pipeline)
        result = runner.run(filename="data.csv", mode="parallel")

    # Display results
    print("\nExecution complete!")
    print("\nFinal Report:")
    print(result)

    # Show intermediate results
    print("\nIntermediate Results:")
    print(f"Loaded data: {pipeline['load_data']['source']}")
    print(f"Valid records: {len(pipeline['validate_data']['valid_records'])}")
    print(f"Transformed count: {pipeline['transform_data']['count']}")


if __name__ == "__main__":
    main()
