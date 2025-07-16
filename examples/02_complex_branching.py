#!/usr/bin/env python3
"""
Complex Branching DAG Example

Demonstrates:
- Conditional nodes (if/else branching)
- Parallel branches (multiple children)
- AND nodes (multiple inputs)
- Different execution modes
"""

import argparse
import asyncio

from fast_dag import DAG, ConditionalReturn, Context, DAGRunner


def main():
    parser = argparse.ArgumentParser(description="Complex Branching DAG")
    parser.add_argument(
        "--mode",
        choices=["sync", "async", "parallel"],
        default="sync",
        help="Execution mode",
    )
    parser.add_argument("--viz", action="store_true", help="Generate visualization")
    args = parser.parse_args()

    # Create workflow
    workflow = DAG("order_processing")

    # Entry point
    @workflow.node
    def receive_order(order_data: dict) -> dict:
        """Receive and parse order"""
        print(f"Received order: {order_data['order_id']}")
        return {
            "order_id": order_data["order_id"],
            "amount": order_data["amount"],
            "customer_type": order_data.get("customer_type", "regular"),
            "items": order_data.get("items", []),
        }

    # Conditional node - check order validity
    @workflow.condition()
    def validate_order(order: dict) -> ConditionalReturn:
        """Validate order and branch"""
        print(f"Validating order {order['order_id']}")

        is_valid = (
            order["amount"] > 0
            and len(order["items"]) > 0
            and order["amount"] < 10000  # Fraud check
        )

        return ConditionalReturn(condition=is_valid, value=order)

    # Valid path - parallel processing
    @workflow.node
    def check_inventory(order: dict) -> dict:
        """Check inventory availability"""
        print("Checking inventory...")
        available = all(item["quantity"] <= 100 for item in order["items"])
        return {"order_id": order["order_id"], "inventory_ok": available}

    @workflow.node
    def calculate_pricing(order: dict) -> dict:
        """Calculate final pricing"""
        print("Calculating pricing...")
        discount = 0.1 if order["customer_type"] == "premium" else 0
        final_amount = order["amount"] * (1 - discount)
        return {
            "order_id": order["order_id"],
            "original_amount": order["amount"],
            "discount": discount,
            "final_amount": final_amount,
        }

    @workflow.node
    def check_fraud(order: dict) -> dict:
        """Run fraud detection"""
        print("Running fraud check...")
        # Simple fraud rules
        risk_score = 0
        if order["amount"] > 5000:
            risk_score += 30
        if order["customer_type"] == "new":
            risk_score += 20

        return {
            "order_id": order["order_id"],
            "risk_score": risk_score,
            "fraud_check_passed": risk_score < 50,
        }

    # AND node - combines multiple inputs
    @workflow.node
    def process_payment(pricing: dict, fraud: dict, inventory: dict) -> dict:
        """Process payment after all checks"""
        print("Processing payment...")

        if not inventory["inventory_ok"]:
            return {"status": "failed", "reason": "inventory_unavailable"}

        if not fraud["fraud_check_passed"]:
            return {"status": "failed", "reason": "fraud_detected"}

        return {
            "status": "success",
            "order_id": pricing["order_id"],
            "amount_charged": pricing["final_amount"],
            "transaction_id": f"TXN-{pricing['order_id']}",
        }

    # Invalid path
    @workflow.node
    def reject_order(order: dict) -> dict:
        """Handle invalid orders"""
        print(f"Rejecting order {order['order_id']}")
        return {
            "status": "rejected",
            "order_id": order["order_id"],
            "reason": "validation_failed",
        }

    # Final node
    @workflow.node(outputs=("msg", "result"))
    def send_notification(result: dict) -> tuple[str, dict]:
        """Send customer notification"""
        if result["status"] == "success":
            msg = f"Order confirmed! Transaction: {result['transaction_id']}"
        else:
            msg = f"Order failed: {result['reason']}"

        print(f"Notification: {msg}")
        return msg, result

    # we could connect these with:
    # send_notification.msg >> process_payment
    # send_notification.result >> send_notification
    # [send_notification.result, reject_order.result] >> other_node

    # Connect nodes
    receive_order >> validate_order

    # Valid branch - parallel checks then merge
    validate_order.on_true >> [check_inventory, calculate_pricing, check_fraud]

    # All three must complete before payment (AND node)
    [check_inventory, calculate_pricing, check_fraud] >> process_payment

    # optional ANY node
    @workflow.any()
    def any_node(
        check_inventory: dict | None,
        calculate_pricing: dict | None,
        check_fraud: dict | None,
    ) -> dict:
        # internal logic asserts that at least one of the inputs is not None
        return check_inventory or calculate_pricing or check_fraud  # type: ignore

    # how to add:
    # [check_inventory, calculate_pricing, check_fraud] >> any_node >> process_payment

    # Invalid branch
    validate_order.on_false >> reject_order

    # Both paths lead to notification
    [process_payment, reject_order] >> send_notification

    # Validate
    print("Validating workflow...")
    workflow.validate_or_raise()
    print("✓ Workflow is valid")

    # Visualize
    if args.viz:
        print("\nGenerating visualization...")
        workflow.save_mermaid("complex_branching.mmd")

    # Test data
    test_orders = [
        {
            "order_id": "ORD-001",
            "amount": 150.00,
            "customer_type": "regular",
            "items": [{"id": "A", "quantity": 2}],
        },
        {
            "order_id": "ORD-002",
            "amount": 7500.00,
            "customer_type": "premium",
            "items": [{"id": "B", "quantity": 1}],
        },
        {
            "order_id": "ORD-003",
            "amount": -50.00,  # Invalid
            "items": [],
        },
    ]

    # Execute for each test order
    for order_data in test_orders:
        print(f"\n{'=' * 50}")
        print(f"Processing order: {order_data['order_id']}")
        print(f"{'=' * 50}")

        context = Context()
        if args.mode == "sync":
            result = workflow.run(context=context, order_data=order_data)
        elif args.mode == "async":
            result = asyncio.run(
                workflow.run_async(context=context, order_data=order_data)
            )
        else:
            runner: DAGRunner = workflow.runner
            result = runner.run(
                order_data=order_data, mode="parallel", max_workers=3, context=context
            )

        print(f"\nFinal result: {result}")

        if args.viz:
            workflow.save_mermaid("complex_branching_executed.mmd", context=context)


if __name__ == "__main__":
    main()
