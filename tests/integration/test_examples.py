"""
Integration tests based on the example workflows.
"""

import asyncio
from fast_dag import (
    DAG,
    FSM,
    Node,
    Context,
    FSMContext,
    ConditionalReturn,
    FSMReturn,
    DAGRunner,
    FunctionRegistry,
)


class TestSimpleSequentialExample:
    """Test implementation of example 01 - simple sequential pipeline"""

    def test_data_processing_pipeline(self):
        """Test complete data processing pipeline"""
        # Create workflow
        pipeline = DAG("data_processing_pipeline")

        # Define nodes with decorators
        @pipeline.node
        def load_data(filename: str) -> dict:
            """Load data from file"""
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
            records = transformed["transformed"]

            total_value = sum(r["value"] for r in records)
            high_count = sum(1 for r in records if r["category"] == "high")

            report = f"""Data Processing Report
=====================
Total Records: {transformed["count"]}
Total Value: {total_value}
High Value Records: {high_count}
Low Value Records: {transformed["count"] - high_count}"""

            return report

        # Connect nodes sequentially
        load_data >> validate_data >> transform_data >> generate_report

        # Validate workflow
        errors = pipeline.validate()
        assert errors == []

        # Execute
        result = pipeline.run(filename="data.csv")

        # Verify results
        assert "Total Records: 3" in result
        assert "Total Value: 600" in result
        assert "High Value Records: 2" in result
        assert "Low Value Records: 1" in result

        # Check intermediate results
        assert pipeline["load_data"]["source"] == "data.csv"
        assert len(pipeline["validate_data"]["valid_records"]) == 3
        assert pipeline["transform_data"]["count"] == 3

    def test_pipeline_execution_modes(self):
        """Test different execution modes"""
        # Simplified pipeline
        dag = DAG("simple")

        @dag.node
        def step1() -> int:
            return 1

        @dag.node
        def step2(x: int) -> int:
            return x + 1

        @dag.node
        def step3(x: int) -> int:
            return x + 1

        dag.nodes["step1"] >> dag.nodes["step2"] >> dag.nodes["step3"]

        # Test sync mode
        result_sync = dag.run()
        assert result_sync == 3

        # Test async mode
        result_async = asyncio.run(dag.run_async())
        assert result_async == 3

        # Test parallel mode (should work even for sequential)
        runner = DAGRunner(dag)
        result_parallel = runner.run(mode="parallel")
        assert result_parallel == 3


class TestComplexBranchingExample:
    """Test implementation of example 02 - complex branching with conditions"""

    def test_order_processing_workflow(self):
        """Test order processing with conditional branching"""
        workflow = DAG("order_processing")

        # Entry point
        @workflow.node
        def receive_order(order_data: dict) -> dict:
            """Receive and parse order"""
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
            available = all(item.get("quantity", 0) <= 100 for item in order["items"])
            return {"order_id": order["order_id"], "inventory_ok": available}

        @workflow.node
        def calculate_pricing(order: dict) -> dict:
            """Calculate final pricing"""
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

            return msg, result

        # Connect nodes
        receive_order >> validate_order

        # Valid branch - parallel checks then merge
        validate_order.on_true >> [check_inventory, calculate_pricing, check_fraud]

        # All three must complete before payment (AND node)
        [check_inventory, calculate_pricing, check_fraud] >> process_payment

        # Invalid branch
        validate_order.on_false >> reject_order

        # Both paths lead to notification
        [process_payment, reject_order] >> send_notification

        # Validate
        workflow.validate_or_raise()

        # Test valid order
        valid_order = {
            "order_id": "ORD-001",
            "amount": 150.00,
            "customer_type": "regular",
            "items": [{"id": "A", "quantity": 2}],
        }

        context = Context()
        result = workflow.run(context=context, order_data=valid_order)

        assert result[0] == "Order confirmed! Transaction: TXN-ORD-001"
        assert result[1]["status"] == "success"

        # Test invalid order
        invalid_order = {"order_id": "ORD-002", "amount": -50.00, "items": []}

        context2 = Context()
        result2 = workflow.run(context=context2, order_data=invalid_order)

        assert "Order failed" in result2[0]
        assert result2[1]["status"] == "rejected"


class TestFSMExample:
    """Test implementation of example 03 - FSM state machine"""

    def test_traffic_light_fsm(self):
        """Test traffic light state machine"""
        fsm = FSM("traffic_light", max_cycles=10)

        # Define states
        @fsm.state(initial=True)
        def red(context: FSMContext) -> FSMReturn:
            """Red light state"""
            # Track how long we've been red
            red_count = context.metadata.get("red_count", 0) + 1
            context.metadata["red_count"] = red_count

            # After 3 cycles, go to green
            if red_count >= 3:
                context.metadata["red_count"] = 0
                return FSMReturn(
                    next_state="green",
                    value={"light": "red", "action": "stop", "duration": red_count},
                )

            # Stay red
            return FSMReturn(
                next_state="red",
                value={"light": "red", "action": "stop", "duration": red_count},
            )

        @fsm.state
        def green(context: FSMContext) -> FSMReturn:
            """Green light state"""
            # Track green duration
            green_count = context.metadata.get("green_count", 0) + 1
            context.metadata["green_count"] = green_count

            # Check for emergency stop
            if context.metadata.get("emergency", False):
                return FSMReturn(
                    next_state="emergency_stop",
                    value={"light": "green", "action": "emergency"},
                )

            # After 4 cycles, go to yellow
            if green_count >= 4:
                context.metadata["green_count"] = 0
                return FSMReturn(
                    next_state="yellow",
                    value={"light": "green", "action": "go", "duration": green_count},
                )

            return FSMReturn(
                next_state="green",
                value={"light": "green", "action": "go", "duration": green_count},
            )

        @fsm.state
        def yellow(context: FSMContext) -> FSMReturn:
            """Yellow light state"""
            # Yellow is always brief - 1 cycle
            return FSMReturn(
                next_state="red", value={"light": "yellow", "action": "caution"}
            )

        @fsm.state(terminal=True)
        def emergency_stop(context: FSMContext) -> FSMReturn:
            """Emergency stop state"""
            return FSMReturn(
                value={"light": "emergency", "action": "stop", "reason": "emergency"},
                stop=True,
            )

        # Validate FSM
        fsm.validate_or_raise()

        # Test step-by-step execution
        context = FSMContext()
        results = []

        for i in range(12):
            context, result = fsm.step(context)
            results.append(result)

            if fsm.is_terminal_state(fsm.current_state):
                break

        # Verify state transitions
        # Should go: red (3x) -> green (4x) -> yellow (1x) -> red (3x) -> green (1x)
        assert len([r for r in results[:3] if r["light"] == "red"]) == 3
        assert len([r for r in results[3:7] if r["light"] == "green"]) == 4
        assert results[7]["light"] == "yellow"

        # Test emergency scenario
        context2 = FSMContext()
        context2.metadata["emergency"] = False

        # Run until green, then trigger emergency
        for i in range(10):
            if i == 5:
                context2.metadata["emergency"] = True

            context2, result = fsm.step(context2)

            if fsm.current_state == "emergency_stop":
                assert result["action"] == "stop"
                assert result["reason"] == "emergency"
                break


class TestManualNodeCreation:
    """Test implementation of example 05 - manual node creation"""

    def test_manual_dag_creation(self):
        """Test creating DAG with manual node instantiation"""

        # Standalone functions
        def fetch_data(source: str) -> dict:
            return {"data": [1, 2, 3, 4, 5], "source": source}

        def filter_data(data: dict, threshold: int) -> dict:
            filtered = [x for x in data["data"] if x > threshold]
            return {"filtered": filtered, "count": len(filtered)}

        def aggregate_results(filtered: dict) -> dict:
            data = filtered["filtered"]
            return {
                "sum": sum(data),
                "avg": sum(data) / len(data) if data else 0,
                "count": filtered["count"],
            }

        # Create workflow
        dag = DAG("manual_workflow")

        # Create nodes manually
        fetch_node = Node(
            func=fetch_data, name="fetch", inputs=["source"], outputs=["data"]
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

        # Add nodes to DAG
        dag.add_node(fetch_node)
        dag.add_node(filter_node)
        dag.add_node(aggregate_node)

        # Connect using explicit method
        dag.connect("fetch", "filter", output="data", input="data")
        dag.connect("filter", "aggregate", output="filtered", input="filtered")

        # Execute
        result = dag.run(source="database", threshold=2)

        assert result["sum"] == 12  # 3 + 4 + 5
        assert result["avg"] == 4.0
        assert result["count"] == 3

    def test_function_registry_pattern(self):
        """Test using function registry for dynamic workflows"""
        # Create registry
        registry = FunctionRegistry()

        # Define and register functions
        def data_fetcher(source: str) -> dict:
            return {"data": [10, 20, 30]}

        def data_processor(data: dict) -> dict:
            return {"processed": [x * 2 for x in data["data"]]}

        registry.register(data_fetcher, "fetch")
        registry.register(data_processor, "process")

        # Create DAG programmatically
        dag = DAG("registry_workflow")

        # Define workflow structure
        workflow_def = [
            {"name": "get_data", "func": "fetch", "outputs": ["data"]},
            {"name": "process_data", "func": "process", "inputs": ["data"]},
        ]

        # Create nodes from definitions
        for node_def in workflow_def:
            func = registry.get(node_def["func"])
            node = Node(
                func=func,
                name=node_def["name"],
                inputs=node_def.get("inputs"),
                outputs=node_def.get("outputs"),
            )
            dag.add_node(node)

        # Connect
        dag.connect("get_data", "process_data")

        # Execute
        result = dag.run(source="test")
        assert result["processed"] == [20, 40, 60]
