"""
Unit tests for connection patterns and operators.
"""

import pytest

from fast_dag import DAG, ConditionalReturn


class TestOperatorOverloading:
    """Test connection operator overloading"""

    def test_shift_operator_simple(self):
        """Test >> operator for simple connection"""
        dag = DAG("test")

        @dag.node
        def a() -> int:
            return 1

        @dag.node
        def b(x: int) -> int:
            return x + 1

        # Use >> operator
        dag.nodes["a"] >> dag.nodes["b"]

        # Verify connection
        assert dag.nodes["b"].input_connections["x"][0] == dag.nodes["a"]
        assert (dag.nodes["b"], "x") in dag.nodes["a"].output_connections["result"]

    def test_shift_operator_chain(self):
        """Test chaining >> operators"""
        dag = DAG("test")

        @dag.node
        def step1() -> int:
            return 1

        @dag.node
        def step2(x: int) -> int:
            return x * 2

        @dag.node
        def step3(x: int) -> int:
            return x + 10

        # Chain connections
        dag.nodes["step1"] >> dag.nodes["step2"] >> dag.nodes["step3"]

        # Verify all connections
        assert dag.nodes["step2"].input_connections["x"][0] == dag.nodes["step1"]
        assert dag.nodes["step3"].input_connections["x"][0] == dag.nodes["step2"]

    def test_pipe_operator(self):
        """Test | operator for connection"""
        dag = DAG("test")

        @dag.node
        def source() -> str:
            return "hello"

        @dag.node
        def process(text: str) -> str:
            return text.upper()

        # Use | operator
        dag.nodes["source"] | dag.nodes["process"]

        # Verify connection
        assert dag.nodes["process"].input_connections["text"][0] == dag.nodes["source"]

    def test_pipe_operator_chain(self):
        """Test chaining | operators"""
        dag = DAG("test")

        @dag.node
        def a() -> int:
            return 1

        @dag.node
        def b(x: int) -> int:
            return x + 1

        @dag.node
        def c(x: int) -> int:
            return x * 2

        # Chain with pipe
        dag.nodes["a"] | dag.nodes["b"] | dag.nodes["c"]

        # Verify connections
        assert dag.nodes["b"].input_connections["x"][0] == dag.nodes["a"]
        assert dag.nodes["c"].input_connections["x"][0] == dag.nodes["b"]


class TestParallelConnections:
    """Test parallel branching connections"""

    def test_one_to_many_connection(self):
        """Test connecting one node to multiple nodes"""
        dag = DAG("test")

        @dag.node
        def source() -> int:
            return 10

        @dag.node
        def branch1(x: int) -> int:
            return x * 2

        @dag.node
        def branch2(x: int) -> int:
            return x + 5

        @dag.node
        def branch3(x: int) -> int:
            return x - 3

        # Connect source to multiple branches
        dag.nodes["source"] >> [
            dag.nodes["branch1"],
            dag.nodes["branch2"],
            dag.nodes["branch3"],
        ]

        # Verify all connections
        for branch in ["branch1", "branch2", "branch3"]:
            assert dag.nodes[branch].input_connections["x"][0] == dag.nodes["source"]
            assert (dag.nodes[branch], "x") in dag.nodes["source"].output_connections[
                "result"
            ]

    def test_many_to_one_connection(self):
        """Test connecting multiple nodes to one node"""
        dag = DAG("test")

        @dag.node
        def input1() -> int:
            return 1

        @dag.node
        def input2() -> int:
            return 2

        @dag.node
        def input3() -> int:
            return 3

        @dag.node
        def combine(a: int, b: int, c: int) -> int:
            return a + b + c

        # Connect multiple inputs to combine
        [dag.nodes["input1"], dag.nodes["input2"], dag.nodes["input3"]] >> dag.nodes[
            "combine"
        ]

        # Verify connections with automatic input mapping
        assert dag.nodes["combine"].input_connections["a"][0] == dag.nodes["input1"]
        assert dag.nodes["combine"].input_connections["b"][0] == dag.nodes["input2"]
        assert dag.nodes["combine"].input_connections["c"][0] == dag.nodes["input3"]

    def test_mixed_parallel_sequential(self):
        """Test mixing parallel and sequential connections"""
        dag = DAG("test")

        @dag.node
        def start() -> int:
            return 5

        @dag.node
        def parallel1(x: int) -> int:
            return x * 2

        @dag.node
        def parallel2(x: int) -> int:
            return x + 10

        @dag.node
        def merge(a: int, b: int) -> int:
            return a + b

        @dag.node
        def final(x: int) -> int:
            return x * 10

        # Complex connection pattern
        dag.nodes["start"] >> [dag.nodes["parallel1"], dag.nodes["parallel2"]]
        [dag.nodes["parallel1"], dag.nodes["parallel2"]] >> dag.nodes["merge"]
        dag.nodes["merge"] >> dag.nodes["final"]

        # Verify execution works correctly
        result = dag.run()
        # start: 5
        # parallel1: 5 * 2 = 10
        # parallel2: 5 + 10 = 15
        # merge: 10 + 15 = 25
        # final: 25 * 10 = 250
        assert result == 250


class TestConditionalConnections:
    """Test conditional node connections"""

    def test_conditional_output_access(self):
        """Test accessing conditional outputs"""
        dag = DAG("test")

        @dag.condition()
        def check(x: int) -> ConditionalReturn:
            return ConditionalReturn(condition=x > 0, value=x)

        @dag.node
        def positive(x: int) -> str:
            return f"positive: {x}"

        @dag.node
        def negative(x: int) -> str:
            return f"negative: {x}"

        # Access conditional outputs
        dag.nodes["check"].on_true >> dag.nodes["positive"]
        dag.nodes["check"].on_false >> dag.nodes["negative"]

        # Verify connections
        assert (dag.nodes["positive"], "x") in dag.nodes["check"].output_connections[
            "true"
        ]
        assert (dag.nodes["negative"], "x") in dag.nodes["check"].output_connections[
            "false"
        ]

    def test_conditional_complex_branching(self):
        """Test complex conditional branching"""
        dag = DAG("test")

        @dag.node
        def start() -> int:
            return 10

        @dag.condition()
        def first_check(x: int) -> ConditionalReturn:
            return ConditionalReturn(condition=x > 5, value=x)

        @dag.condition()
        def second_check(x: int) -> ConditionalReturn:
            return ConditionalReturn(condition=x > 15, value=x)

        @dag.node
        def small(x: int) -> str:
            return f"small: {x}"

        @dag.node
        def medium(x: int) -> str:
            return f"medium: {x}"

        @dag.node
        def large(x: int) -> str:
            return f"large: {x}"

        # Nested conditions
        dag.nodes["start"] >> dag.nodes["first_check"]
        dag.nodes["first_check"].on_false >> dag.nodes["small"]
        dag.nodes["first_check"].on_true >> dag.nodes["second_check"]
        dag.nodes["second_check"].on_false >> dag.nodes["medium"]
        dag.nodes["second_check"].on_true >> dag.nodes["large"]

        # Test execution
        dag.run()
        assert dag["medium"] == "medium: 10"  # 10 > 5 but not > 15


class TestMethodChaining:
    """Test method chaining for connections"""

    def test_connect_to_method(self):
        """Test connect_to method chaining"""
        dag = DAG("test")

        @dag.node
        def a() -> int:
            return 1

        @dag.node
        def b(x: int) -> int:
            return x + 1

        @dag.node
        def c(x: int) -> int:
            return x * 2

        # Use connect_to
        dag.nodes["a"].connect_to(dag.nodes["b"]).connect_to(dag.nodes["c"])

        # Verify chain
        assert dag.nodes["b"].input_connections["x"][0] == dag.nodes["a"]
        assert dag.nodes["c"].input_connections["x"][0] == dag.nodes["b"]

    def test_specific_port_connections(self):
        """Test connecting specific outputs to inputs"""
        dag = DAG("test")

        @dag.node(outputs=["sum", "product"])
        def math_ops(a: int, b: int) -> tuple[int, int]:
            return a + b, a * b

        @dag.node
        def use_sum(value: int) -> int:
            return value * 10

        @dag.node
        def use_product(value: int) -> int:
            return value + 100

        # Connect specific outputs
        dag.nodes["math_ops"].output_ports["sum"].connect_to(
            dag.nodes["use_sum"].input_ports["value"]
        )
        dag.nodes["math_ops"].output_ports["product"].connect_to(
            dag.nodes["use_product"].input_ports["value"]
        )

        # Verify specific connections
        assert dag.nodes["use_sum"].input_connections["value"] == (
            dag.nodes["math_ops"],
            "sum",
        )
        assert dag.nodes["use_product"].input_connections["value"] == (
            dag.nodes["math_ops"],
            "product",
        )


class TestAnyNode:
    """Test ANY node functionality (at least one input required)"""

    def test_any_node_basic(self):
        """Test basic ANY node functionality"""
        dag = DAG("test")

        @dag.node
        def source1() -> int:
            return 10

        @dag.node
        def source2() -> int:
            raise Exception("This fails")

        @dag.node
        def source3() -> int:
            return 30

        @dag.any()
        def any_input(a: int | None, b: int | None, c: int | None) -> int:
            # Return first non-None value
            return a or b or c or 0

        # Connect all sources to any
        [dag.nodes["source1"], dag.nodes["source2"], dag.nodes["source3"]] >> dag.nodes[
            "any_input"
        ]

        # Run with error strategy continue
        result = dag.run(error_strategy="continue")

        # Should get result from source1 (or source3 if execution order differs)
        assert result in [10, 30]

    def test_any_node_all_fail(self):
        """Test ANY node when all inputs fail"""
        dag = DAG("test")

        @dag.node
        def fail1() -> int:
            raise Exception("Fail 1")

        @dag.node
        def fail2() -> int:
            raise Exception("Fail 2")

        @dag.any()
        def any_input(a: int | None, b: int | None) -> int:
            # This should fail if all inputs are None
            if a is None and b is None:
                raise ValueError("No valid inputs")
            return a or b or 0

        [dag.nodes["fail1"], dag.nodes["fail2"]] >> dag.nodes["any_input"]

        # Should raise error
        with pytest.raises(ValueError):
            dag.run(error_strategy="continue")


class TestComplexConnectionPatterns:
    """Test complex real-world connection patterns"""

    def test_diamond_pattern(self):
        """Test diamond-shaped DAG pattern"""
        dag = DAG("diamond")

        @dag.node
        def top() -> int:
            return 100

        @dag.node
        def left(x: int) -> int:
            return x - 10

        @dag.node
        def right(x: int) -> int:
            return x + 10

        @dag.node
        def bottom(k: int, r: int) -> int:
            return k + r

        # Create diamond
        dag.nodes["top"] >> [dag.nodes["left"], dag.nodes["right"]]
        dag.connect("left", "bottom", input="k")
        dag.connect("right", "bottom", input="r")

        result = dag.run()
        # top: 100
        # left: 100 - 10 = 90
        # right: 100 + 10 = 110
        # bottom: 90 + 110 = 200
        assert result == 200

    def test_multi_stage_pipeline(self):
        """Test multi-stage processing pipeline"""
        dag = DAG("pipeline")

        # Stage 1: Data sources
        @dag.node
        def source_a() -> dict:
            return {"type": "a", "value": 10}

        @dag.node
        def source_b() -> dict:
            return {"type": "b", "value": 20}

        # Stage 2: Validation
        @dag.node
        def validate(data: dict) -> dict:
            assert "type" in data and "value" in data
            return data

        # Stage 3: Processing
        @dag.node
        def process(data: dict) -> int:
            return data["value"] * 2

        # Stage 4: Aggregation
        @dag.node
        def aggregate(a: int, b: int) -> int:
            return a + b

        # Connect pipeline
        dag.nodes["source_a"] >> dag.nodes["validate"] >> dag.nodes["process"]
        dag.nodes["source_b"] >> dag.nodes["validate"] >> dag.nodes["process"]

        # Note: This would require proper handling of multiple nodes with same name
        # In practice, we'd use unique names for each validate/process instance

    def test_fan_out_fan_in(self):
        """Test fan-out fan-in pattern"""
        dag = DAG("fan_pattern")

        @dag.node
        def splitter(data: list) -> dict:
            return {"chunk1": data[:2], "chunk2": data[2:4], "chunk3": data[4:]}

        @dag.node
        def process1(chunk: list) -> int:
            return sum(chunk)

        @dag.node
        def process2(chunk: list) -> int:
            return sum(chunk)

        @dag.node
        def process3(chunk: list) -> int:
            return sum(chunk)

        @dag.node
        def combine(r1: int, r2: int, r3: int) -> int:
            return r1 + r2 + r3

        # Manual connections for specific chunks
        # This demonstrates the need for more sophisticated output routing


class TestConnectionValidation:
    """Test connection validation for multi-input convergence"""

    def test_regular_node_rejects_multiple_connections_to_fewer_inputs(self):
        """Test that regular nodes reject multiple source connections when sources > inputs"""
        dag = DAG("test")

        @dag.node
        def source1() -> int:
            return 1

        @dag.node
        def source2() -> int:
            return 2

        @dag.node
        def source3() -> int:
            return 3

        @dag.node
        def target(a: int) -> int:
            return a

        # Should fail when connecting 3 sources to 1 input
        with pytest.raises(ValueError, match="Cannot connect 3 source nodes"):
            [
                dag.nodes["source1"],
                dag.nodes["source2"],
                dag.nodes["source3"],
            ] >> dag.nodes["target"]

    def test_regular_node_accepts_multiple_connections_to_different_inputs(self):
        """Test that regular nodes accept multiple source connections to different inputs"""
        dag = DAG("test")

        @dag.node
        def source1() -> int:
            return 1

        @dag.node
        def source2() -> int:
            return 2

        @dag.node
        def target(a: int, b: int) -> int:
            return a + b

        # Should work when connecting 2 sources to 2 inputs
        [dag.nodes["source1"], dag.nodes["source2"]] >> dag.nodes["target"]

        # Test execution
        result = dag.run()
        assert result == 3

    def test_any_node_accepts_multiple_connections(self):
        """Test that ANY nodes accept multiple source connections"""
        dag = DAG("test")

        @dag.node
        def source1() -> int:
            return 1

        @dag.node
        def source2() -> int:
            return 2

        @dag.any()
        def any_target(a: int | None, b: int | None) -> int:
            return (a or 0) + (b or 0)

        # Should work with ANY node
        [dag.nodes["source1"], dag.nodes["source2"]] >> dag.nodes["any_target"]

        # Test execution
        result = dag.run()
        assert result == 3

    def test_all_node_accepts_multiple_connections(self):
        """Test that ALL nodes accept multiple source connections"""
        dag = DAG("test")

        @dag.node
        def source1() -> int:
            return 1

        @dag.node
        def source2() -> int:
            return 2

        @dag.all()
        def all_target(a: int, b: int) -> int:
            return a + b

        # Should work with ALL node
        [dag.nodes["source1"], dag.nodes["source2"]] >> dag.nodes["all_target"]

        # Test execution
        result = dag.run()
        assert result == 3

    def test_validation_rejects_multiple_connections_to_same_input(self):
        """Test that validation rejects multiple connections to the same input for regular nodes"""
        dag = DAG("test")

        @dag.node
        def source1() -> int:
            return 1

        @dag.node
        def source2() -> int:
            return 2

        @dag.node
        def target(a: int) -> int:
            return a

        # Make manual connections - two sources to same input
        dag.connect("source1", "target", input="a")
        dag.connect("source2", "target", input="a")

        # Should fail validation
        with pytest.raises(Exception, match="multiple connections"):
            dag.validate_or_raise()

    def test_validation_allows_multiple_connections_to_any_node(self):
        """Test that validation allows multiple connections to ANY node inputs"""
        dag = DAG("test")

        @dag.node
        def source1() -> int:
            return 1

        @dag.node
        def source2() -> int:
            return 2

        @dag.any()
        def any_target(a: int | None) -> int:
            return a or 0

        # Make manual connections - two sources to same input of ANY node
        dag.connect("source1", "any_target", input="a")
        dag.connect("source2", "any_target", input="a")

        # Should pass validation
        dag.validate_or_raise()

    def test_validation_allows_multiple_connections_to_all_node(self):
        """Test that validation allows multiple connections to ALL node inputs"""
        dag = DAG("test")

        @dag.node
        def source1() -> int:
            return 1

        @dag.node
        def source2() -> int:
            return 2

        @dag.all()
        def all_target(a: int) -> int:
            return a

        # Make manual connections - two sources to same input of ALL node
        dag.connect("source1", "all_target", input="a")
        dag.connect("source2", "all_target", input="a")

        # Should pass validation
        dag.validate_or_raise()
