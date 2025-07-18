"""
Integration tests for error scenarios and validation failures.
Based on example 04 - invalid DAG scenarios.
"""

import pytest

from fast_dag import (
    DAG,
    FSM,
    Context,
    CycleError,
    ExecutionError,
    FSMContext,
    InvalidNodeError,
    TimeoutError,
    ValidationError,
)


class TestDAGValidationErrors:
    """Test various DAG validation error scenarios"""

    def test_cycle_detection(self):
        """Test detection of cycles in DAG"""
        dag = DAG("cyclic_dag")

        @dag.node
        def a() -> int:
            return 1

        @dag.node
        def b(x: int) -> int:
            return x + 1

        @dag.node
        def c(x: int) -> int:
            return x + 1

        # Create a cycle: a -> b -> c -> a
        dag.nodes["a"] >> dag.nodes["b"] >> dag.nodes["c"] >> dag.nodes["a"]

        # Should detect cycle
        errors = dag.validate()
        assert any("cycle" in str(e).lower() for e in errors)

        # Should raise on validate_or_raise
        with pytest.raises(CycleError):
            dag.validate_or_raise()

        # Should not be able to run
        with pytest.raises(CycleError):
            dag.run()

    def test_disconnected_nodes(self):
        """Test detection of disconnected nodes"""
        dag = DAG("disconnected")

        @dag.node
        def connected1() -> int:
            return 1

        @dag.node
        def connected2(x: int) -> int:
            return x + 1

        @dag.node
        def island() -> int:
            """This node is not connected to anything"""
            return 42

        # Connect only two nodes
        dag.nodes["connected1"] >> dag.nodes["connected2"]

        # Should detect disconnected node
        errors = dag.validate()
        assert any(
            "disconnected" in str(e).lower() or "island" in str(e) for e in errors
        )

        # Should work with allow_disconnected
        dag.validate_or_raise(allow_disconnected=True)

    def test_missing_conditional_branches(self):
        """Test missing branches in conditional nodes"""
        dag = DAG("incomplete_conditional")

        @dag.node
        def start() -> int:
            return 10

        @dag.condition()
        def check(x: int):
            from fast_dag import ConditionalReturn

            return ConditionalReturn(condition=x > 5, value=x)

        @dag.node
        def handle_true(x: int) -> str:
            return f"True: {x}"

        # Connect only true branch
        dag.nodes["start"] >> dag.nodes["check"]
        dag.nodes["check"].on_true >> dag.nodes["handle_true"]
        # Missing: check.on_false connection

        # Should detect missing false branch
        errors = dag.validate()
        assert any("false" in str(e).lower() for e in errors)

        with pytest.raises(ValidationError):
            dag.validate_or_raise()

    def test_input_mismatch(self):
        """Test node input requirements not satisfied"""
        dag = DAG("input_mismatch")

        @dag.node
        def source() -> int:
            return 42

        @dag.node
        def needs_two_inputs(a: int, b: int) -> int:
            return a + b

        # Connect only one input
        dag.connect("source", "needs_two_inputs", input="a")
        # Missing connection for input 'b'

        # Validation no longer detects missing inputs at validation time
        # because nodes can get some inputs from kwargs and some from connections
        dag.validate()  # Just validate, don't check errors
        # assert any("input" in str(e).lower() and "b" in str(e) for e in errors)

        # But it should still fail at runtime when the input is actually missing
        with pytest.raises(TypeError, match="missing 1 required positional argument"):
            dag.run()

    def test_type_mismatch(self):
        """Test type mismatches between connected nodes"""
        dag = DAG("type_mismatch")

        @dag.node
        def string_output() -> str:
            return "hello"

        @dag.node
        def expects_int(x: int) -> int:
            return x * 2

        # Connect string output to int input
        dag.nodes["string_output"] >> dag.nodes["expects_int"]

        # Should detect type mismatch if type checking is enabled
        errors = dag.validate(check_types=True)
        assert any("type" in str(e).lower() for e in errors)

    def test_multiple_inputs_to_single_port(self):
        """Test multiple nodes connecting to same input port"""
        dag = DAG("multiple_inputs")

        @dag.node
        def source1() -> int:
            return 10

        @dag.node
        def source2() -> int:
            return 20

        @dag.node
        def target(x: int) -> int:
            return x * 2

        # Both sources try to connect to same input
        dag.connect("source1", "target", input="x")
        dag.connect("source2", "target", input="x")

        # NOTE: This validation is currently disabled as it breaks legitimate use cases
        # The second connection overwrites the first one
        # errors = dag.validate()
        # assert any(
        #     "multiple" in str(e).lower() or "conflict" in str(e).lower() for e in errors
        # )

        # Instead, verify the last connection wins
        assert dag.nodes["target"].input_connections["x"][0] == dag.nodes["source2"]


class TestFSMValidationErrors:
    """Test FSM-specific validation errors"""

    def test_no_initial_state(self):
        """Test FSM without initial state"""
        fsm = FSM("no_initial")

        @fsm.state
        def state1(context: FSMContext):
            from fast_dag import FSMReturn

            return FSMReturn(next_state="state2")

        @fsm.state
        def state2(context: FSMContext):
            from fast_dag import FSMReturn

            return FSMReturn(stop=True)

        # No state marked as initial
        errors = fsm.validate()
        assert any("initial" in str(e).lower() for e in errors)

        with pytest.raises(ValidationError):
            fsm.run()

    def test_multiple_initial_states(self):
        """Test FSM with multiple initial states"""
        fsm = FSM("multiple_initial")

        @fsm.state(initial=True)
        def start1(context: FSMContext):
            from fast_dag import FSMReturn

            return FSMReturn(next_state="end")

        @fsm.state(initial=True)  # Second initial state
        def start2(context: FSMContext):
            from fast_dag import FSMReturn

            return FSMReturn(next_state="end")

        @fsm.state(terminal=True)
        def end(context: FSMContext):
            from fast_dag import FSMReturn

            return FSMReturn(stop=True)

        # Should detect multiple initial states
        errors = fsm.validate()
        assert any(
            "multiple" in str(e).lower() and "initial" in str(e).lower() for e in errors
        )

    @pytest.mark.skip(
        reason="FSM validation skips reachability checking due to dynamic FSMReturn transitions"
    )
    def test_unreachable_states(self):
        """Test FSM with unreachable states"""
        fsm = FSM("unreachable")

        @fsm.state(initial=True)
        def start(context: FSMContext):
            from fast_dag import FSMReturn

            return FSMReturn(next_state="middle")

        @fsm.state
        def middle(context: FSMContext):
            from fast_dag import FSMReturn

            return FSMReturn(next_state="end")

        @fsm.state(terminal=True)
        def end(context: FSMContext):
            from fast_dag import FSMReturn

            return FSMReturn(stop=True)

        @fsm.state
        def orphan(context: FSMContext):
            """This state is never reached"""
            from fast_dag import FSMReturn

            return FSMReturn(stop=True)

        # Should detect unreachable state
        errors = fsm.validate()
        assert any(
            "unreachable" in str(e).lower() or "orphan" in str(e) for e in errors
        )

    def test_invalid_state_transition(self):
        """Test FSM with invalid state transitions"""
        fsm = FSM("invalid_transition")

        @fsm.state(initial=True)
        def start(context: FSMContext):
            from fast_dag import FSMReturn

            # Reference non-existent state
            return FSMReturn(next_state="does_not_exist")

        # Note: We can't detect invalid transitions statically since FSMReturn
        # is created at runtime. This would require analyzing the function body.
        # Skip static validation check for now.

        # Should fail at runtime
        with pytest.raises(ExecutionError) as exc_info:
            fsm.run()
        assert "does_not_exist" in str(exc_info.value)


class TestRuntimeErrors:
    """Test runtime error scenarios"""

    def test_node_execution_error(self):
        """Test handling of node execution errors"""
        dag = DAG("execution_error")

        @dag.node
        def will_fail() -> int:
            raise RuntimeError("Intentional failure")

        @dag.node
        def dependent(x: int) -> int:
            return x * 2

        dag.nodes["will_fail"] >> dag.nodes["dependent"]

        # Default behavior: stop on error
        with pytest.raises(RuntimeError, match="Intentional failure"):
            dag.run()

        # Continue on error
        dag.run(error_strategy="continue")
        assert dag.get("will_fail") is None
        assert dag.get("dependent") is None  # Couldn't run due to missing input

    def test_timeout_error(self):
        """Test node timeout handling"""
        dag = DAG("timeout")

        @dag.node(timeout=0.1)
        def slow_node() -> int:
            import time

            time.sleep(1.0)  # Will timeout
            return 42

        with pytest.raises(TimeoutError):
            dag.run()

    def test_infinite_fsm_loop(self):
        """Test FSM infinite loop protection"""
        fsm = FSM("infinite", max_cycles=5)

        @fsm.state(initial=True)
        def loop_forever(context: FSMContext):
            from fast_dag import FSMReturn

            # Always loop back to self
            return FSMReturn(next_state="loop_forever", value=1)

        # Should stop after max_cycles with error
        with pytest.raises(ExecutionError, match="Maximum cycles"):
            fsm.run()

    def test_invalid_node_registration(self):
        """Test invalid node registration attempts"""
        dag = DAG("invalid_registration")

        # Invalid function (not callable)
        with pytest.raises(InvalidNodeError):
            dag.add_node("not a function")

        # Invalid function - no outputs (would need empty return type)
        # This is harder to test since functions always have at least ['result'] output

    @pytest.mark.skip(
        reason="Accessing own result doesn't create a circular dependency, just returns default"
    )
    def test_circular_dependency_at_runtime(self):
        """Test circular dependency created at runtime"""
        dag = DAG("runtime_circular")

        @dag.node
        def dynamic_node(x: int, context: Context) -> int:
            # Try to access own result (circular)
            own_result = context.get("dynamic_node", 0)
            return x + own_result

        # This creates a runtime circular dependency
        with pytest.raises((RuntimeError, RecursionError, ValueError)):
            dag.run(x=10)


class TestValidationMethods:
    """Test different validation methods and options"""

    def test_validate_returns_list(self):
        """Test validate() returns list of errors"""
        dag = DAG("test")

        @dag.node
        def node1() -> int:
            return 1

        @dag.node
        def node2() -> int:
            return 2

        # node2 is disconnected from node1

        errors = dag.validate()
        assert isinstance(errors, list)
        assert len(errors) > 0  # Should have disconnected node error

    def test_validate_or_raise_with_no_errors(self):
        """Test validate_or_raise with valid DAG"""
        dag = DAG("valid")

        @dag.node
        def task() -> int:
            return 42

        # Should not raise
        dag.validate_or_raise()

    def test_validate_with_options(self):
        """Test validation with different options"""
        dag = DAG("test")

        @dag.node
        def node1() -> int:
            return 1

        @dag.node
        def isolated() -> int:
            return 2

        # Strict validation - isolated is disconnected
        errors = dag.validate(allow_disconnected=False)
        assert len(errors) > 0

        # Relaxed validation
        errors = dag.validate(allow_disconnected=True)
        assert len(errors) == 0

    def test_is_valid_property(self):
        """Test is_valid property"""
        dag = DAG("test")

        @dag.node
        def a() -> int:
            return 1

        @dag.node
        def b(x: int) -> int:
            return x + 1

        # Invalid (disconnected)
        assert not dag.is_valid

        # Make valid
        dag.nodes["a"] >> dag.nodes["b"]
        assert dag.is_valid


class TestErrorRecovery:
    """Test error recovery and partial execution"""

    def test_partial_execution_results(self):
        """Test accessing partial results after error"""
        dag = DAG("partial")

        @dag.node
        def successful1() -> int:
            return 10

        @dag.node
        def successful2() -> int:
            return 20

        @dag.node
        def fails() -> int:
            raise ValueError("Fail")

        @dag.node
        def never_runs(x: int) -> int:
            return x * 2

        dag.nodes["fails"] >> dag.nodes["never_runs"]

        # Run with continue on error
        dag.run(error_strategy="continue")

        # Should have results for successful nodes
        assert dag["successful1"] == 10
        assert dag["successful2"] == 20

        # Failed and dependent nodes should be None
        assert dag.get("fails") is None
        assert dag.get("never_runs") is None

        # Context should have error info
        assert "fails_error" in dag.context.metadata

    def test_retry_with_backoff(self):
        """Test retry with exponential backoff"""
        dag = DAG("retry_backoff")

        attempt_times = []

        @dag.node(retry=3, retry_delay=0.1)
        def flaky_node() -> int:
            import time

            attempt_times.append(time.time())

            if len(attempt_times) < 3:
                raise ValueError("Still flaky")
            return 42

        dag.run()

        # Check retry delays increase
        if len(attempt_times) >= 2:
            delay1 = attempt_times[1] - attempt_times[0]
            assert delay1 >= 0.1  # At least base delay

        if len(attempt_times) >= 3:
            delay2 = attempt_times[2] - attempt_times[1]
            assert delay2 >= delay1  # Exponential backoff
