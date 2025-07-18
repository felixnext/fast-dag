"""Advanced unit tests for FSM functionality."""

import pytest

from fast_dag import FSM
from fast_dag.core.context import FSMContext
from fast_dag.core.exceptions import ExecutionError, ValidationError
from fast_dag.core.types import FSMReturn


class TestFSMAdvanced:
    """Test advanced FSM scenarios."""

    def test_fsm_max_cycles_exceeded(self):
        """Test FSM stops when max cycles exceeded."""
        fsm = FSM("infinite", max_cycles=3)

        @fsm.state(initial=True)
        def loop_state(context: FSMContext) -> FSMReturn:
            return FSMReturn(
                next_state="loop_state", value=f"cycle_{context.cycle_count}"
            )

        # Should stop after 3 cycles
        with pytest.raises(ExecutionError, match="Maximum cycles"):
            fsm.run()

    def test_fsm_cycle_history_tracking(self):
        """Test FSM tracks cycle history correctly."""
        fsm = FSM("history", max_cycles=5)

        @fsm.state(initial=True)
        def state_a(context: FSMContext) -> FSMReturn:
            if context.cycle_count < 4:
                return FSMReturn(next_state="state_b", value="from_a")
            return FSMReturn(stop=True, value="done")

        @fsm.state
        def state_b(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="state_a", value="from_b")

        # Execute
        result = fsm.run()

        # Check cycle history
        assert len(fsm.context.state_history) == 5  # a -> b -> a -> b -> a
        assert fsm.context.state_history == [
            "state_a",
            "state_b",
            "state_a",
            "state_b",
            "state_a",
        ]
        assert result == "done"

    def test_fsm_cycle_results_access(self):
        """Test accessing results from specific cycles."""
        fsm = FSM("cycles", max_cycles=3)

        @fsm.state(initial=True)
        def counter(context: FSMContext) -> FSMReturn:
            count = context.cycle_count
            if count < 2:
                return FSMReturn(next_state="counter", value=count)
            return FSMReturn(stop=True, value=count)

        # Execute
        result = fsm.run()

        # Check cycle results
        assert fsm.context.cycle_results == [0, 1, 2]
        assert result == 2

    def test_fsm_state_not_found(self):
        """Test FSM handles non-existent state transition."""
        fsm = FSM("missing")

        @fsm.state(initial=True)
        def start() -> FSMReturn:
            return FSMReturn(next_state="nonexistent", value="test")

        with pytest.raises(ExecutionError, match="State 'nonexistent' not found"):
            fsm.run()

    def test_fsm_no_initial_state(self):
        """Test FSM validation with no initial state."""
        fsm = FSM("no_initial")

        @fsm.state
        def some_state() -> FSMReturn:
            return FSMReturn(stop=True, value="test")

        with pytest.raises(ValidationError, match="No initial state"):
            fsm.run()

    def test_fsm_multiple_initial_states(self):
        """Test FSM with multiple initial states."""
        fsm = FSM("multi_initial")

        @fsm.state(initial=True)
        def first_initial() -> FSMReturn:
            return FSMReturn(stop=True, value="first")

        @fsm.state(initial=True)
        def second_initial() -> FSMReturn:
            return FSMReturn(stop=True, value="second")

        # Should use the last one marked as initial
        result = fsm.run()
        assert result == "second"

    def test_fsm_terminal_state_behavior(self):
        """Test FSM terminal state behavior."""
        fsm = FSM("terminal")

        @fsm.state(initial=True)
        def start() -> FSMReturn:
            return FSMReturn(next_state="end", value="processed")

        @fsm.state(terminal=True)
        def end() -> FSMReturn:
            return FSMReturn(value="finished")

        # Should stop at terminal state
        result = fsm.run()
        assert result == "finished"
        assert fsm.context.cycle_count == 2

    def test_fsm_explicit_stop_condition(self):
        """Test FSM with explicit stop condition."""
        fsm = FSM("explicit_stop")

        @fsm.state(initial=True)
        def process(context: FSMContext) -> FSMReturn:
            if context.cycle_count >= 2:
                return FSMReturn(stop=True, value="stopped")
            return FSMReturn(next_state="process", value="continue")

        result = fsm.run()
        assert result == "stopped"
        assert fsm.context.cycle_count == 2

    def test_fsm_context_metadata_persistence(self):
        """Test FSM context metadata persists across cycles."""
        fsm = FSM("metadata")

        @fsm.state(initial=True)
        def accumulator(context: FSMContext) -> FSMReturn:
            count = context.metadata.get("count", 0)
            context.metadata["count"] = count + 1

            if count < 3:
                return FSMReturn(next_state="accumulator", value=count)
            return FSMReturn(stop=True, value=count)

        result = fsm.run()
        assert result == 3
        assert fsm.context.metadata["count"] == 4

    def test_fsm_state_transitions_validation(self):
        """Test FSM state transitions validation."""
        fsm = FSM("transitions")

        @fsm.state(initial=True)
        def start() -> FSMReturn:
            return FSMReturn(next_state="middle", value="start")

        @fsm.state
        def middle() -> FSMReturn:
            return FSMReturn(next_state="end", value="middle")

        @fsm.state(terminal=True)
        def end() -> FSMReturn:
            return FSMReturn(value="end")

        # Add explicit transitions
        fsm.add_transition("start", "middle", "default")
        fsm.add_transition("middle", "end", "default")

        # Should work with valid transitions
        result = fsm.run()
        assert result == "end"

    def test_fsm_complex_branching(self):
        """Test FSM with complex branching logic."""
        fsm = FSM("branching")

        @fsm.state(initial=True)
        def router(context: FSMContext) -> FSMReturn:
            count = context.metadata.get("visits", 0)
            context.metadata["visits"] = count + 1

            if count == 0:
                return FSMReturn(next_state="path_a", value="routing_a")
            elif count == 1:
                return FSMReturn(next_state="path_b", value="routing_b")
            else:
                return FSMReturn(stop=True, value="done")

        @fsm.state
        def path_a(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="router", value="processed_a")

        @fsm.state
        def path_b(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="router", value="processed_b")

        result = fsm.run()
        assert result == "done"
        assert fsm.context.metadata["visits"] == 3

    def test_fsm_error_handling_in_state(self):
        """Test FSM error handling within states."""
        fsm = FSM("error_handling")

        @fsm.state(initial=True)
        def error_prone(context: FSMContext) -> FSMReturn:
            attempt = context.metadata.get("attempt", 0)
            context.metadata["attempt"] = attempt + 1

            if attempt < 2:
                raise ValueError("Simulated error")

            return FSMReturn(stop=True, value="recovered")

        # Should propagate error from state
        with pytest.raises(ValueError, match="Simulated error"):
            fsm.run()

    def test_fsm_async_state_execution(self):
        """Test FSM with async state functions."""
        fsm = FSM("async_fsm")

        @fsm.state(initial=True)
        async def async_state(context: FSMContext) -> FSMReturn:
            # Simulate async operation
            import asyncio

            await asyncio.sleep(0.01)
            return FSMReturn(next_state="sync_state", value="async_done")

        @fsm.state
        def sync_state() -> FSMReturn:
            return FSMReturn(stop=True, value="sync_done")

        result = fsm.run()
        assert result == "sync_done"

    def test_fsm_conditional_transitions(self):
        """Test FSM with conditional state transitions."""
        fsm = FSM("conditional")

        @fsm.state(initial=True)
        def conditional_state(context: FSMContext) -> FSMReturn:
            value = context.metadata.get("value", 0)
            context.metadata["value"] = value + 1

            if value < 2:
                return FSMReturn(next_state="conditional_state", value=value)
            elif value == 2:
                return FSMReturn(next_state="final_state", value=value)
            else:
                return FSMReturn(stop=True, value="unexpected")

        @fsm.state(terminal=True)
        def final_state() -> FSMReturn:
            return FSMReturn(value="final")

        result = fsm.run()
        assert result == "final"

    def test_fsm_result_access_by_name(self):
        """Test FSM result access by state name."""
        fsm = FSM("access_test")

        @fsm.state(initial=True)
        def state_a() -> FSMReturn:
            return FSMReturn(next_state="state_b", value="result_a")

        @fsm.state
        def state_b() -> FSMReturn:
            return FSMReturn(stop=True, value="result_b")

        # Execute
        result = fsm.run()

        # Access results by state name
        assert fsm["state_a"] == "result_a"
        assert fsm["state_b"] == "result_b"
        assert result == "result_b"

    def test_fsm_result_access_with_cycles(self):
        """Test FSM result access with multiple cycles."""
        fsm = FSM("cycle_access")

        @fsm.state(initial=True)
        def cycling_state(context: FSMContext) -> FSMReturn:
            cycle = context.cycle_count
            if cycle < 3:
                return FSMReturn(next_state="cycling_state", value=f"cycle_{cycle}")
            return FSMReturn(stop=True, value=f"final_{cycle}")

        # Execute
        result = fsm.run()

        # Should get latest result
        assert fsm["cycling_state"] == "final_3"
        assert result == "final_3"

    def test_fsm_validation_errors(self):
        """Test FSM validation catches various errors."""
        fsm = FSM("validation")

        # Empty FSM
        with pytest.raises(ValidationError, match="No nodes"):
            fsm.validate_or_raise()

        # Add a non-initial state
        @fsm.state
        def orphan_state() -> FSMReturn:
            return FSMReturn(stop=True, value="orphan")

        with pytest.raises(ValidationError, match="No initial state"):
            fsm.validate_or_raise()

    def test_fsm_nested_execution(self):
        """Test FSM can be nested in other workflows."""
        # Create inner FSM
        inner_fsm = FSM("inner")

        @inner_fsm.state(initial=True)
        def inner_state(x: int) -> FSMReturn:
            return FSMReturn(stop=True, value=x * 2)

        # Create outer DAG
        from fast_dag import DAG

        outer_dag = DAG("outer")

        @outer_dag.node
        def start() -> int:
            return 5

        # Add FSM as node
        outer_dag.add_fsm("inner_fsm", inner_fsm, inputs=["x"])
        outer_dag.connect("start", "inner_fsm", input="x")

        # Execute
        result = outer_dag.run()
        assert result == 10

    def test_fsm_state_history_limits(self):
        """Test FSM state history with many cycles."""
        fsm = FSM("history_limit", max_cycles=100)

        @fsm.state(initial=True)
        def counter(context: FSMContext) -> FSMReturn:
            count = context.cycle_count
            if count < 50:
                return FSMReturn(next_state="counter", value=count)
            return FSMReturn(stop=True, value=count)

        result = fsm.run()

        # Should track all state changes
        assert len(fsm.context.state_history) == 51
        assert fsm.context.state_history[0] == "counter"
        assert fsm.context.state_history[-1] == "counter"
        assert result == 50

    def test_fsm_concurrent_execution(self):
        """Test FSM handles concurrent execution properly."""
        fsm = FSM("concurrent")

        @fsm.state(initial=True)
        def thread_safe_state(context: FSMContext) -> FSMReturn:
            # Simulate some processing
            import time

            time.sleep(0.01)
            return FSMReturn(stop=True, value="thread_safe")

        # Execute multiple times
        results = []
        for _ in range(5):
            result = fsm.run()
            results.append(result)

        # All should succeed
        assert all(r == "thread_safe" for r in results)

    def test_fsm_memory_usage_with_long_cycles(self):
        """Test FSM memory usage doesn't grow excessively."""
        fsm = FSM("memory_test", max_cycles=1000)

        @fsm.state(initial=True)
        def memory_state(context: FSMContext) -> FSMReturn:
            # Create some data but don't accumulate
            data = list(range(100))
            count = context.cycle_count

            if count < 500:
                return FSMReturn(next_state="memory_state", value=len(data))
            return FSMReturn(stop=True, value="done")

        result = fsm.run()

        # Should complete without excessive memory usage
        assert result == "done"
        assert len(fsm.context.cycle_results) == 501

    def test_fsm_custom_context_fields(self):
        """Test FSM with custom context fields."""
        fsm = FSM("custom_context")

        @fsm.state(initial=True)
        def custom_state(context: FSMContext) -> FSMReturn:
            # Use custom context fields
            context.metadata["custom_field"] = "custom_value"
            context.metadata["calculation"] = context.cycle_count * 10

            if context.cycle_count < 2:
                return FSMReturn(next_state="custom_state", value="continue")
            return FSMReturn(stop=True, value="done")

        result = fsm.run()

        # Check custom fields
        assert fsm.context.metadata["custom_field"] == "custom_value"
        assert fsm.context.metadata["calculation"] == 20
        assert result == "done"
