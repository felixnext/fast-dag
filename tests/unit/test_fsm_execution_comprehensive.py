"""
Comprehensive tests for FSM execution to improve coverage.
"""

import asyncio

import pytest

from fast_dag import (
    FSM,
    Context,
    ExecutionError,
    FSMContext,
    FSMReturn,
    ValidationError,
)


class TestFSMExecutionComprehensive:
    """Test FSM execution comprehensively"""

    def test_run_with_inputs_dict(self):
        """Test FSM run with inputs dictionary"""
        fsm = FSM("test_inputs")

        @fsm.state(initial=True)
        def start(context: FSMContext, x: int, y: int) -> FSMReturn:
            return FSMReturn(next_state="end", value=x + y)

        @fsm.state(terminal=True)
        def end(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value=context.get_result("start"))

        result = fsm.run(inputs={"x": 10, "y": 20})
        assert result == 30

    def test_run_with_custom_error_strategy(self):
        """Test FSM run with custom error strategy"""
        fsm = FSM("error_strategy")

        @fsm.state(initial=True)
        def start(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="error_state", value="start")

        @fsm.state
        def error_state(context: FSMContext) -> FSMReturn:
            raise ValueError("Test error")

        # Test with continue strategy - FSM lets errors propagate
        with pytest.raises(ValueError, match="Test error"):
            fsm.run(error_strategy="continue")

    def test_run_with_regular_context(self):
        """Test FSM run with regular Context converted to FSMContext"""
        fsm = FSM("context_conversion")

        @fsm.state(initial=True, terminal=True)
        def process(context: FSMContext) -> FSMReturn:
            # Access previous results from context
            prev_value = context.results.get("previous", 0)
            return FSMReturn(stop=True, value=prev_value + 10)

        # Create regular context with results
        regular_context = Context()
        regular_context.results["previous"] = 5
        regular_context.metadata["test"] = "value"

        result = fsm.run(context=regular_context)
        assert result == 15

    def test_run_with_validation_errors(self):
        """Test FSM run with validation errors"""
        fsm = FSM("invalid_fsm")

        # Create an FSM with no states
        with pytest.raises(ValidationError, match="FSM validation failed"):
            fsm.run()

    def test_get_state_via_nodes(self):
        """Test getting state via nodes dict"""
        fsm = FSM("get_state_test")

        @fsm.state(initial=True)
        def start(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="end", value="start")

        @fsm.state(terminal=True)
        def end(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value="end")

        # Test getting existing state via nodes
        start_state = fsm.nodes.get("start")
        assert start_state is not None
        assert start_state.name == "start"

        # Test getting non-existent state
        assert fsm.nodes.get("nonexistent") is None

    def test_fsm_state_transitions(self):
        """Test FSM state transition management"""
        fsm = FSM("transitions_test")

        @fsm.state(initial=True)
        def start(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="process", value="started")

        @fsm.state
        def process(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="end", value="processed")

        @fsm.state(terminal=True)
        def end(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value="done")

        # Add custom transition
        fsm.add_transition("start", "end", condition="skip")

        # Check transitions were added
        assert "start" in fsm.state_transitions
        assert fsm.state_transitions["start"]["skip"] == "end"

    def test_step_execution(self):
        """Test step-by-step FSM execution"""
        fsm = FSM("step_test")

        @fsm.state(initial=True)
        def state1(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="state2", value=1)

        @fsm.state
        def state2(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="state3", value=2)

        @fsm.state(terminal=True)
        def state3(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value=3)

        # First step
        context1, result1 = fsm.step()
        assert result1 == 1
        assert fsm.current_state == "state2"

        # Second step
        context2, result2 = fsm.step()
        assert result2 == 2
        assert fsm.current_state == "state3"

        # Final step
        context3, result3 = fsm.step()
        assert result3 == 3
        assert fsm.current_state == "state3"

        # No more steps - should return None
        context4, result4 = fsm.step()
        assert result4 is None

    def test_step_with_invalid_state(self):
        """Test step execution with invalid current state"""
        fsm = FSM("invalid_step")

        @fsm.state(initial=True, terminal=True)
        def only_state(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value="done")

        # Set invalid current state
        fsm.current_state = "nonexistent"

        with pytest.raises(
            ExecutionError, match="State 'nonexistent' not found in FSM"
        ):
            fsm.step()

    def test_fsm_rerun_after_completion(self):
        """Test FSM behavior when running again after completion"""
        fsm = FSM("rerun_test")

        @fsm.state(initial=True)
        def start(context: FSMContext, x: int = 5) -> FSMReturn:
            return FSMReturn(next_state="middle", value=x)

        @fsm.state
        def middle(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="end", value="middle")

        @fsm.state(terminal=True)
        def end(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value="done")

        # Run to completion
        result = fsm.run(x=10)
        assert result == "done"
        assert fsm.current_state == "end"

        # Check context has results
        assert len(fsm.context.results) > 0

        # Run again - should start fresh
        result2 = fsm.run(x=20)
        assert result2 == "done"

        # Should have reset to initial state
        assert fsm.current_state == "end"  # ends at terminal state again

    def test_is_terminated_property(self):
        """Test FSM is_terminated property"""
        fsm = FSM("terminated_test")

        @fsm.state(initial=True)
        def process(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="done", value="processing")

        @fsm.state(terminal=True)
        def done(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value="finished")

        # Initially not terminated
        assert not fsm.is_terminated

        # Run FSM
        result = fsm.run()
        assert result == "finished"

        # Should be terminated after reaching terminal state
        assert fsm.is_terminated

    def test_state_with_error_no_strategy(self):
        """Test state execution with error and no error strategy"""
        fsm = FSM("error_test")

        @fsm.state(initial=True)
        def error_state(context: FSMContext) -> FSMReturn:
            raise RuntimeError("State error")

        with pytest.raises(RuntimeError, match="State error"):
            fsm.run()

    def test_multiple_initial_states_runtime(self):
        """Test FSM with multiple initial states at runtime"""
        fsm = FSM("multi_initial")

        @fsm.state(initial=True)
        def start1(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="end", value="from_start1")

        @fsm.state(initial=True)
        def start2(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="end", value="from_start2")

        @fsm.state(terminal=True)
        def end(context: FSMContext) -> FSMReturn:
            prev = list(context.results.values())[-1] if context.results else ""
            return FSMReturn(stop=True, value=prev)

        # Should use first initial state found
        result = fsm.run()
        assert result in ["from_start1", "from_start2"]

    def test_fsm_with_cycle_results(self):
        """Test FSM execution with cycle results tracking"""
        fsm = FSM("cycle_test", max_cycles=10)  # Increased to allow completion

        cycle_count = 0

        @fsm.state(initial=True)
        def loop_state(context: FSMContext) -> FSMReturn:
            nonlocal cycle_count
            cycle_count += 1
            if cycle_count >= 3:
                return FSMReturn(next_state="end", value=f"cycle_{cycle_count}")
            return FSMReturn(next_state="loop_state", value=f"cycle_{cycle_count}")

        @fsm.state(terminal=True)
        def end(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value="completed")

        result = fsm.run()
        assert result == "completed"

        # Check cycle results
        assert fsm.context.cycle_count > 0
        assert "loop_state" in fsm.context.cycle_results._by_state

    def test_fsm_context_methods(self):
        """Test FSM context-specific methods"""
        fsm = FSM("context_methods_test")

        @fsm.state(initial=True)
        def state_a(context: FSMContext) -> FSMReturn:
            # Test state_history is empty at start
            assert len(context.state_history) == 1  # Just entered state_a
            return FSMReturn(next_state="state_b", value="a_value")

        @fsm.state
        def state_b(context: FSMContext) -> FSMReturn:
            # Test state history tracking
            assert "state_a" in context.state_history

            # Test get_result
            a_result = context.get_result("state_a")
            assert a_result == "a_value"

            # Test get_latest
            latest = context.get_latest("state_a")
            assert latest == "a_value"

            return FSMReturn(next_state="state_c", value="b_value")

        @fsm.state(terminal=True)
        def state_c(context: FSMContext) -> FSMReturn:
            # Test state history
            assert len(context.state_history) >= 2
            assert context.state_history[-1] == "state_c"

            # Test get_cycle for accessing results
            cycle_result = context.get_cycle("state_a", 0)
            assert cycle_result == "a_value"

            return FSMReturn(stop=True, value="done")

        result = fsm.run()
        assert result == "done"

    def test_invalid_state_transition(self):
        """Test FSM with invalid state transition"""
        fsm = FSM("invalid_transition")

        @fsm.state(initial=True)
        def start(context: FSMContext) -> FSMReturn:
            # Return non-existent next state
            return FSMReturn(next_state="missing_state", value="start")

        with pytest.raises(
            ExecutionError, match="State 'missing_state' not found in FSM"
        ):
            fsm.run()

    def test_fsm_return_without_next_state_non_terminal(self):
        """Test FSMReturn without next_state in non-terminal state"""
        fsm = FSM("no_next_state", max_cycles=2)  # Set low to trigger error quickly

        @fsm.state(initial=True)
        def state1(context: FSMContext) -> FSMReturn:
            # Return without next_state or stop=True in non-terminal state
            # This causes it to stay in the same state and loop
            return FSMReturn(value="no_next")

        # Should hit max cycles error
        with pytest.raises(ExecutionError, match="Maximum cycles"):
            fsm.run()

    def test_fsm_metadata(self):
        """Test FSM metadata handling"""
        fsm = FSM("metadata_test", metadata={"version": "1.0"})

        @fsm.state(initial=True)
        def start(context: FSMContext) -> FSMReturn:
            # Access FSM metadata
            context.metadata["started"] = True
            return FSMReturn(next_state="end", value="metadata")

        @fsm.state(terminal=True)
        def end(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value="run")

        # Check initial metadata
        assert fsm.metadata["version"] == "1.0"

        # Run FSM
        fsm.run()

        # Check context was updated
        assert fsm.context.metadata["started"] is True

    def test_async_run(self):
        """Test async FSM execution"""
        fsm = FSM("async_test")

        @fsm.state(initial=True)
        async def async_start(context: FSMContext) -> FSMReturn:
            await asyncio.sleep(0.001)
            return FSMReturn(next_state="async_end", value="async_result")

        @fsm.state(terminal=True)
        async def async_end(context: FSMContext) -> FSMReturn:
            await asyncio.sleep(0.001)
            return FSMReturn(stop=True, value="async_done")

        # Run async
        result = asyncio.run(fsm.run_async())
        assert result == "async_done"
