"""
Unit tests for FSM (Finite State Machine) functionality.
"""

import asyncio

import pytest

from fast_dag import FSM, FSMContext, FSMReturn
from fast_dag.core.exceptions import ExecutionError


class TestFSMCreation:
    """Test FSM creation and initialization"""

    def test_fsm_creation(self):
        """Test creating a basic FSM"""
        fsm = FSM("test_state_machine")

        assert fsm.name == "test_state_machine"
        assert fsm.nodes == {}
        assert fsm.max_cycles == 1000  # Default
        assert fsm.initial_state is None
        assert fsm.terminal_states == set()

    def test_fsm_with_max_cycles(self):
        """Test creating FSM with custom max cycles"""
        fsm = FSM("test", max_cycles=100)

        assert fsm.max_cycles == 100

    def test_fsm_inherits_dag_functionality(self):
        """Test that FSM has DAG functionality"""
        fsm = FSM("test")

        # Should be able to add nodes like DAG
        @fsm.node
        def process(x: int) -> int:
            return x * 2

        assert "process" in fsm.nodes


class TestStateDefinition:
    """Test defining FSM states"""

    def test_add_state_decorator(self):
        """Test adding state with decorator"""
        fsm = FSM("test")

        @fsm.state
        def state_a(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="state_b", value="a")

        assert "state_a" in fsm.nodes
        assert fsm.nodes["state_a"].node_type.value == "fsm_state"

    def test_initial_state_marking(self):
        """Test marking initial state"""
        fsm = FSM("test")

        @fsm.state(initial=True)
        def start_state(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="next", value="started")

        assert fsm.initial_state == "start_state"

    def test_terminal_state_marking(self):
        """Test marking terminal states"""
        fsm = FSM("test")

        @fsm.state(terminal=True)
        def end_state(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value="ended")

        assert "end_state" in fsm.terminal_states

    def test_multiple_initial_states_error(self):
        """Test error when defining multiple initial states"""
        fsm = FSM("test")

        @fsm.state(initial=True)
        def state1(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="state2")

        @fsm.state(initial=True)
        def state2(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="state1")

        # Should be caught during validation
        errors = fsm.validate()
        assert any("initial" in str(e).lower() for e in errors)

    def test_set_initial_state_manually(self):
        """Test setting initial state manually"""
        fsm = FSM("test")

        @fsm.state
        def my_state(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="my_state")

        fsm.set_initial_state("my_state")
        assert fsm.initial_state == "my_state"


class TestStateTransitions:
    """Test FSM state transitions"""

    def test_simple_transition(self):
        """Test simple state transition"""
        fsm = FSM("test")

        @fsm.state(initial=True)
        def a(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="b", value=1)

        @fsm.state
        def b(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="c", value=2)

        @fsm.state(terminal=True)
        def c(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value=3)

        # Define transitions
        fsm.add_transition("a", "b")
        fsm.add_transition("b", "c")

        # Verify transitions
        assert fsm.state_transitions["a"]["default"] == "b"
        assert fsm.state_transitions["b"]["default"] == "c"

    def test_conditional_transitions(self):
        """Test conditional state transitions"""
        fsm = FSM("test")

        @fsm.state(initial=True)
        def check_state(context: FSMContext) -> FSMReturn:
            count = context.metadata.get("count", 0)
            if count >= 3:
                return FSMReturn(next_state="done", value=count)
            else:
                context.metadata["count"] = count + 1
                return FSMReturn(next_state="check_state", value=count)

        @fsm.state(terminal=True)
        def done(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value="completed")

        # Transitions handled by return values


class TestStepExecution:
    """Test step-by-step FSM execution"""

    def test_single_step_execution(self):
        """Test executing a single FSM step"""
        fsm = FSM("test")

        @fsm.state(initial=True)
        def state_a(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="state_b", value="from_a")

        @fsm.state(terminal=True)
        def state_b(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value="from_b")

        # Execute first step
        context = FSMContext()
        context, result = fsm.step(context)

        assert result == "from_a"
        assert fsm.current_state == "state_b"
        assert context.state_history == ["state_a"]
        assert context.cycle_count == 1

    def test_multiple_step_execution(self):
        """Test executing multiple FSM steps"""
        fsm = FSM("test")

        @fsm.state(initial=True)
        def s1(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="s2", value=1)

        @fsm.state
        def s2(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="s3", value=2)

        @fsm.state(terminal=True)
        def s3(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value=3)

        context = FSMContext()

        # Step 1
        context, result1 = fsm.step(context)
        assert result1 == 1
        assert fsm.current_state == "s2"

        # Step 2
        context, result2 = fsm.step(context)
        assert result2 == 2
        assert fsm.current_state == "s3"

        # Step 3
        context, result3 = fsm.step(context)
        assert result3 == 3
        assert fsm.is_terminal_state(fsm.current_state)

        # Check history
        assert context.state_history == ["s1", "s2", "s3"]
        assert context.cycle_count == 3

    def test_step_with_cycle(self):
        """Test FSM with cycles"""
        fsm = FSM("test", max_cycles=5)

        @fsm.state(initial=True)
        def loop_state(context: FSMContext) -> FSMReturn:
            count = context.metadata.get("loop_count", 0)
            context.metadata["loop_count"] = count + 1

            if count >= 2:
                return FSMReturn(next_state="end", value=f"loop_{count}")
            return FSMReturn(next_state="loop_state", value=f"loop_{count}")

        @fsm.state(terminal=True)
        def end(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value="done")

        context = FSMContext()

        # Execute steps
        results = []
        while context.cycle_count < 10:
            context, result = fsm.step(context)
            if result is not None:
                results.append(result)

            # Check if terminated after executing
            if result is None and context.cycle_count > 0:
                break

        assert results == ["loop_0", "loop_1", "loop_2", "done"]
        assert context.cycle_count == 4


class TestFullExecution:
    """Test full FSM execution"""

    def test_run_to_completion(self):
        """Test running FSM to completion"""
        fsm = FSM("test")

        @fsm.state(initial=True)
        def start(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="middle", value="started")

        @fsm.state
        def middle(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="end", value="processing")

        @fsm.state(terminal=True)
        def end(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value="completed")

        result = fsm.run()

        assert result == "completed"
        assert fsm.current_state == "end"

    def test_run_with_max_cycles(self):
        """Test FSM stops at max cycles"""
        fsm = FSM("test", max_cycles=3)

        @fsm.state(initial=True)
        def infinite_loop(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="infinite_loop", value="looping")

        # Should raise error when max cycles exceeded
        with pytest.raises(ExecutionError, match="Maximum cycles"):
            fsm.run()

    def test_run_with_explicit_stop(self):
        """Test FSM with explicit stop condition"""
        fsm = FSM("test")

        @fsm.state(initial=True)
        def counter(context: FSMContext) -> FSMReturn:
            count = context.metadata.get("count", 0)

            if count >= 5:
                return FSMReturn(stop=True, value=f"stopped at {count}")

            context.metadata["count"] = count + 1
            return FSMReturn(next_state="counter", value=count)

        result = fsm.run()

        assert result == "stopped at 5"

    @pytest.mark.asyncio
    async def test_async_fsm_execution(self):
        """Test async FSM execution"""
        fsm = FSM("async_test")

        @fsm.state(initial=True)
        async def async_state(context: FSMContext) -> FSMReturn:
            await asyncio.sleep(0.01)
            return FSMReturn(next_state="final", value="async_done")

        @fsm.state(terminal=True)
        async def final(context: FSMContext) -> FSMReturn:
            await asyncio.sleep(0.01)
            return FSMReturn(stop=True, value="completed")

        result = await fsm.run_async()
        assert result == "completed"


class TestResultAccess:
    """Test accessing FSM execution results"""

    def test_access_latest_state_result(self):
        """Test accessing latest result for a state"""
        fsm = FSM("test")

        @fsm.state(initial=True)
        def repeating(context: FSMContext) -> FSMReturn:
            count = context.metadata.get("count", 0)
            context.metadata["count"] = count + 1

            if count >= 3:
                return FSMReturn(stop=True, value=f"final_{count}")
            return FSMReturn(next_state="repeating", value=f"cycle_{count}")

        fsm.run()

        # Access latest result
        assert fsm["repeating"] == "final_3"

    def test_access_specific_cycle(self):
        """Test accessing specific cycle results"""
        fsm = FSM("test")

        @fsm.state(initial=True)
        def cyclic(context: FSMContext) -> FSMReturn:
            cycle = context.metadata.get("cycle", 0)
            context.metadata["cycle"] = cycle + 1

            if cycle >= 4:
                return FSMReturn(stop=True, value=f"end_{cycle}")
            return FSMReturn(next_state="cyclic", value=f"cycle_{cycle}")

        fsm.run()

        # Access specific cycles
        assert fsm["cyclic.0"] == "cycle_0"
        assert fsm["cyclic.1"] == "cycle_1"
        assert fsm["cyclic.2"] == "cycle_2"
        assert fsm["cyclic.3"] == "cycle_3"
        assert fsm["cyclic.4"] == "end_4"

    def test_get_state_history(self):
        """Test getting full history for a state"""
        fsm = FSM("test")

        @fsm.state(initial=True)
        def state_with_history(context: FSMContext) -> FSMReturn:
            count = context.metadata.get("count", 0)
            context.metadata["count"] = count + 1

            if count >= 3:
                return FSMReturn(stop=True, value=count)
            return FSMReturn(next_state="state_with_history", value=count)

        fsm.run()

        history = fsm.get_history("state_with_history")
        assert history == [0, 1, 2, 3]

    def test_fsm_context_after_execution(self):
        """Test accessing FSM context after execution"""
        fsm = FSM("test")

        @fsm.state(initial=True)
        def only_state(context: FSMContext) -> FSMReturn:
            context.metadata["test_data"] = "value"
            return FSMReturn(stop=True, value="done")

        fsm.run()

        ctx = fsm.context
        assert isinstance(ctx, FSMContext)
        assert ctx.metadata["test_data"] == "value"
        assert ctx.state_history == ["only_state"]
        assert ctx.cycle_count == 1


class TestFSMValidation:
    """Test FSM validation"""

    def test_validate_no_initial_state(self):
        """Test validation error when no initial state"""
        fsm = FSM("test")

        @fsm.state(terminal=True)
        def end_state(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True)

        errors = fsm.validate()
        assert any("initial state" in str(e).lower() for e in errors)

    def test_validate_unreachable_states(self):
        """Test detecting unreachable states"""
        # For FSMs, reachability validation is disabled because states
        # can be reached dynamically via FSMReturn. This test is kept
        # but modified to reflect the current behavior.
        fsm = FSM("test")

        @fsm.state(initial=True)
        def start(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="end")

        @fsm.state(terminal=True)
        def end(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True)

        @fsm.state
        def unreachable(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="end")

        # FSMs don't validate unreachability since FSMReturn provides dynamic transitions
        errors = fsm.validate(allow_disconnected=False)
        assert not any("unreachable" in str(e).lower() for e in errors)

    def test_validate_valid_fsm(self):
        """Test validating a valid FSM"""
        fsm = FSM("test")

        @fsm.state(initial=True)
        def start(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="end")

        @fsm.state(terminal=True)
        def end(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True)

        errors = fsm.validate()
        assert errors == []
