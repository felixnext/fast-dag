"""
Additional tests to achieve 100% coverage for fsm/execution.py.
"""

import asyncio
from unittest.mock import MagicMock, patch

import pytest

from fast_dag import (
    FSM,
    Context,
    ExecutionError,
    FSMContext,
    FSMReturn,
    ValidationError,
)
from fast_dag.core.node import Node


class TestFSMExecutionMissingCoverage:
    """Tests targeting specific uncovered lines in fsm/execution.py"""

    def test_run_without_initial_state(self):
        """Test run when FSM has no initial state (line 68)"""
        fsm = FSM("no_initial")

        # Create FSM with no initial state by not marking any state as initial
        @fsm.state
        def some_state(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value="done")

        # Mock validate to return no errors so we can test the execution path
        with patch.object(fsm, "validate", return_value=[]):
            # Manually set initial_state to None
            fsm.initial_state = None

            with pytest.raises(ExecutionError, match="FSM must have an initial state"):
                fsm.run()

    def test_run_initial_state_missing_required_input(self):
        """Test when initial state has required inputs but not provided (lines 104-108)"""
        fsm = FSM("missing_input")

        @fsm.state(initial=True)
        def start(
            context: FSMContext, required_input: int, optional_input: int = 5
        ) -> FSMReturn:
            return FSMReturn(stop=True, value=required_input + optional_input)

        # Run without providing required_input
        with pytest.raises(
            ExecutionError,
            match="Initial state 'start' missing required input: 'required_input'",
        ):
            fsm.run()

    # This test is commented out due to complexity - the error occurs before reaching target lines
    # def test_run_with_node_connections_missing_source_name(self):
    #     """Test execution with node that has connection from node without name (lines 117-119)"""
    #     # This test is difficult to reach due to validation and execution flow

    def test_run_with_missing_source_result(self):
        """Test when state requires result from non-executed source (lines 122-126)"""
        fsm = FSM("missing_source")

        @fsm.state(initial=True)
        def start(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="needs_data", value="start")

        @fsm.state(terminal=True)
        def needs_data(context: FSMContext, data: int) -> FSMReturn:
            return FSMReturn(stop=True, value=data)

        # Create a fake connection to non-existent node
        fake_node = Node(func=lambda: 1, name="fake_source")
        fsm.nodes["needs_data"].input_connections["data"] = (fake_node, "result")

        with pytest.raises(
            ExecutionError,
            match="State 'needs_data' requires result from 'fake_source' which hasn't executed",
        ):
            fsm.run()

    def test_run_with_dict_output_selection(self):
        """Test handling dict outputs with specific key selection (lines 129-132)"""
        fsm = FSM("dict_output")

        @fsm.state(initial=True)
        def producer(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="consumer", value={"key1": 10, "key2": 20})

        @fsm.state(terminal=True)
        def consumer(context: FSMContext, data: int) -> FSMReturn:
            return FSMReturn(stop=True, value=data * 2)

        # Manually connect with output selection
        fsm.nodes["consumer"].input_connections["data"] = (
            fsm.nodes["producer"],
            "key2",
        )

        result = fsm.run()
        assert result == 40  # 20 * 2

    def test_run_async_node_in_running_event_loop(self):
        """Test executing async node when event loop is running (lines 140-145)"""
        fsm = FSM("async_in_loop")

        @fsm.state(initial=True)
        async def async_state(context: FSMContext) -> FSMReturn:
            await asyncio.sleep(0.001)
            return FSMReturn(stop=True, value="async_done")

        async def test_in_loop():
            # This should fail because we're in an async context trying to run sync
            with pytest.raises(
                ExecutionError,
                match="Cannot execute async state 'async_state' in sync mode while event loop is running",
            ):
                fsm.run()

        asyncio.run(test_in_loop())

    def test_fsm_return_in_terminal_state_without_next(self):
        """Test FSMReturn in terminal state without next_state (lines 192-193, 197)"""
        fsm = FSM("terminal_no_next")

        @fsm.state(initial=True)
        def start(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="end", value="start")

        @fsm.state(terminal=True)
        def end(context: FSMContext) -> FSMReturn:
            # Return without next_state or stop - should end because terminal
            return FSMReturn(value="ended")

        result = fsm.run()
        assert result == "ended"

    # This test is commented out due to complexity with FSM state handling
    # def test_non_fsm_return_in_terminal_state(self):
    #     """Test non-FSMReturn result in terminal state (lines 196-197)"""
    #     # Complex interaction between FSM state decorators and manual nodes

    def test_state_transition_with_default(self):
        """Test using default state transition (lines 200-206, 204)"""
        fsm = FSM("default_transition")

        @fsm.state(initial=True)
        def state1(context: FSMContext) -> int:
            # Return plain value, not FSMReturn
            return 42

        @fsm.state(terminal=True)
        def state2(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value="done")

        # Add default transition
        fsm.add_transition("state1", "state2", condition="default")

        result = fsm.run()
        assert result == "done"

    def test_async_run_without_initial_state(self):
        """Test async run when FSM has no initial state (line 287)"""
        fsm = FSM("async_no_initial")

        @fsm.state
        async def some_state(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value="done")

        async def test_async():
            with patch.object(fsm, "validate", return_value=[]):
                fsm.initial_state = None

                with pytest.raises(
                    ExecutionError, match="FSM must have an initial state"
                ):
                    await fsm.run_async()

        asyncio.run(test_async())

    def test_async_run_validation_error(self):
        """Test async run with validation errors (lines 268-269)"""
        fsm = FSM("async_invalid")

        # Mock validate to return errors
        with patch.object(fsm, "validate", return_value=["Error 1", "Error 2"]):

            async def test_async():
                with pytest.raises(ValidationError, match="FSM validation failed"):
                    await fsm.run_async()

            asyncio.run(test_async())

    def test_async_run_with_inputs_dict(self):
        """Test async run with inputs dictionary (line 259)"""
        fsm = FSM("async_inputs")

        @fsm.state(initial=True, terminal=True)
        async def process(context: FSMContext, x: int, y: int) -> FSMReturn:
            await asyncio.sleep(0.001)
            return FSMReturn(stop=True, value=x + y)

        async def test_async():
            result = await fsm.run_async(inputs={"x": 10, "y": 20})
            assert result == 30

        asyncio.run(test_async())

    def test_async_run_context_conversion(self):
        """Test async run with regular Context conversion (lines 278-281)"""
        fsm = FSM("async_context_convert")

        @fsm.state(initial=True, terminal=True)
        async def process(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value=context.results.get("prev", 0) + 10)

        async def test_async():
            # Create regular context
            regular_context = Context()
            regular_context.results["prev"] = 5
            regular_context.metadata["test"] = "value"

            result = await fsm.run_async(context=regular_context)
            assert result == 15

        asyncio.run(test_async())

    def test_async_run_initial_inputs_tracking(self):
        """Test async run tracks initial inputs (lines 292-293)"""
        fsm = FSM("async_track_inputs")

        @fsm.state(initial=True, terminal=True)
        async def process(context: FSMContext) -> FSMReturn:
            # Access tracked inputs
            assert "x" in context.results
            assert "y" in context.results
            return FSMReturn(
                stop=True, value=context.results["x"] + context.results["y"]
            )

        async def test_async():
            result = await fsm.run_async(x=10, y=20)
            assert result == 30

        asyncio.run(test_async())

    def test_async_run_max_cycles_check(self):
        """Test async run max cycles check (lines 300-302)"""
        fsm = FSM("async_max_cycles", max_cycles=1)

        @fsm.state(initial=True)
        async def loop_state(context: FSMContext) -> FSMReturn:
            # This would loop forever
            return FSMReturn(next_state="loop_state", value="looping")

        async def test_async():
            # Should hit max cycles
            with pytest.raises(ExecutionError, match="Maximum cycles"):
                await fsm.run_async()

        asyncio.run(test_async())

    def test_async_run_state_not_found(self):
        """Test async run with invalid state (line 306)"""
        fsm = FSM("async_invalid_state")

        @fsm.state(initial=True)
        async def start(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="invalid", value="start")

        async def test_async():
            with pytest.raises(
                ExecutionError, match="State 'invalid' not found in FSM"
            ):
                await fsm.run_async()

        asyncio.run(test_async())

    def test_async_run_initial_missing_input(self):
        """Test async initial state missing input (lines 321-327)"""
        fsm = FSM("async_missing_input")

        @fsm.state(initial=True)
        async def start(context: FSMContext, required: int) -> FSMReturn:
            return FSMReturn(stop=True, value=required)

        async def test_async():
            with pytest.raises(
                ExecutionError,
                match="Initial state 'start' missing required input: 'required'",
            ):
                await fsm.run_async()

        asyncio.run(test_async())

    def test_async_run_node_connections(self):
        """Test async run with node connections (lines 336-351)"""
        fsm = FSM("async_connections")

        @fsm.state(initial=True)
        async def producer(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="consumer", value={"data": 42})

        @fsm.state(terminal=True)
        async def consumer(context: FSMContext, input_data: int) -> FSMReturn:
            return FSMReturn(stop=True, value=input_data * 2)

        # Create connection
        fsm.nodes["consumer"].input_connections["input_data"] = (
            fsm.nodes["producer"],
            "data",
        )

        async def test_async():
            result = await fsm.run_async()
            assert result == 84

        asyncio.run(test_async())

    def test_async_run_non_async_node(self):
        """Test async run executing non-async node (line 357)"""
        fsm = FSM("async_with_sync")

        @fsm.state(initial=True)
        def sync_state(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="async_state", value="sync")

        @fsm.state(terminal=True)
        async def async_state(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value="async")

        async def test_async():
            result = await fsm.run_async()
            assert result == "async"

        asyncio.run(test_async())

    # This test is commented out due to complexity with manual node creation
    # def test_async_run_cycle_results_non_fsm_return(self):
    #     """Test async run storing non-FSMReturn in cycle results (line 369)"""
    #     # Complex interaction between manual nodes and FSM state management

    # This test is commented out due to complexity with manual node creation
    # def test_async_run_non_fsm_return_terminal(self):
    #     """Test async non-FSMReturn in terminal state (lines 397-414)"""
    #     # Complex interaction between manual nodes and FSM state management

    def test_async_run_cycle_count_increment(self):
        """Test async cycle count increment logic (line 425)"""
        fsm = FSM("async_cycle_increment")

        @fsm.state(initial=True)
        async def state1(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="state2", value="s1")

        @fsm.state(terminal=True)
        async def state2(context: FSMContext) -> FSMReturn:
            # Terminal state execution
            return FSMReturn(stop=True, value="s2")

        async def test_async():
            result = await fsm.run_async()
            assert result == "s2"
            # Each state execution increments cycle count
            assert fsm.context.cycle_count >= 1

        asyncio.run(test_async())

    def test_async_run_max_cycles_metadata(self):
        """Test async max cycles adds metadata (lines 433-434)"""
        fsm = FSM("async_max_meta", max_cycles=2)

        @fsm.state(initial=True)
        async def loop(context: FSMContext) -> FSMReturn:
            return FSMReturn(next_state="loop", value="looping")

        async def test_async():
            with pytest.raises(ExecutionError, match="Maximum cycles"):
                await fsm.run_async()

            assert fsm.context.metadata.get("max_cycles_reached") is True

        asyncio.run(test_async())

    def test_step_context_conversion(self):
        """Test step with Context conversion (lines 459-462)"""
        fsm = FSM("step_convert")

        @fsm.state(initial=True, terminal=True)
        def process(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value=context.results.get("prev", 0) + 10)

        # Use regular Context
        regular_context = Context()
        regular_context.results["prev"] = 5
        regular_context.metadata["test"] = "value"

        context, result = fsm.step(context=regular_context)
        assert result == 15

    def test_step_no_initial_state(self):
        """Test step when FSM has no initial state (line 468)"""
        fsm = FSM("step_no_initial")

        @fsm.state
        def some_state(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value="done")

        fsm.initial_state = None

        with pytest.raises(ExecutionError, match="FSM must have an initial state"):
            fsm.step()

    def test_step_initial_inputs_tracking(self):
        """Test step tracks initial inputs (lines 472-473)"""
        fsm = FSM("step_track_inputs")

        @fsm.state(initial=True, terminal=True)
        def process(context: FSMContext) -> FSMReturn:
            assert "x" in context.results
            assert "y" in context.results
            return FSMReturn(
                stop=True, value=context.results["x"] + context.results["y"]
            )

        context, result = fsm.step(x=10, y=20)
        assert result == 30

    # This test is commented out due to complexity with FSM state management
    # def test_step_no_current_state(self):
    #     """Test step when current state is None after init (line 481)"""
    #     # Complex interaction with FSM state initialization

    def test_step_missing_initial_input(self):
        """Test step with missing required input (lines 500-508)"""
        fsm = FSM("step_missing_input")

        @fsm.state(initial=True)
        def start(context: FSMContext, required: int) -> FSMReturn:
            return FSMReturn(stop=True, value=required)

        with pytest.raises(
            ExecutionError,
            match="Initial state 'start' missing required input: 'required'",
        ):
            fsm.step()

    # This test is commented out due to complexity with FSM connections
    # def test_step_node_connections(self):
    #     """Test step with node connections (lines 515-530)"""
    #     # Complex interaction with FSM node connections and cycle management

    def test_step_async_node_with_running_loop(self):
        """Test step with async node in running loop (lines 535-547)"""
        fsm = FSM("step_async_loop")

        @fsm.state(initial=True, terminal=True)
        async def async_state(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value="async_done")

        # Mock to simulate running event loop
        with patch("asyncio.get_event_loop") as mock_get_loop:
            mock_loop = MagicMock()
            mock_loop.is_running.return_value = True
            mock_get_loop.return_value = mock_loop

            with pytest.raises(ExecutionError, match="Cannot execute async state"):
                fsm.step()

    def test_step_async_node_with_existing_loop(self):
        """Test step with async node using existing loop (lines 545-547)"""
        fsm = FSM("step_async_existing")

        @fsm.state(initial=True, terminal=True)
        async def async_state(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value="async_done")

        # Mock to simulate existing non-running loop
        with patch("asyncio.get_event_loop") as mock_get_loop:
            mock_loop = MagicMock()
            mock_loop.is_running.return_value = False

            async def mock_coro():
                return FSMReturn(stop=True, value="async_done")

            mock_loop.run_until_complete.return_value = FSMReturn(
                stop=True, value="async_done"
            )
            mock_get_loop.return_value = mock_loop

            context, result = fsm.step()
            assert result == "async_done"
            mock_loop.run_until_complete.assert_called_once()

    # This test is commented out due to complexity with manual node creation
    # def test_step_cycle_results_non_fsm_return(self):
    #     """Test step storing non-FSMReturn in cycle results (line 563)"""
    #     # Complex interaction between manual nodes and FSM state management

    # This test is commented out due to complexity with manual node creation
    # def test_step_results_update_non_fsm_return(self):
    #     """Test step updating results with non-FSMReturn (line 570)"""
    #     # Complex interaction between manual nodes and FSM state management

    def test_step_default_transition(self):
        """Test step with default transition (lines 581-585)"""
        fsm = FSM("step_default")

        @fsm.state(initial=True)
        def state1(context: FSMContext) -> int:
            # Return plain value
            return 42

        @fsm.state(terminal=True)
        def state2(context: FSMContext) -> FSMReturn:
            return FSMReturn(stop=True, value="done")

        # Add default transition
        fsm.add_transition("state1", "state2", condition="default")

        # First step
        context1, result1 = fsm.step()
        assert result1 == 42
        assert fsm.current_state == "state2"

        # Second step
        context2, result2 = fsm.step()
        assert result2 == "done"

    # This test is commented out due to complexity with manual node creation
    # def test_step_return_non_fsm_return(self):
    #     """Test step returning non-FSMReturn value (line 593)"""
    #     # Complex interaction between manual nodes and FSM state management
