"""
Tests for FSM core to improve coverage.
"""

from fast_dag import FSM


class TestFSMCoreCoverage:
    """Test FSM core functionality"""

    def test_fsm_getitem(self):
        """Test FSM __getitem__ (line 40)"""
        fsm = FSM("test")

        @fsm.state(initial=True)
        def start() -> str:
            return "end"

        @fsm.state
        def end() -> None:
            pass

        # Run the FSM to create context
        from fast_dag import FSMContext

        context = FSMContext()
        fsm.run(context=context)

        # Test getting a state result
        result = fsm["start"]
        assert result == "end"  # The return value from start state

    def test_fsm_setitem(self):
        """Test FSM __setitem__ - FSM doesn't have setitem, removing test"""
        # FSM doesn't support __setitem__ for states
        # This test was based on incorrect assumptions
        pass

    def test_fsm_contains(self):
        """Test FSM __contains__ - checking nodes instead"""
        fsm = FSM("test")

        @fsm.state(initial=True)
        def start() -> str:
            return "end"

        # FSM uses nodes dict, not direct contains
        assert "start" in fsm.nodes
        assert "missing" not in fsm.nodes

    def test_fsm_validate_no_initial_state(self):
        """Test FSM validate with no initial state (lines 149-151)"""
        fsm = FSM("test")

        @fsm.state
        def state1() -> str:
            return "state2"

        @fsm.state
        def state2() -> None:
            pass

        # No initial state set
        errors = fsm.validate()
        assert any("No initial state defined" in e for e in errors)

    def test_fsm_validate_multiple_initial_states(self):
        """Test FSM validate with multiple initial states (line 156)"""
        fsm = FSM("test")

        @fsm.state(initial=True)
        def start1() -> str:
            return "end"

        @fsm.state(initial=True)
        def start2() -> str:
            return "end"

        @fsm.state
        def end() -> None:
            pass

        errors = fsm.validate()
        assert any("Multiple initial states" in e for e in errors)

    def test_fsm_str(self):
        """Test FSM __str__ (line 169)"""
        fsm = FSM("test_fsm")

        @fsm.state(initial=True)
        def start() -> str:
            return "end"

        @fsm.state
        def end() -> None:
            pass

        # FSM inherits __str__ from DAG
        assert str(fsm) == "DAG(name='test_fsm', nodes=2)"

    def test_fsm_repr(self):
        """Test FSM __repr__ - FSM uses dataclass repr"""
        fsm = FSM("test_fsm")

        # FSM uses dataclass repr
        repr_str = repr(fsm)
        assert "FSM(name='test_fsm'" in repr_str
        assert "nodes={}" in repr_str

        @fsm.state(initial=True)
        def start() -> str:
            return "end"

        @fsm.state
        def end() -> None:
            pass

        # FSM with states and transitions
        fsm.add_transition("start", "end")

        repr_str = repr(fsm)
        assert "nodes={'start':" in repr_str
        assert "'end':" in repr_str
