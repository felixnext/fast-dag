"""
Tests for context module to improve coverage.
"""

import pytest

from fast_dag.core.context import Context, CycleResults, FSMContext


class TestContextCoverage:
    """Test context classes comprehensively"""

    def test_cycle_results_getitem_with_int(self):
        """Test CycleResults.__getitem__ with int key (lines 26-27)"""
        cycle_results = CycleResults()
        cycle_results.append("value1")
        cycle_results.append("value2")

        assert cycle_results[0] == "value1"
        assert cycle_results[1] == "value2"

    def test_cycle_results_getitem_with_str(self):
        """Test CycleResults.__getitem__ with str key (lines 28-29)"""
        cycle_results = CycleResults()
        cycle_results.add_for_state("state1", "value1")
        cycle_results.add_for_state("state2", "value2")

        assert cycle_results["state1"] == ["value1"]
        assert cycle_results["state2"] == ["value2"]

    def test_cycle_results_getitem_with_invalid_type(self):
        """Test CycleResults.__getitem__ with invalid key type (lines 30-31)"""
        cycle_results = CycleResults()

        with pytest.raises(
            TypeError, match="Key must be int or str, not <class 'float'>"
        ):
            cycle_results[3.14]

    def test_cycle_results_eq_with_list(self):
        """Test CycleResults.__eq__ with list (lines 43-44)"""
        cycle_results = CycleResults()
        cycle_results.append("value1")
        cycle_results.append("value2")

        assert cycle_results == ["value1", "value2"]
        assert cycle_results != ["value1", "value3"]

    def test_cycle_results_eq_with_dict(self):
        """Test CycleResults.__eq__ with dict (lines 45-46)"""
        cycle_results = CycleResults()
        cycle_results.add_for_state("state1", "value1")
        cycle_results.add_for_state("state2", "value2")

        assert cycle_results == {"state1": ["value1"], "state2": ["value2"]}
        assert cycle_results != {"state1": ["value1"], "state2": ["value3"]}

    def test_cycle_results_eq_with_other_type(self):
        """Test CycleResults.__eq__ with other types (line 47)"""
        cycle_results = CycleResults()
        cycle_results.append("value1")

        assert cycle_results != "not a list or dict"
        assert cycle_results != 42
        assert cycle_results is not None

    def test_cycle_results_contains_with_str(self):
        """Test CycleResults.__contains__ with str key (lines 51-52)"""
        cycle_results = CycleResults()
        cycle_results.add_for_state("state1", "value1")

        assert "state1" in cycle_results
        assert "state2" not in cycle_results

    def test_cycle_results_contains_with_non_str(self):
        """Test CycleResults.__contains__ with non-str key (line 53)"""
        cycle_results = CycleResults()
        cycle_results.append("value1")

        # Non-string keys should return False
        assert 0 not in cycle_results
        assert 42 not in cycle_results
        assert None not in cycle_results

    def test_cycle_results_len(self):
        """Test CycleResults.__len__"""
        cycle_results = CycleResults()
        assert len(cycle_results) == 0

        cycle_results.append("value1")
        assert len(cycle_results) == 1

        cycle_results.append("value2")
        assert len(cycle_results) == 2

    def test_cycle_results_iter(self):
        """Test CycleResults.__iter__"""
        cycle_results = CycleResults()
        cycle_results.append("value1")
        cycle_results.append("value2")

        values = list(cycle_results)
        assert values == ["value1", "value2"]

    def test_cycle_results_repr(self):
        """Test CycleResults.__repr__"""
        cycle_results = CycleResults()
        cycle_results.append("value1")
        cycle_results.add_for_state("state1", "value2")

        repr_str = repr(cycle_results)
        assert "CycleResults" in repr_str
        assert "values=" in repr_str
        assert "by_state=" in repr_str

    def test_cycle_results_add_for_state_new_state(self):
        """Test CycleResults.add_for_state with new state"""
        cycle_results = CycleResults()
        cycle_results.add_for_state("new_state", "value1")

        assert "new_state" in cycle_results
        assert cycle_results["new_state"] == ["value1"]

    def test_cycle_results_add_for_state_existing_state(self):
        """Test CycleResults.add_for_state with existing state"""
        cycle_results = CycleResults()
        cycle_results.add_for_state("state1", "value1")
        cycle_results.add_for_state("state1", "value2")

        assert cycle_results["state1"] == ["value1", "value2"]

    def test_context_get_result(self):
        """Test Context.get_result"""
        context = Context()
        context.set_result("key1", "value1")

        assert context.get_result("key1") == "value1"

    def test_context_get_with_default(self):
        """Test Context.get with default value"""
        context = Context()

        assert context.get("missing_key", "default") == "default"
        assert context.get("missing_key") is None

    def test_context_has_result(self):
        """Test Context.has_result"""
        context = Context()
        context.set_result("key1", "value1")

        assert context.has_result("key1") is True
        assert context.has_result("missing_key") is False

    def test_context_getitem(self):
        """Test Context.__getitem__"""
        context = Context()
        context.set_result("key1", "value1")

        assert context["key1"] == "value1"

    def test_context_contains(self):
        """Test Context.__contains__"""
        context = Context()
        context.set_result("key1", "value1")

        assert "key1" in context
        assert "missing_key" not in context

    def test_fsm_context_add_cycle_result_new_node(self):
        """Test FSMContext.add_cycle_result with new node"""
        fsm_context = FSMContext()
        fsm_context.add_cycle_result("node1", "value1")

        # Should be added to both flat list and per-state dictionary
        assert len(fsm_context.cycle_results) == 1
        assert fsm_context.cycle_results[0] == "value1"
        assert "node1" in fsm_context.cycle_results
        assert fsm_context.cycle_results["node1"] == ["value1"]

        # Should also be in state_cycle_results
        assert "node1" in fsm_context.state_cycle_results
        assert fsm_context.state_cycle_results["node1"] == ["value1"]

    def test_fsm_context_add_cycle_result_existing_node(self):
        """Test FSMContext.add_cycle_result with existing node"""
        fsm_context = FSMContext()
        fsm_context.add_cycle_result("node1", "value1")
        fsm_context.add_cycle_result("node1", "value2")

        assert len(fsm_context.cycle_results) == 2
        assert fsm_context.cycle_results["node1"] == ["value1", "value2"]
        assert fsm_context.state_cycle_results["node1"] == ["value1", "value2"]

    def test_fsm_context_get_latest_with_cycle_results(self):
        """Test FSMContext.get_latest with cycle results available"""
        fsm_context = FSMContext()
        fsm_context.add_cycle_result("node1", "value1")
        fsm_context.add_cycle_result("node1", "value2")

        # Should return latest cycle result
        assert fsm_context.get_latest("node1") == "value2"

    def test_fsm_context_get_latest_fallback_to_regular_results(self):
        """Test FSMContext.get_latest fallback to regular results"""
        fsm_context = FSMContext()
        fsm_context.set_result("node1", "regular_value")

        # Should fallback to regular results
        assert fsm_context.get_latest("node1") == "regular_value"

    def test_fsm_context_get_latest_empty_state_cycle_results(self):
        """Test FSMContext.get_latest with empty state cycle results"""
        fsm_context = FSMContext()
        fsm_context.state_cycle_results["node1"] = []  # Empty list
        fsm_context.set_result("node1", "regular_value")

        # Should fallback to regular results when cycle results are empty
        assert fsm_context.get_latest("node1") == "regular_value"

    def test_fsm_context_get_latest_missing_node(self):
        """Test FSMContext.get_latest with missing node"""
        fsm_context = FSMContext()

        # Should return None when node doesn't exist
        assert fsm_context.get_latest("missing_node") is None

    def test_fsm_context_get_cycle_valid_index(self):
        """Test FSMContext.get_cycle with valid index"""
        fsm_context = FSMContext()
        fsm_context.add_cycle_result("node1", "value1")
        fsm_context.add_cycle_result("node1", "value2")

        assert fsm_context.get_cycle("node1", 0) == "value1"
        assert fsm_context.get_cycle("node1", 1) == "value2"

    def test_fsm_context_get_cycle_invalid_index(self):
        """Test FSMContext.get_cycle with invalid index"""
        fsm_context = FSMContext()
        fsm_context.add_cycle_result("node1", "value1")

        # Out of bounds indices should return None
        assert fsm_context.get_cycle("node1", -1) is None
        assert fsm_context.get_cycle("node1", 1) is None
        assert fsm_context.get_cycle("node1", 100) is None

    def test_fsm_context_get_cycle_missing_node(self):
        """Test FSMContext.get_cycle with missing node"""
        fsm_context = FSMContext()

        # Missing node should return None
        assert fsm_context.get_cycle("missing_node", 0) is None
