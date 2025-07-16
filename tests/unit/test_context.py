"""
Unit tests for Context and FSMContext classes.
"""

from fast_dag import Context, FSMContext


class TestContext:
    """Test basic Context functionality"""

    def test_context_creation(self):
        """Test creating a new context"""
        ctx = Context()
        assert ctx.results == {}
        assert ctx.metadata == {}
        assert ctx.metrics == {}

    def test_set_and_get_result(self):
        """Test setting and getting results"""
        ctx = Context()
        ctx.set_result("node1", 42)

        assert ctx.get_result("node1") == 42
        assert ctx.results["node1"] == 42

    def test_dict_like_access(self):
        """Test dictionary-style access to results"""
        ctx = Context()
        ctx.set_result("key", "value")

        # Test __getitem__
        assert ctx["key"] == "value"

        # Test get() with default
        assert ctx.get("key") == "value"
        assert ctx.get("missing", "default") == "default"
        assert ctx.get("missing") is None

    def test_metadata_storage(self):
        """Test storing metadata in context"""
        ctx = Context()
        ctx.metadata["run_id"] = "test-123"
        ctx.metadata["user"] = "test_user"

        assert ctx.metadata["run_id"] == "test-123"
        assert ctx.metadata["user"] == "test_user"

    def test_metrics_storage(self):
        """Test storing metrics in context"""
        ctx = Context()
        ctx.metrics["total_time"] = 1.5
        ctx.metrics["node_times"] = {"node1": 0.5, "node2": 1.0}

        assert ctx.metrics["total_time"] == 1.5
        assert ctx.metrics["node_times"]["node1"] == 0.5

    def test_context_isolation(self):
        """Test that contexts are isolated from each other"""
        ctx1 = Context()
        ctx2 = Context()

        ctx1.set_result("key", "value1")
        ctx2.set_result("key", "value2")

        assert ctx1["key"] == "value1"
        assert ctx2["key"] == "value2"

    def test_contains_operator(self):
        """Test 'in' operator for checking result existence"""
        ctx = Context()
        ctx.set_result("exists", True)

        assert "exists" in ctx
        assert "missing" not in ctx


class TestFSMContext:
    """Test FSM-specific context functionality"""

    def test_fsm_context_creation(self):
        """Test creating an FSM context"""
        ctx = FSMContext()

        # Should have all base Context attributes
        assert ctx.results == {}
        assert ctx.metadata == {}
        assert ctx.metrics == {}

        # Plus FSM-specific attributes
        assert ctx.state_history == []
        assert ctx.cycle_count == 0
        assert ctx.cycle_results == {}

    def test_state_history_tracking(self):
        """Test tracking state history"""
        ctx = FSMContext()

        ctx.state_history.append("state_a")
        ctx.state_history.append("state_b")
        ctx.state_history.append("state_a")

        assert ctx.state_history == ["state_a", "state_b", "state_a"]

    def test_cycle_count_tracking(self):
        """Test tracking cycle count"""
        ctx = FSMContext()

        ctx.cycle_count = 5
        assert ctx.cycle_count == 5

        ctx.cycle_count += 1
        assert ctx.cycle_count == 6

    def test_cycle_results_storage(self):
        """Test storing results per cycle"""
        ctx = FSMContext()

        # Add results for different cycles of the same state
        ctx.add_cycle_result("state_a", {"value": 1})
        ctx.add_cycle_result("state_a", {"value": 2})
        ctx.add_cycle_result("state_b", {"value": 10})

        assert len(ctx.cycle_results["state_a"]) == 2
        assert ctx.cycle_results["state_a"][0] == {"value": 1}
        assert ctx.cycle_results["state_a"][1] == {"value": 2}
        assert ctx.cycle_results["state_b"][0] == {"value": 10}

    def test_get_latest_result(self):
        """Test getting the most recent result for a state"""
        ctx = FSMContext()

        # Add multiple results for a state
        ctx.add_cycle_result("state_a", "first")
        ctx.add_cycle_result("state_a", "second")
        ctx.add_cycle_result("state_a", "latest")

        assert ctx.get_latest("state_a") == "latest"

        # Test with no results
        assert ctx.get_latest("missing") is None

        # Test fallback to regular results
        ctx.set_result("regular_node", "value")
        assert ctx.get_latest("regular_node") == "value"

    def test_get_cycle_result(self):
        """Test getting result from specific cycle"""
        ctx = FSMContext()

        ctx.add_cycle_result("state_a", "cycle0")
        ctx.add_cycle_result("state_a", "cycle1")
        ctx.add_cycle_result("state_a", "cycle2")

        assert ctx.get_cycle("state_a", 0) == "cycle0"
        assert ctx.get_cycle("state_a", 1) == "cycle1"
        assert ctx.get_cycle("state_a", 2) == "cycle2"

        # Test out of bounds
        assert ctx.get_cycle("state_a", 10) is None
        assert ctx.get_cycle("state_a", -1) is None

        # Test missing state
        assert ctx.get_cycle("missing", 0) is None

    def test_fsm_context_inherits_base_functionality(self):
        """Test that FSMContext has all Context functionality"""
        ctx = FSMContext()

        # Test base Context methods work
        ctx.set_result("key", "value")
        assert ctx.get_result("key") == "value"
        assert ctx["key"] == "value"
        assert "key" in ctx

        # Test metadata and metrics
        ctx.metadata["test"] = True
        ctx.metrics["time"] = 1.0

        assert ctx.metadata["test"] is True
        assert ctx.metrics["time"] == 1.0
