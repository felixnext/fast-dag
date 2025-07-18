"""
Tests for node operators to improve coverage.
"""

from fast_dag import DAG


class TestNodeOperatorsCoverage:
    """Test node operators comprehensively"""

    def test_rrshift_with_non_list(self):
        """Test __rrshift__ with non-list input (line 62)"""
        dag = DAG("rrshift_test")

        @dag.node
        def task1() -> int:
            return 1

        @dag.node
        def task2() -> int:
            return 2

        node1 = dag.nodes["task1"]
        node2 = dag.nodes["task2"]

        # Test the __rrshift__ method with a non-list input
        # This should return NotImplemented
        result = node2.__rrshift__(node1)  # single node instead of list
        assert result is NotImplemented

    def test_rshift_with_list(self):
        """Test __rshift__ with list of nodes"""
        dag = DAG("rshift_list_test")

        @dag.node
        def source() -> int:
            return 1

        @dag.node
        def target1(x: int) -> int:
            return x + 1

        @dag.node
        def target2(x: int) -> int:
            return x + 2

        source_node = dag.nodes["source"]
        target_nodes = [dag.nodes["target1"], dag.nodes["target2"]]

        # Test connecting to multiple targets
        result = source_node >> target_nodes
        assert result == target_nodes

    def test_rshift_with_single_node(self):
        """Test __rshift__ with single node"""
        dag = DAG("rshift_single_test")

        @dag.node
        def source() -> int:
            return 1

        @dag.node
        def target(x: int) -> int:
            return x + 1

        source_node = dag.nodes["source"]
        target_node = dag.nodes["target"]

        # Test connecting to single target
        result = source_node >> target_node
        assert result == target_node

    def test_or_operator(self):
        """Test __or__ operator (|)"""
        dag = DAG("or_test")

        @dag.node
        def source() -> int:
            return 1

        @dag.node
        def target(x: int) -> int:
            return x + 1

        source_node = dag.nodes["source"]
        target_node = dag.nodes["target"]

        # Test using | operator
        result = source_node | target_node
        assert result == target_node

    def test_rrshift_with_any_node(self):
        """Test __rrshift__ with ANY node accepting multiple sources"""
        dag = DAG("rrshift_any_test")

        @dag.node
        def source1() -> int:
            return 1

        @dag.node
        def source2() -> int:
            return 2

        @dag.any
        def target(inputs: list[int]) -> int:
            return sum(inputs)

        source_nodes = [dag.nodes["source1"], dag.nodes["source2"]]
        target_node = dag.nodes["target"]

        # Test connecting multiple sources to ANY node
        result = source_nodes >> target_node
        assert result == target_node

    def test_rrshift_with_all_node(self):
        """Test __rrshift__ with ALL node accepting multiple sources"""
        dag = DAG("rrshift_all_test")

        @dag.node
        def source1() -> int:
            return 1

        @dag.node
        def source2() -> int:
            return 2

        @dag.all
        def target(inputs: list[int]) -> int:
            return sum(inputs)

        source_nodes = [dag.nodes["source1"], dag.nodes["source2"]]
        target_node = dag.nodes["target"]

        # Test connecting multiple sources to ALL node
        result = source_nodes >> target_node
        assert result == target_node

    def test_rrshift_error_too_many_sources(self):
        """Test __rrshift__ error when too many sources for regular node"""
        dag = DAG("rrshift_error_test")

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
        def target(x: int) -> int:  # Only one input
            return x + 1

        source_nodes = [
            dag.nodes["source1"],
            dag.nodes["source2"],
            dag.nodes["source3"],
        ]
        target_node = dag.nodes["target"]

        # Should raise error - too many sources for single input
        import pytest

        with pytest.raises(
            ValueError,
            match="Cannot connect 3 source nodes to node 'target' with only 1 inputs",
        ):
            source_nodes >> target_node

    def test_rrshift_multiple_inputs_mapping(self):
        """Test __rrshift__ with multiple inputs that map correctly"""
        dag = DAG("rrshift_mapping_test")

        @dag.node
        def source1() -> int:
            return 1

        @dag.node
        def source2() -> int:
            return 2

        @dag.node
        def target(x: int, y: int) -> int:  # Two inputs
            return x + y

        source_nodes = [dag.nodes["source1"], dag.nodes["source2"]]
        target_node = dag.nodes["target"]

        # Should work - 2 sources to 2 inputs
        result = source_nodes >> target_node
        assert result == target_node

    def test_rrshift_more_sources_than_inputs(self):
        """Test __rrshift__ when more sources than inputs raises error"""
        dag = DAG("rrshift_overflow_test")

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
        def target(x: int, y: int) -> int:  # Two inputs
            return x + y

        source_nodes = [
            dag.nodes["source1"],
            dag.nodes["source2"],
            dag.nodes["source3"],
        ]
        target_node = dag.nodes["target"]

        # Should raise error - 3 sources to 2 inputs is not allowed
        import pytest

        with pytest.raises(
            ValueError,
            match="Cannot connect 3 source nodes to node 'target' with only 2 inputs",
        ):
            source_nodes >> target_node
