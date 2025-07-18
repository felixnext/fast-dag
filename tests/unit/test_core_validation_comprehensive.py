"""
Comprehensive tests for core validation functionality to improve coverage.
"""

import pytest

from fast_dag.core.node import Node
from fast_dag.core.validation import (
    find_cycles,
    find_disconnected_nodes,
    find_entry_points,
    topological_sort,
)


class TestFindCycles:
    """Test cycle detection functionality"""

    def test_find_cycles_no_cycles(self):
        """Test finding cycles in a DAG with no cycles"""
        # Create nodes
        nodes = {
            "a": Node(func=lambda: 1, name="a"),
            "b": Node(func=lambda x: x + 1, name="b"),
            "c": Node(func=lambda x: x * 2, name="c"),
        }

        # Create connections (a -> b -> c)
        nodes["a"].output_connections["result"] = [(nodes["b"], "x")]
        nodes["b"].input_connections["x"] = [(nodes["a"], "result")]
        nodes["b"].output_connections["result"] = [(nodes["c"], "x")]
        nodes["c"].input_connections["x"] = [(nodes["b"], "result")]

        cycles = find_cycles(nodes)
        assert len(cycles) == 0

    def test_find_cycles_simple_cycle(self):
        """Test finding a simple cycle"""
        # Create nodes
        nodes = {
            "a": Node(func=lambda: 1, name="a"),
            "b": Node(func=lambda x: x + 1, name="b"),
            "c": Node(func=lambda x: x * 2, name="c"),
        }

        # Create cycle (a -> b -> c -> a)
        nodes["a"].output_connections["result"] = [(nodes["b"], "x")]
        nodes["b"].input_connections["x"] = [(nodes["a"], "result")]
        nodes["b"].output_connections["result"] = [(nodes["c"], "x")]
        nodes["c"].input_connections["x"] = [(nodes["b"], "result")]
        nodes["c"].output_connections["result"] = [(nodes["a"], "x")]
        nodes["a"].input_connections["x"] = [(nodes["c"], "result")]

        cycles = find_cycles(nodes)
        assert len(cycles) > 0
        # Should find the cycle
        cycle_nodes = set()
        for cycle in cycles:
            cycle_nodes.update(cycle[:-1])  # Exclude duplicate last node
        assert {"a", "b", "c"}.issubset(cycle_nodes)

    def test_find_cycles_multiple_cycles(self):
        """Test finding multiple cycles"""
        # Create nodes
        nodes = {
            "a": Node(func=lambda: 1, name="a"),
            "b": Node(func=lambda x: x + 1, name="b"),
            "c": Node(func=lambda x: x * 2, name="c"),
            "d": Node(func=lambda x: x - 1, name="d"),
        }

        # Create two cycles: a -> b -> a and c -> d -> c
        nodes["a"].output_connections["result"] = [(nodes["b"], "x")]
        nodes["b"].input_connections["x"] = [(nodes["a"], "result")]
        nodes["b"].output_connections["result"] = [(nodes["a"], "x")]
        nodes["a"].input_connections["x"] = [(nodes["b"], "result")]

        nodes["c"].output_connections["result"] = [(nodes["d"], "x")]
        nodes["d"].input_connections["x"] = [(nodes["c"], "result")]
        nodes["d"].output_connections["result"] = [(nodes["c"], "x")]
        nodes["c"].input_connections["x"] = [(nodes["d"], "result")]

        cycles = find_cycles(nodes)
        assert len(cycles) >= 2

    def test_find_cycles_self_loop(self):
        """Test finding self-loop cycle"""
        # Create node with self-loop
        nodes = {
            "a": Node(func=lambda x: x + 1, name="a"),
        }

        # Create self-loop
        nodes["a"].output_connections["result"] = [(nodes["a"], "x")]
        nodes["a"].input_connections["x"] = [(nodes["a"], "result")]

        cycles = find_cycles(nodes)
        assert len(cycles) > 0
        assert "a" in cycles[0]

    def test_find_cycles_with_none_name(self):
        """Test cycle detection with node that has None name"""
        # Create nodes, one with None name
        node_none = Node(func=lambda: 1, name=None)
        nodes = {
            "a": Node(func=lambda: 1, name="a"),
            "none": node_none,
        }

        # Create connection to node with None name
        nodes["a"].output_connections["result"] = [(node_none, "x")]

        # Should not crash
        cycles = find_cycles(nodes)
        assert isinstance(cycles, list)


class TestFindDisconnectedNodes:
    """Test disconnected node detection"""

    def test_find_disconnected_empty(self):
        """Test with empty graph"""
        nodes = {}
        disconnected = find_disconnected_nodes(nodes)
        assert len(disconnected) == 0

    def test_find_disconnected_single_node(self):
        """Test with single node (should not be considered disconnected)"""
        nodes = {
            "a": Node(func=lambda: 1, name="a"),
        }
        disconnected = find_disconnected_nodes(nodes)
        assert len(disconnected) == 0

    def test_find_disconnected_all_connected(self):
        """Test with all nodes connected"""
        nodes = {
            "a": Node(func=lambda: 1, name="a"),
            "b": Node(func=lambda x: x + 1, name="b"),
            "c": Node(func=lambda x: x * 2, name="c"),
        }

        # Connect all nodes
        nodes["a"].output_connections["result"] = [(nodes["b"], "x")]
        nodes["b"].input_connections["x"] = [(nodes["a"], "result")]
        nodes["b"].output_connections["result"] = [(nodes["c"], "x")]
        nodes["c"].input_connections["x"] = [(nodes["b"], "result")]

        disconnected = find_disconnected_nodes(nodes)
        assert len(disconnected) == 0

    def test_find_disconnected_isolated_node(self):
        """Test with isolated node"""
        nodes = {
            "a": Node(func=lambda: 1, name="a"),
            "b": Node(func=lambda x: x + 1, name="b"),
            "c": Node(func=lambda: 2, name="c"),  # Isolated
        }

        # Connect a and b, leave c isolated
        nodes["a"].output_connections["result"] = [(nodes["b"], "x")]
        nodes["b"].input_connections["x"] = [(nodes["a"], "result")]

        disconnected = find_disconnected_nodes(nodes)
        assert disconnected == {"c"}

    def test_find_disconnected_multiple_isolated(self):
        """Test with multiple isolated nodes"""
        nodes = {
            "a": Node(func=lambda: 1, name="a"),
            "b": Node(func=lambda x: x + 1, name="b"),
            "c": Node(func=lambda: 2, name="c"),  # Isolated
            "d": Node(func=lambda: 3, name="d"),  # Isolated
        }

        # Connect only a and b
        nodes["a"].output_connections["result"] = [(nodes["b"], "x")]
        nodes["b"].input_connections["x"] = [(nodes["a"], "result")]

        disconnected = find_disconnected_nodes(nodes)
        assert disconnected == {"c", "d"}


class TestFindEntryPoints:
    """Test entry point detection"""

    def test_find_entry_points_single(self):
        """Test finding single entry point"""
        nodes = {
            "start": Node(func=lambda: 1, name="start"),
            "middle": Node(func=lambda x: x + 1, name="middle"),
            "end": Node(func=lambda x: x * 2, name="end"),
        }

        # Create chain: start -> middle -> end
        nodes["start"].output_connections["result"] = [(nodes["middle"], "x")]
        nodes["middle"].input_connections["x"] = [(nodes["start"], "result")]
        nodes["middle"].output_connections["result"] = [(nodes["end"], "x")]
        nodes["end"].input_connections["x"] = [(nodes["middle"], "result")]

        entry_points = find_entry_points(nodes)
        assert entry_points == ["start"]

    def test_find_entry_points_multiple(self):
        """Test finding multiple entry points"""
        nodes = {
            "start1": Node(func=lambda: 1, name="start1"),
            "start2": Node(func=lambda: 2, name="start2"),
            "merge": Node(func=lambda x, y: x + y, name="merge"),
        }

        # Two entry points merging
        nodes["start1"].output_connections["result"] = [(nodes["merge"], "x")]
        nodes["start2"].output_connections["result"] = [(nodes["merge"], "y")]
        nodes["merge"].input_connections["x"] = [(nodes["start1"], "result")]
        nodes["merge"].input_connections["y"] = [(nodes["start2"], "result")]

        entry_points = find_entry_points(nodes)
        assert set(entry_points) == {"start1", "start2"}

    def test_find_entry_points_none(self):
        """Test with no entry points (cycle)"""
        nodes = {
            "a": Node(func=lambda x: x + 1, name="a"),
            "b": Node(func=lambda x: x * 2, name="b"),
        }

        # Create cycle with no entry points
        nodes["a"].output_connections["result"] = [(nodes["b"], "x")]
        nodes["b"].input_connections["x"] = [(nodes["a"], "result")]
        nodes["b"].output_connections["result"] = [(nodes["a"], "x")]
        nodes["a"].input_connections["x"] = [(nodes["b"], "result")]

        entry_points = find_entry_points(nodes)
        assert entry_points == []

    def test_find_entry_points_all_nodes(self):
        """Test where all nodes are entry points"""
        nodes = {
            "a": Node(func=lambda: 1, name="a"),
            "b": Node(func=lambda: 2, name="b"),
            "c": Node(func=lambda: 3, name="c"),
        }

        # No connections, all are entry points
        entry_points = find_entry_points(nodes)
        assert set(entry_points) == {"a", "b", "c"}


class TestTopologicalSort:
    """Test topological sorting"""

    def test_topological_sort_linear(self):
        """Test topological sort on linear DAG"""
        nodes = {
            "a": Node(func=lambda: 1, name="a"),
            "b": Node(func=lambda x: x + 1, name="b"),
            "c": Node(func=lambda x: x * 2, name="c"),
        }

        # Create linear chain: a -> b -> c
        nodes["a"].output_connections["result"] = [(nodes["b"], "x")]
        nodes["b"].input_connections["x"] = [(nodes["a"], "result")]
        nodes["b"].output_connections["result"] = [(nodes["c"], "x")]
        nodes["c"].input_connections["x"] = [(nodes["b"], "result")]

        sorted_nodes = topological_sort(nodes)
        assert sorted_nodes == ["a", "b", "c"]

    def test_topological_sort_diamond(self):
        """Test topological sort on diamond-shaped DAG"""
        nodes = {
            "start": Node(func=lambda: 1, name="start"),
            "left": Node(func=lambda x: x + 1, name="left"),
            "right": Node(func=lambda x: x * 2, name="right"),
            "end": Node(func=lambda x, y: x + y, name="end"),
        }

        # Create diamond: start -> left/right -> end
        nodes["start"].output_connections["result"] = [
            (nodes["left"], "x"),
            (nodes["right"], "x"),
        ]
        nodes["left"].input_connections["x"] = [(nodes["start"], "result")]
        nodes["right"].input_connections["x"] = [(nodes["start"], "result")]
        nodes["left"].output_connections["result"] = [(nodes["end"], "x")]
        nodes["right"].output_connections["result"] = [(nodes["end"], "y")]
        nodes["end"].input_connections["x"] = [(nodes["left"], "result")]
        nodes["end"].input_connections["y"] = [(nodes["right"], "result")]

        sorted_nodes = topological_sort(nodes)

        # Check ordering constraints
        assert sorted_nodes.index("start") < sorted_nodes.index("left")
        assert sorted_nodes.index("start") < sorted_nodes.index("right")
        assert sorted_nodes.index("left") < sorted_nodes.index("end")
        assert sorted_nodes.index("right") < sorted_nodes.index("end")

    def test_topological_sort_with_cycle(self):
        """Test topological sort with cycle (should raise error)"""
        nodes = {
            "a": Node(func=lambda x: x + 1, name="a"),
            "b": Node(func=lambda x: x * 2, name="b"),
            "c": Node(func=lambda x: x - 1, name="c"),
        }

        # Create cycle: a -> b -> c -> a
        nodes["a"].output_connections["result"] = [(nodes["b"], "x")]
        nodes["b"].input_connections["x"] = [(nodes["a"], "result")]
        nodes["b"].output_connections["result"] = [(nodes["c"], "x")]
        nodes["c"].input_connections["x"] = [(nodes["b"], "result")]
        nodes["c"].output_connections["result"] = [(nodes["a"], "x")]
        nodes["a"].input_connections["x"] = [(nodes["c"], "result")]

        with pytest.raises(ValueError, match="Graph has cycles"):
            topological_sort(nodes)

    def test_topological_sort_disconnected(self):
        """Test topological sort with disconnected components"""
        nodes = {
            "a": Node(func=lambda: 1, name="a"),
            "b": Node(func=lambda x: x + 1, name="b"),
            "c": Node(func=lambda: 2, name="c"),
            "d": Node(func=lambda x: x * 2, name="d"),
        }

        # Two disconnected chains: a -> b and c -> d
        nodes["a"].output_connections["result"] = [(nodes["b"], "x")]
        nodes["b"].input_connections["x"] = [(nodes["a"], "result")]
        nodes["c"].output_connections["result"] = [(nodes["d"], "x")]
        nodes["d"].input_connections["x"] = [(nodes["c"], "result")]

        sorted_nodes = topological_sort(nodes)

        # Check ordering within each component
        assert sorted_nodes.index("a") < sorted_nodes.index("b")
        assert sorted_nodes.index("c") < sorted_nodes.index("d")

    def test_topological_sort_empty(self):
        """Test topological sort with empty graph"""
        nodes = {}
        sorted_nodes = topological_sort(nodes)
        assert sorted_nodes == []

    def test_topological_sort_single_node(self):
        """Test topological sort with single node"""
        nodes = {
            "a": Node(func=lambda: 1, name="a"),
        }
        sorted_nodes = topological_sort(nodes)
        assert sorted_nodes == ["a"]
