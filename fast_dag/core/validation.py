"""Validation utilities for DAG structures."""

from .node import Node


def find_cycles(nodes: dict[str, Node]) -> list[list[str]]:
    """Find all cycles in the graph using DFS.

    Returns a list of cycles, where each cycle is a list of node names.
    """
    cycles = []
    visited = set()
    rec_stack = set()
    path = []

    def dfs(node_name: str) -> None:
        """Depth-first search to find cycles."""
        visited.add(node_name)
        rec_stack.add(node_name)
        path.append(node_name)

        node = nodes.get(node_name)
        if node:
            # Check all output connections
            for _output_name, connections in node.output_connections.items():
                for target_node, _ in connections:
                    target_name = target_node.name
                    if target_name is not None and target_name not in visited:
                        dfs(target_name)
                    elif target_name in rec_stack:
                        # Found a cycle
                        cycle_start = path.index(target_name)
                        cycle = path[cycle_start:] + [target_name]
                        cycles.append(cycle)

        path.pop()
        rec_stack.remove(node_name)

    # Check all nodes
    for node_name in nodes:
        if node_name not in visited:
            dfs(node_name)

    return cycles


def find_disconnected_nodes(nodes: dict[str, Node]) -> set[str]:
    """Find nodes that are not connected to the main graph.

    A node is considered disconnected if it has no connections at all
    (neither inputs nor outputs).
    """
    disconnected = set()

    for node_name, node in nodes.items():
        has_inputs = bool(node.input_connections)
        has_outputs = any(
            connections for connections in node.output_connections.values()
        )

        if not has_inputs and not has_outputs:
            disconnected.add(node_name)

    return disconnected


def find_entry_points(nodes: dict[str, Node]) -> list[str]:
    """Find entry point nodes (nodes with no input connections)."""
    entry_points = []

    for node_name, node in nodes.items():
        if not node.input_connections:
            entry_points.append(node_name)

    return entry_points


def topological_sort(nodes: dict[str, Node]) -> list[str]:
    """Perform topological sort on the DAG.

    Returns a list of node names in execution order.
    Raises ValueError if the graph has cycles.
    """
    # Count incoming edges for each node
    in_degree = dict.fromkeys(nodes, 0)

    for node in nodes.values():
        for connections in node.output_connections.values():
            for target_node, _ in connections:
                target_name = target_node.name
                if target_name in in_degree:
                    in_degree[target_name] += 1

    # Find all nodes with no incoming edges
    queue = [name for name, degree in in_degree.items() if degree == 0]
    result = []

    while queue:
        # Remove a node from queue
        current = queue.pop(0)
        result.append(current)

        # Reduce in-degree for all neighbors
        node = nodes[current]
        for connections in node.output_connections.values():
            for target_node, _ in connections:
                target_name = target_node.name
                if target_name in in_degree:
                    in_degree[target_name] -= 1
                    if in_degree[target_name] == 0:
                        queue.append(target_name)

    # Check if all nodes were processed
    if len(result) != len(nodes):
        raise ValueError("Graph has cycles, cannot perform topological sort")

    return result
