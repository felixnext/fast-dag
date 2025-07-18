"""Nested workflow functionality for DAGs."""

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from ..core.nested import DAGNode, FSMNode
from ..core.node import Node
from ..core.types import NodeType

if TYPE_CHECKING:
    from ..fsm import FSM
    from .core import DAG


class NestedWorkflows:
    """Mixin class for nested workflow functionality."""

    # Type hints for attributes from DAG
    name: str
    nodes: dict[str, Node]

    def add_node(
        self,
        node: Node | str,
        func: Callable | None = None,
        *,
        inputs: list[str] | None = None,
        outputs: list[str] | None = None,
        description: str | None = None,
        node_type: NodeType = NodeType.STANDARD,
        retry: int | None = None,
        retry_delay: float | None = None,
        timeout: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "DAG":
        """Add node - defined in DAG."""
        raise NotImplementedError

    def add_dag(
        self,
        name: str,
        dag: "DAG",
        *,
        inputs: list[str] | dict[str, str] | None = None,
        outputs: list[str] | dict[str, str] | None = None,
        description: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Node:
        """Add a DAG as a node in this DAG.

        Args:
            name: Name for the nested DAG node
            dag: DAG instance to add
            inputs: Input parameter names
            outputs: Output parameter names
            description: Node description
            metadata: Additional metadata

        Returns:
            The created DAG node
        """
        # Auto-detect inputs from entry points if not provided
        if inputs is None:
            # Get the inputs from the DAG's entry points
            entry_points = dag.get_entry_points()
            all_inputs = set()
            for entry_name in entry_points:
                entry_node = dag.nodes[entry_name]
                if entry_node.inputs:
                    all_inputs.update(entry_node.inputs)
            inputs = list(all_inputs) if all_inputs else []
        elif isinstance(inputs, dict):
            # If inputs is a dict, extract the keys as the input names
            inputs = list(inputs.keys())

        # Handle outputs format
        if outputs is not None and isinstance(outputs, dict):
            outputs = list(outputs.keys())

        # Create DAG node wrapper
        node_metadata = metadata or {}
        node_metadata["node_name"] = name  # Store node name for nested context access
        dag_node = DAGNode(
            func=dag.run
            if hasattr(dag, "run")
            else lambda **_: None,  # Use the DAG's run method
            name=name,
            inputs=inputs,
            outputs=outputs or ["result"],
            description=description or f"Nested DAG: {dag.name}",
            node_type=NodeType.DAG,
            metadata=node_metadata,
            dag=dag,  # type: ignore[arg-type]
        )

        # Add to this DAG
        self.add_node(dag_node)

        return dag_node

    def add_fsm(
        self,
        name: str,
        fsm: "FSM",
        *,
        inputs: list[str] | dict[str, str] | None = None,
        outputs: list[str] | dict[str, str] | None = None,
        description: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Node:
        """Add an FSM as a node in this DAG.

        Args:
            name: Name for the nested FSM node
            fsm: FSM instance to add
            inputs: Input parameter names
            outputs: Output parameter names
            description: Node description
            metadata: Additional metadata

        Returns:
            The created FSM node
        """
        # Handle inputs format
        if inputs is not None and isinstance(inputs, dict):
            inputs = list(inputs.keys())

        # Handle outputs format
        if outputs is not None and isinstance(outputs, dict):
            outputs = list(outputs.keys())

        # Create FSM node wrapper
        fsm_node = FSMNode(
            func=fsm.run
            if hasattr(fsm, "run")
            else lambda **_: None,  # Use the FSM's run method
            name=name,
            inputs=inputs,
            outputs=outputs or ["result"],
            description=description or f"Nested FSM: {fsm.name}",
            node_type=NodeType.FSM,
            metadata=metadata or {},
            fsm=fsm,
        )

        # Add to this DAG
        self.add_node(fsm_node)

        return fsm_node

    def add_workflow(
        self,
        name: str,
        workflow: "DAG | FSM",
        *,
        inputs: list[str] | dict[str, str] | None = None,
        outputs: list[str] | dict[str, str] | None = None,
        description: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Node:
        """Add a workflow (DAG or FSM) as a node in this DAG.

        Args:
            name: Name for the nested workflow node
            workflow: Workflow instance to add
            inputs: Input parameter names
            outputs: Output parameter names
            description: Node description
            metadata: Additional metadata

        Returns:
            The created workflow node
        """
        # Import here to avoid circular imports
        from ..fsm import FSM

        if isinstance(workflow, FSM):
            return self.add_fsm(
                name=name,
                fsm=workflow,
                inputs=inputs,
                outputs=outputs,
                description=description,
                metadata=metadata,
            )
        else:
            return self.add_dag(
                name=name,
                dag=workflow,
                inputs=inputs,
                outputs=outputs,
                description=description,
                metadata=metadata,
            )

    def get_nested_workflows(self) -> dict[str, "DAG | FSM"]:
        """Get all nested workflows in this DAG.

        Returns:
            Dictionary mapping node names to nested workflows
        """
        nested = {}

        for node_name, node in self.nodes.items():
            if node.node_type == NodeType.DAG and hasattr(node, "dag"):
                nested[node_name] = node.dag
            elif node.node_type == NodeType.FSM and hasattr(node, "fsm"):
                nested[node_name] = node.fsm

        return nested

    def get_nested_depth(self) -> int:
        """Get the maximum nesting depth of workflows.

        Returns:
            Maximum nesting depth
        """
        max_depth = 0

        for node in self.nodes.values():
            if node.node_type == NodeType.DAG and hasattr(node, "dag"):
                depth = 1 + node.dag.get_nested_depth()
                max_depth = max(max_depth, depth)
            elif node.node_type == NodeType.FSM and hasattr(node, "fsm"):
                depth = 1 + node.fsm.get_nested_depth()
                max_depth = max(max_depth, depth)

        return max_depth

    def flatten_nested_workflows(self) -> dict[str, Any]:
        """Flatten all nested workflows into a single structure.

        Returns:
            Dictionary with flattened workflow information
        """
        flattened: dict[str, Any] = {
            "name": self.name,
            "type": "DAG",
            "nodes": {},
            "nested_workflows": {},
        }

        for node_name, node in self.nodes.items():
            if node.node_type == NodeType.DAG and hasattr(node, "dag"):
                flattened["nested_workflows"][node_name] = (
                    node.dag.flatten_nested_workflows()
                )
            elif node.node_type == NodeType.FSM and hasattr(node, "fsm"):
                flattened["nested_workflows"][node_name] = (
                    node.fsm.flatten_nested_workflows()
                )
            else:
                flattened["nodes"][node_name] = {
                    "name": node.name,
                    "type": node.node_type.value,
                    "function": node.func.__name__ if node.func is not None else None,
                    "inputs": node.inputs,
                    "outputs": node.outputs,
                }

        return flattened
