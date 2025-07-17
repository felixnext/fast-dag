"""Support for nested workflows (DAG/FSM as nodes)."""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from .node import Node
from .types import NodeType

if TYPE_CHECKING:
    from ..dag import DAG
    from ..fsm import FSM


@dataclass
class DAGNode(Node):
    """Node that wraps a DAG for nested execution."""

    dag: "DAG" = field(default=None, kw_only=True)  # type: ignore

    def __post_init__(self) -> None:
        """Initialize the DAG node."""
        # Ensure metadata is initialized
        if not hasattr(self, "metadata"):
            self.metadata = {}

        # Set node type
        self.node_type = NodeType.DAG

        # Create a wrapper function that executes the DAG
        # Store references we need
        dag = self.dag
        metadata = self.metadata
        outputs = self.outputs

        # Create a function that captures these references
        def make_execute_function(dag, metadata, outputs):
            # Build the function dynamically with the right signature
            param_names = self.inputs or []
            params_with_context = param_names + ["context=None"]
            func_str = f"def execute_dag({', '.join(params_with_context)}):\n"
            if param_names:
                # Build the kwargs dict string without nested f-strings
                kwargs_items = [f'"{p}": {p}' for p in param_names]
                func_str += f"    kwargs = {{{', '.join(kwargs_items)}}}\n"
            else:
                func_str += "    kwargs = {}\n"
            func_str += """
    # Map inputs if mapping is provided
    dag_kwargs = {}
    if "input_mapping" in metadata:
        mapping = metadata["input_mapping"]
        for dag_input, node_input in mapping.items():
            if node_input in kwargs:
                dag_kwargs[dag_input] = kwargs[node_input]
    else:
        dag_kwargs = kwargs

    # Run the DAG with mapped kwargs
    result = dag.run(context=context, **dag_kwargs)

    # Handle output mapping
    if "output_mapping" in metadata and outputs:
        # For now, assume single output
        if len(outputs) == 1:
            return result
    return result
"""

            # Create the function in a scope that has access to our captured variables
            local_scope = {"dag": dag, "metadata": metadata, "outputs": outputs}
            exec(func_str, local_scope)
            return local_scope["execute_dag"]

        # Set the function
        self.func = make_execute_function(dag, metadata, outputs)

        # Initialize parent class
        super().__post_init__()


@dataclass
class FSMNode(Node):
    """Node that wraps an FSM for nested execution."""

    fsm: "FSM" = field(default=None, kw_only=True)  # type: ignore

    def __post_init__(self) -> None:
        """Initialize the FSM node."""
        # Ensure metadata is initialized
        if not hasattr(self, "metadata"):
            self.metadata = {}

        # Set node type
        self.node_type = NodeType.FSM

        # Create a wrapper function that executes the FSM
        # Store references we need
        fsm = self.fsm
        metadata = self.metadata
        outputs = self.outputs

        # Create a function that captures these references
        def make_execute_function(fsm, metadata, outputs):
            # Build the function dynamically with the right signature
            param_names = self.inputs or []
            params_with_context = param_names + ["context=None"]
            func_str = f"def execute_fsm({', '.join(params_with_context)}):\n"
            if param_names:
                # Build the kwargs dict string without nested f-strings
                kwargs_items = [f'"{p}": {p}' for p in param_names]
                func_str += f"    kwargs = {{{', '.join(kwargs_items)}}}\n"
            else:
                func_str += "    kwargs = {}\n"
            func_str += """
    # Map inputs if mapping is provided
    fsm_kwargs = {}
    if "input_mapping" in metadata:
        mapping = metadata["input_mapping"]
        for fsm_input, node_input in mapping.items():
            if node_input in kwargs:
                fsm_kwargs[fsm_input] = kwargs[node_input]
    else:
        fsm_kwargs = kwargs

    # Run the FSM with mapped kwargs
    result = fsm.run(context=context, **fsm_kwargs)

    # Handle output mapping
    if "output_mapping" in metadata and outputs:
        # For now, assume single output
        if len(outputs) == 1:
            return result
    return result
"""

            # Create the function in a scope that has access to our captured variables
            local_scope = {"fsm": fsm, "metadata": metadata, "outputs": outputs}
            exec(func_str, local_scope)
            return local_scope["execute_fsm"]

        # Set the function
        self.func = make_execute_function(fsm, metadata, outputs)

        # Initialize parent class
        super().__post_init__()


def create_dag_node(
    dag: "DAG",
    name: str,
    inputs: list[str] | None = None,
    outputs: list[str] | None = None,
    description: str | None = None,
) -> DAGNode:
    """Create a node that wraps a DAG.

    Args:
        dag: The DAG to wrap
        name: Name for the node
        inputs: Input names that map to DAG kwargs
        outputs: Output names (typically ["result"])
        description: Node description

    Returns:
        DAGNode instance
    """
    # Create a dummy function with the right signature
    # This will be replaced in __post_init__
    param_names = inputs or []
    func_str = f"def wrapper_func({', '.join(param_names)}): pass"
    exec(func_str, globals())
    wrapper_func = globals()["wrapper_func"]

    return DAGNode(
        dag=dag,
        name=name,
        inputs=inputs,
        outputs=outputs,
        description=description or f"Nested DAG: {dag.name}",
        func=wrapper_func,
    )


def create_fsm_node(
    fsm: "FSM",
    name: str,
    inputs: list[str] | None = None,
    outputs: list[str] | None = None,
    description: str | None = None,
) -> FSMNode:
    """Create a node that wraps an FSM.

    Args:
        fsm: The FSM to wrap
        name: Name for the node
        inputs: Input names that map to FSM kwargs
        outputs: Output names (typically ["result"])
        description: Node description

    Returns:
        FSMNode instance
    """
    # Create a dummy function with the right signature
    # This will be replaced in __post_init__
    param_names = inputs or []
    func_str = f"def wrapper_func({', '.join(param_names)}): pass"
    exec(func_str, globals())
    wrapper_func = globals()["wrapper_func"]

    return FSMNode(
        fsm=fsm,
        name=name,
        inputs=inputs,
        outputs=outputs,
        description=description or f"Nested FSM: {fsm.name}",
        func=wrapper_func,
    )
