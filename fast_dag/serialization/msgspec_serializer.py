"""msgspec-based serializer implementation."""

from typing import TYPE_CHECKING, Any

from ..core.context import Context, FSMContext
from ..core.node import Node
from ..core.types import NodeType

if TYPE_CHECKING:
    from ..dag import DAG
    from ..fsm import FSM
from .base import Serializer
from .registry import FunctionRegistry, get_function_path
from .types import (
    SerializableConnection,
    SerializableContext,
    SerializableDAG,
    SerializableFSM,
    SerializableFSMContext,
    SerializableNode,
)

try:
    import msgspec
    import yaml  # type: ignore[import-untyped]

    HAS_MSGSPEC = True
except ImportError:
    HAS_MSGSPEC = False


class MsgspecSerializer(Serializer):
    """Serializer using msgspec for high performance."""

    def __init__(self):
        if not HAS_MSGSPEC:
            raise ImportError(
                "msgspec is required for serialization. "
                "Install with: pip install fast-dag[serialize]"
            )

    def serialize(self, obj: Any, format: str = "json") -> bytes | str:
        """Serialize an object to the specified format."""
        # Convert to serializable type
        serializable: SerializableDAG | SerializableFSM | SerializableContext
        if obj.__class__.__name__ == "FSM":
            # Check FSM before DAG since FSM is a subclass of DAG
            serializable = self._fsm_to_serializable(obj)
        elif obj.__class__.__name__ == "DAG":
            serializable = self._dag_to_serializable(obj)
        elif isinstance(obj, Context):
            serializable = self._context_to_serializable(obj)
        else:
            raise TypeError(f"Cannot serialize type: {type(obj)}")

        # Encode based on format
        if format == "json":
            json_encoder = msgspec.json.Encoder()
            return json_encoder.encode(serializable).decode("utf-8")
        elif format == "msgpack":
            msgpack_encoder = msgspec.msgpack.Encoder()
            return msgpack_encoder.encode(serializable)
        elif format == "yaml":
            # Convert to dict first for yaml
            data = msgspec.to_builtins(serializable)
            return yaml.dump(data, default_flow_style=False)
        else:
            raise ValueError(f"Unknown format: {format}")

    def deserialize(
        self, data: bytes | str, target_type: type, format: str = "json"
    ) -> Any:
        """Deserialize data to the target type."""
        # Decode based on format
        if format == "json":
            if isinstance(data, str):
                data = data.encode("utf-8")
            json_decoder = msgspec.json.Decoder()
            raw = json_decoder.decode(data)
        elif format == "msgpack":
            if isinstance(data, str):
                data = data.encode("utf-8")
            msgpack_decoder = msgspec.msgpack.Decoder()
            raw = msgpack_decoder.decode(data)
        elif format == "yaml":
            if isinstance(data, bytes):
                data = data.decode("utf-8")
            raw = yaml.safe_load(data)
        else:
            raise ValueError(f"Unknown format: {format}")

        # Convert to target type
        if target_type.__name__ == "DAG":
            serializable_dag = msgspec.convert(raw, SerializableDAG)
            return self._serializable_to_dag(serializable_dag)
        elif target_type.__name__ == "FSM":
            serializable_fsm = msgspec.convert(raw, SerializableFSM)
            return self._serializable_to_fsm(serializable_fsm)
        elif target_type == Context:
            serializable_context = msgspec.convert(raw, SerializableContext)
            return self._serializable_to_context(serializable_context)
        elif target_type == FSMContext:
            serializable_fsm_context = msgspec.convert(raw, SerializableFSMContext)
            return self._serializable_to_fsm_context(serializable_fsm_context)
        else:
            raise TypeError(f"Cannot deserialize to type: {target_type}")

    def _dag_to_serializable(self, dag: "DAG") -> SerializableDAG:
        """Convert a DAG to its serializable representation."""
        nodes = []
        connections = []

        # Convert nodes
        for node in dag.nodes.values():
            serializable_node = SerializableNode(
                name=node.name or "",
                function=get_function_path(node.func),
                inputs=node.inputs,
                outputs=node.outputs,
                description=node.description,
                node_type=node.node_type.value,
                retry=node.retry,
                retry_delay=node.retry_delay,
                timeout=node.timeout,
                metadata=node.metadata,
            )
            nodes.append(serializable_node)

            # Convert connections
            for output_name, conns in node.output_connections.items():
                for target_node, input_name in conns:
                    if target_node.name:
                        connection = SerializableConnection(
                            source=node.name or "",
                            target=target_node.name,
                            output=output_name,
                            input=input_name,
                        )
                        connections.append(connection)

        return SerializableDAG(
            name=dag.name,
            nodes=nodes,
            connections=connections,
            description=dag.description,
            metadata=dag.metadata,
        )

    def _serializable_to_dag(self, serializable: SerializableDAG) -> "DAG":
        """Convert a serializable representation to a DAG."""
        # Import here to avoid circular imports
        from ..dag import DAG

        dag = DAG(
            name=serializable.name,
            description=serializable.description,
            metadata=serializable.metadata,
        )

        # Add nodes
        for node_data in serializable.nodes:
            func = FunctionRegistry.get(node_data.function)
            node = Node(
                func=func,
                name=node_data.name,
                inputs=node_data.inputs,
                outputs=node_data.outputs,
                description=node_data.description,
                node_type=NodeType(node_data.node_type),
                retry=node_data.retry,
                retry_delay=node_data.retry_delay,
                timeout=node_data.timeout,
                metadata=node_data.metadata,
            )
            dag.add_node(node)

        # Add connections
        for conn in serializable.connections:
            dag.connect(
                conn.source,
                conn.target,
                output=conn.output or "result",
                input=conn.input,
            )

        return dag

    def _fsm_to_serializable(self, fsm: "FSM") -> SerializableFSM:
        """Convert an FSM to its serializable representation."""
        # Get base DAG serializable
        dag_serializable = self._dag_to_serializable(fsm)

        return SerializableFSM(
            name=dag_serializable.name,
            nodes=dag_serializable.nodes,
            connections=dag_serializable.connections,
            initial_state=fsm.initial_state,
            terminal_states=list(fsm.terminal_states),
            max_cycles=fsm.max_cycles,
            state_transitions=fsm.state_transitions,
            description=dag_serializable.description,
            metadata=dag_serializable.metadata,
        )

    def _serializable_to_fsm(self, serializable: SerializableFSM) -> "FSM":
        """Convert a serializable representation to an FSM."""
        # Import here to avoid circular imports
        from ..fsm import FSM

        fsm = FSM(
            name=serializable.name,
            description=serializable.description,
            metadata=serializable.metadata,
            initial_state=serializable.initial_state,
            terminal_states=set(serializable.terminal_states),
            max_cycles=serializable.max_cycles,
            state_transitions=serializable.state_transitions,
        )

        # Add nodes
        for node_data in serializable.nodes:
            func = FunctionRegistry.get(node_data.function)
            node = Node(
                func=func,
                name=node_data.name,
                inputs=node_data.inputs,
                outputs=node_data.outputs,
                description=node_data.description,
                node_type=NodeType(node_data.node_type),
                retry=node_data.retry,
                retry_delay=node_data.retry_delay,
                timeout=node_data.timeout,
                metadata=node_data.metadata,
            )
            fsm.add_node(node)

        # Add connections
        for conn in serializable.connections:
            fsm.connect(
                conn.source,
                conn.target,
                output=conn.output or "result",
                input=conn.input,
            )

        return fsm

    def _context_to_serializable(self, context: Context) -> SerializableContext:
        """Convert a Context to its serializable representation."""
        return SerializableContext(
            results=context.results,
            metadata=context.metadata,
            metrics=context.metrics,
        )

    def _serializable_to_context(self, serializable: SerializableContext) -> Context:
        """Convert a serializable representation to a Context."""
        context = Context()
        context.results = serializable.results
        context.metadata = serializable.metadata
        context.metrics = serializable.metrics
        return context

    def _context_to_serializable_fsm(
        self, context: FSMContext
    ) -> SerializableFSMContext:
        """Convert an FSMContext to its serializable representation."""
        return SerializableFSMContext(
            results=context.results,
            metadata=context.metadata,
            metrics=context.metrics,
            state_history=context.state_history,
            cycle_results=context.cycle_results._by_state
            if hasattr(context.cycle_results, "_by_state")
            else {},
            cycle_count=context.cycle_count,
        )

    def _serializable_to_fsm_context(
        self, serializable: SerializableFSMContext
    ) -> FSMContext:
        """Convert a serializable representation to an FSMContext."""
        context = FSMContext()
        context.results = serializable.results
        context.metadata = serializable.metadata
        context.metrics = serializable.metrics
        context.state_history = serializable.state_history
        # Convert dict back to CycleResults if needed
        from ..core.context import CycleResults

        if isinstance(serializable.cycle_results, dict):
            cycle_results = CycleResults()
            cycle_results._by_state = serializable.cycle_results
            context.cycle_results = cycle_results
        else:
            context.cycle_results = serializable.cycle_results
        context.cycle_count = serializable.cycle_count
        return context
