"""FSM module combining all functionality."""

from .serialization import FSMSerialization


class FSM(FSMSerialization):
    """Complete FSM implementation with all functionality.

    This class combines all FSM mixins to provide the full FSM functionality:
    - Core FSM state management (CoreFSM)
    - State decorators and building (FSMBuilder)
    - Execution engine (FSMExecutor)
    - Visualization (FSMVisualization)
    - Serialization (FSMSerialization)
    """

    pass


__all__ = ["FSM"]
