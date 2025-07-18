"""
Tests for core nested to improve coverage.
"""

from unittest.mock import MagicMock

from fast_dag.core.nested import DAGNode, FSMNode
from fast_dag.core.types import NodeType


class TestNestedCoverage:
    """Test nested functionality comprehensively"""

    def test_dag_node_init(self):
        """Test DAGNode initialization (line 24)"""

        def test_func():
            return 42

        mock_dag = MagicMock()
        dag_node = DAGNode(
            name="test",
            func=test_func,
            dag=mock_dag,
            node_type=NodeType.DAG,
        )

        assert dag_node.name == "test"
        # DAGNode replaces func with execute_dag wrapper
        assert dag_node.func.__name__ == "execute_dag"
        assert dag_node.node_type == NodeType.DAG
        assert dag_node.dag is mock_dag

    def test_fsm_node_init(self):
        """Test FSMNode initialization (line 40)"""

        def test_func():
            return "next_state"

        mock_fsm = MagicMock()
        fsm_node = FSMNode(
            name="test_state",
            func=test_func,
            fsm=mock_fsm,
            node_type=NodeType.FSM,
        )

        assert fsm_node.name == "test_state"
        # FSMNode replaces func with execute_fsm wrapper
        assert fsm_node.func.__name__ == "execute_fsm"
        assert fsm_node.node_type == NodeType.FSM
        assert fsm_node.fsm is mock_fsm

    def test_dag_node_execute(self):
        """Test DAGNode execute (line 85)"""
        mock_dag = MagicMock()
        mock_dag.run.return_value = 99

        def test_func():
            return mock_dag

        dag_node = DAGNode(
            name="nested",
            func=test_func,
            dag=mock_dag,
            node_type=NodeType.DAG,
        )

        # Execute should call the nested DAG's run method
        result = dag_node.execute({})
        assert result == 99
        mock_dag.run.assert_called_once()

    def test_fsm_node_execute(self):
        """Test FSMNode execute (line 101)"""
        mock_fsm = MagicMock()
        mock_fsm.run.return_value = "final_state"

        def test_func():
            return mock_fsm

        fsm_node = FSMNode(
            name="nested_fsm",
            func=test_func,
            fsm=mock_fsm,
            node_type=NodeType.FSM,
        )

        # Execute should call the nested FSM's run method
        result = fsm_node.execute({})
        assert result == "final_state"
        mock_fsm.run.assert_called_once()

    def test_dag_node_with_context(self):
        """Test DAGNode with context parameter (line 117)"""
        mock_dag = MagicMock()
        mock_dag.run.return_value = 123

        def test_func(x, context):
            return mock_dag

        dag_node = DAGNode(
            name="with_context",
            func=test_func,
            dag=mock_dag,
            node_type=NodeType.DAG,
            inputs=["x"],  # Need to specify inputs
        )

        # Execute with context
        context = MagicMock()
        result = dag_node.execute({"x": 10}, context=context)
        assert result == 123

    def test_fsm_node_with_initial_state(self):
        """Test FSMNode with initial state (line 160)"""
        mock_fsm = MagicMock()
        mock_fsm.run.return_value = "completed"

        def test_func(initial_state):
            return mock_fsm

        fsm_node = FSMNode(
            name="with_initial",
            func=test_func,
            fsm=mock_fsm,
            node_type=NodeType.FSM,
            inputs=["initial_state"],  # Need to specify inputs
        )

        # Execute with initial state
        result = fsm_node.execute({"initial_state": "custom_start"})
        assert result == "completed"
        mock_fsm.run.assert_called_once()
