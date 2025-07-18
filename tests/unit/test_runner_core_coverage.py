"""
Tests for runner core to achieve 100% coverage.
"""

from fast_dag import DAG
from fast_dag.runner.core import CoreDAGRunner, ExecutionMode


class TestRunnerCoreCoverage:
    """Test runner core to achieve 100% coverage"""

    def test_configure_with_timeout(self):
        """Test configure with timeout parameter (line 62)"""
        dag = DAG("test")
        runner = CoreDAGRunner(dag)

        # Test configuring with timeout
        runner.configure(timeout=10.0)
        assert runner.timeout == 10.0

        # Test configuring with None timeout (should preserve the previous value)
        runner.configure(timeout=None)
        assert (
            runner.timeout == 10.0
        )  # Should remain as 10.0 since None means no change

    def test_configure_all_parameters(self):
        """Test configure with all parameters"""
        dag = DAG("test")
        runner = CoreDAGRunner(dag)

        # Test configuring all parameters
        runner.configure(
            mode=ExecutionMode.PARALLEL,
            max_workers=8,
            timeout=30.0,
            error_strategy="continue",
        )

        assert runner.mode == ExecutionMode.PARALLEL
        assert runner.max_workers == 8
        assert runner.timeout == 30.0
        assert runner.error_strategy == "continue"

    def test_get_metrics_with_zero_nodes(self):
        """Test get_metrics when no nodes have been executed"""
        dag = DAG("test")
        runner = CoreDAGRunner(dag)

        metrics = runner.get_metrics()
        assert metrics["avg_node_time"] == 0.0
        assert metrics["nodes_executed"] == 0

    def test_get_metrics_with_executed_nodes(self):
        """Test get_metrics when nodes have been executed"""
        dag = DAG("test")
        runner = CoreDAGRunner(dag)

        # Manually set some metrics
        runner._metrics.total_duration = 10.0
        runner._metrics.nodes_executed = 2
        runner._metrics.node_times = {"node1": 5.0, "node2": 5.0}

        metrics = runner.get_metrics()
        assert metrics["avg_node_time"] == 5.0
        assert metrics["nodes_executed"] == 2
        assert metrics["total_duration"] == 10.0
