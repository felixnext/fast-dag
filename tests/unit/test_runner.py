"""
Unit tests for DAGRunner and execution strategies.
"""

import asyncio

import pytest

from fast_dag import DAG, Context, DAGRunner, ExecutionMode


class TestRunnerCreation:
    """Test DAGRunner creation and configuration"""

    def test_runner_creation(self):
        """Test creating a basic runner"""
        dag = DAG("test")

        @dag.node
        def task() -> int:
            return 42

        runner = DAGRunner(dag)

        assert runner.dag == dag
        assert runner.mode == ExecutionMode.SEQUENTIAL  # Default
        assert runner.max_workers == 4  # Default

    def test_runner_with_configuration(self):
        """Test creating runner with configuration"""
        dag = DAG("test")
        runner = DAGRunner(
            dag,
            mode=ExecutionMode.PARALLEL,
            max_workers=8,
            timeout=60.0,
            error_strategy="continue",
        )

        assert runner.mode == ExecutionMode.PARALLEL
        assert runner.max_workers == 8
        assert runner.timeout == 60.0
        assert runner.error_strategy == "continue"

    def test_runner_configure_method(self):
        """Test configuring runner after creation"""
        dag = DAG("test")
        runner = DAGRunner(dag)

        runner.configure(
            mode=ExecutionMode.ASYNC, max_workers=2, error_strategy="retry"
        )

        assert runner.mode == ExecutionMode.ASYNC
        assert runner.max_workers == 2
        assert runner.error_strategy == "retry"


class TestSequentialExecution:
    """Test sequential execution mode"""

    def test_sequential_execution_order(self):
        """Test nodes execute in correct order"""
        dag = DAG("test")
        execution_order = []

        @dag.node
        def a() -> int:
            execution_order.append("a")
            return 1

        @dag.node
        def b(x: int) -> int:
            execution_order.append("b")
            return x + 1

        @dag.node
        def c(x: int) -> int:
            execution_order.append("c")
            return x + 1

        dag.nodes["a"] >> dag.nodes["b"] >> dag.nodes["c"]

        runner = DAGRunner(dag, mode=ExecutionMode.SEQUENTIAL)
        result = runner.run()

        assert execution_order == ["a", "b", "c"]
        assert result == 3

    def test_sequential_with_branches(self):
        """Test sequential execution with branches"""
        dag = DAG("test")
        execution_times = {}

        @dag.node
        def start() -> int:
            import time

            execution_times["start"] = time.time()
            return 10

        @dag.node
        def branch1(x: int) -> int:
            import time

            execution_times["branch1"] = time.time()
            return x * 2

        @dag.node
        def branch2(x: int) -> int:
            import time

            execution_times["branch2"] = time.time()
            return x + 5

        dag.nodes["start"] >> [dag.nodes["branch1"], dag.nodes["branch2"]]

        runner = DAGRunner(dag, mode=ExecutionMode.SEQUENTIAL)
        runner.run()

        # In sequential mode, branches still execute one after another
        assert execution_times["start"] < execution_times["branch1"]
        assert execution_times["start"] < execution_times["branch2"]


class TestParallelExecution:
    """Test parallel execution mode"""

    def test_parallel_execution(self):
        """Test parallel execution of independent nodes"""
        dag = DAG("test")

        @dag.node
        def slow1() -> int:
            import time

            time.sleep(0.1)
            return 1

        @dag.node
        def slow2() -> int:
            import time

            time.sleep(0.1)
            return 2

        @dag.node
        def combine(a: int, b: int) -> int:
            return a + b

        dag.connect("slow1", "combine", input="a")
        dag.connect("slow2", "combine", input="b")

        runner = DAGRunner(dag, mode=ExecutionMode.PARALLEL)

        import time

        start_time = time.time()
        result = runner.run()
        duration = time.time() - start_time

        assert result == 3
        # Should take ~0.1s (parallel) not ~0.2s (sequential)
        assert duration < 0.15

    def test_parallel_max_workers(self):
        """Test max_workers limits parallelism"""
        dag = DAG("test")
        execution_tracking = {"concurrent": 0, "max_concurrent": 0}

        def make_node(name: str):
            @dag.node(name=name)
            def node() -> int:
                import time

                execution_tracking["concurrent"] += 1
                execution_tracking["max_concurrent"] = max(
                    execution_tracking["max_concurrent"],
                    execution_tracking["concurrent"],
                )
                time.sleep(0.05)
                execution_tracking["concurrent"] -= 1
                return 1

            return node

        # Create 6 independent nodes
        for i in range(6):
            make_node(f"node_{i}")

        runner = DAGRunner(
            dag, mode=ExecutionMode.PARALLEL, max_workers=3, error_strategy="continue"
        )
        runner.run()

        # Should never exceed max_workers
        assert execution_tracking["max_concurrent"] <= 3

    def test_parallel_dependency_resolution(self):
        """Test parallel execution respects dependencies"""
        dag = DAG("test")
        execution_order = []

        @dag.node
        def a() -> int:
            execution_order.append("a")
            return 1

        @dag.node
        def b() -> int:
            execution_order.append("b")
            return 2

        @dag.node
        def c(x: int, y: int) -> int:
            execution_order.append("c")
            return x + y

        @dag.node
        def d(z: int) -> int:
            execution_order.append("d")
            return z * 2

        # a and b can run in parallel, then c, then d
        dag.connect("a", "c", input="x")
        dag.connect("b", "c", input="y")
        dag.connect("c", "d", input="z")

        runner = DAGRunner(dag, mode=ExecutionMode.PARALLEL)
        result = runner.run()

        # a and b can be in any order, but c must be after both, d must be last
        assert "c" in execution_order
        assert execution_order.index("a") < execution_order.index("c")
        assert execution_order.index("b") < execution_order.index("c")
        assert execution_order.index("c") < execution_order.index("d")
        assert result == 6  # (1 + 2) * 2


class TestAsyncExecution:
    """Test async execution mode"""

    @pytest.mark.asyncio
    async def test_async_execution_mode(self):
        """Test async execution of async nodes"""
        dag = DAG("test")

        @dag.node
        async def async_node1() -> int:
            await asyncio.sleep(0.05)
            return 10

        @dag.node
        async def async_node2() -> int:
            await asyncio.sleep(0.05)
            return 20

        @dag.node
        async def async_combine(a: int, b: int) -> int:
            await asyncio.sleep(0.01)
            return a + b

        dag.connect("async_node1", "async_combine", input="a")
        dag.connect("async_node2", "async_combine", input="b")

        runner = DAGRunner(dag, mode=ExecutionMode.ASYNC)
        result = await runner.run_async()

        assert result == 30

    @pytest.mark.asyncio
    async def test_async_with_sync_nodes(self):
        """Test async execution with mixed sync/async nodes"""
        dag = DAG("test")

        @dag.node
        def sync_start() -> int:
            return 5

        @dag.node
        async def async_process(x: int) -> int:
            await asyncio.sleep(0.01)
            return x * 2

        @dag.node
        def sync_end(x: int) -> str:
            return f"Result: {x}"

        dag.nodes["sync_start"] >> dag.nodes["async_process"] >> dag.nodes["sync_end"]

        runner = DAGRunner(dag, mode=ExecutionMode.ASYNC)
        result = await runner.run_async()

        assert result == "Result: 10"


class TestErrorHandling:
    """Test runner error handling strategies"""

    def test_stop_on_error_strategy(self):
        """Test stop strategy halts on first error"""
        dag = DAG("test")
        executed = []

        @dag.node
        def a() -> int:
            executed.append("a")
            return 1

        @dag.node
        def b() -> int:
            executed.append("b")
            raise ValueError("Intentional error")

        @dag.node
        def c() -> int:
            executed.append("c")
            return 3

        runner = DAGRunner(dag, error_strategy="stop")

        with pytest.raises((ValueError, RuntimeError)):
            runner.run()

        # c should not execute after b fails
        assert "a" in executed
        assert "b" in executed
        assert "c" not in executed

    def test_continue_on_error_strategy(self):
        """Test continue strategy proceeds after errors"""
        dag = DAG("test")

        @dag.node
        def good1() -> int:
            return 1

        @dag.node
        def bad() -> int:
            raise ValueError("Fail")

        @dag.node
        def good2() -> int:
            return 2

        runner = DAGRunner(dag, error_strategy="continue")
        runner.run()

        # Should complete despite error
        assert dag.get("good1") == 1
        assert dag.get("good2") == 2
        assert dag.get("bad") is None

    def test_retry_strategy(self):
        """Test retry strategy for failed nodes"""
        dag = DAG("test")
        attempt_count = {"count": 0}

        @dag.node(retry=3)
        def flaky() -> int:
            attempt_count["count"] += 1
            if attempt_count["count"] < 3:
                raise ValueError(f"Attempt {attempt_count['count']} failed")
            return 42

        runner = DAGRunner(dag, error_strategy="retry")
        result = runner.run()

        assert result == 42
        assert attempt_count["count"] == 3


class TestMetricsCollection:
    """Test execution metrics collection by runner"""

    def test_runner_metrics(self):
        """Test runner collects execution metrics"""
        dag = DAG("test")

        @dag.node
        def fast() -> int:
            return 1

        @dag.node
        def slow() -> int:
            import time

            time.sleep(0.1)
            return 2

        @dag.node
        def combine(a: int, b: int) -> int:
            return a + b

        dag.connect("fast", "combine", input="a")
        dag.connect("slow", "combine", input="b")

        runner = DAGRunner(dag)
        runner.run()

        metrics = runner.get_metrics()

        assert metrics is not None
        assert "total_duration" in metrics
        assert "node_times" in metrics
        assert "fast" in metrics["node_times"]
        assert "slow" in metrics["node_times"]
        assert metrics["node_times"]["slow"] > metrics["node_times"]["fast"]
        assert metrics["nodes_executed"] == 3

    def test_parallel_execution_metrics(self):
        """Test metrics for parallel execution"""
        dag = DAG("test")

        @dag.node
        def a() -> int:
            return 1

        @dag.node
        def b() -> int:
            return 2

        @dag.node
        def c() -> int:
            return 3

        runner = DAGRunner(dag, mode=ExecutionMode.PARALLEL)
        runner.run()

        metrics = runner.get_metrics()

        assert "parallel_groups" in metrics
        # All three nodes should execute in parallel
        assert len(metrics["parallel_groups"][0]) == 3


class TestTimeoutHandling:
    """Test execution timeout handling"""

    @pytest.mark.asyncio
    async def test_global_timeout(self):
        """Test global execution timeout"""
        dag = DAG("test")

        @dag.node
        async def slow_node() -> int:
            await asyncio.sleep(1.0)
            return 42

        runner = DAGRunner(dag, timeout=0.1)

        with pytest.raises(TimeoutError):
            await runner.run_async()

    def test_node_timeout(self):
        """Test per-node timeout"""
        dag = DAG("test")

        @dag.node(timeout=0.1)
        def slow_node() -> int:
            import time

            time.sleep(1.0)
            return 42

        runner = DAGRunner(dag)

        with pytest.raises(Exception, match="timed out"):
            runner.run()


class TestRunnerIntegration:
    """Test runner integration with DAG features"""

    def test_runner_with_context(self):
        """Test runner with custom context"""
        dag = DAG("test")

        @dag.node
        def use_context(x: int, context: Context) -> int:
            previous = context.get("previous", 0)
            return x + previous

        context = Context()
        context.set_result("previous", 100)

        runner = DAGRunner(dag)
        result = runner.run(x=50, context=context)

        assert result == 150

    def test_runner_property_on_dag(self):
        """Test accessing runner as DAG property"""
        dag = DAG("test")

        @dag.node
        def task() -> int:
            return 42

        # Should create default runner
        runner = dag.runner
        assert isinstance(runner, DAGRunner)
        assert runner.dag == dag

        # Should reuse same runner
        assert dag.runner is runner
