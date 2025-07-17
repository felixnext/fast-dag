"""Example demonstrating caching capabilities."""

import time
from pathlib import Path

from fast_dag import DAG
from fast_dag.core.cache import DiskCache, get_cache, register_cache


def simulate_expensive_api_call(user_id: int) -> dict:
    """Simulate an expensive API call."""
    print(f"Making expensive API call for user {user_id}...")
    time.sleep(1)  # Simulate network delay
    return {
        "user_id": user_id,
        "name": f"User {user_id}",
        "email": f"user{user_id}@example.com",
        "credits": user_id * 100,
    }


def process_user_data(user_data: dict) -> dict:
    """Process user data (expensive computation)."""
    print(f"Processing data for {user_data['name']}...")
    time.sleep(0.5)  # Simulate processing
    return {
        "user_id": user_data["user_id"],
        "display_name": user_data["name"].upper(),
        "credit_status": "premium" if user_data["credits"] > 500 else "standard",
        "processed_at": time.time(),
    }


def generate_report(processed_data: dict) -> str:
    """Generate a report from processed data."""
    print(f"Generating report for {processed_data['display_name']}...")
    return (
        f"User Report:\n"
        f"- Name: {processed_data['display_name']}\n"
        f"- Status: {processed_data['credit_status']}\n"
        f"- Generated at: {time.strftime('%Y-%m-%d %H:%M:%S')}"
    )


def memory_cache_example():
    """Example using memory cache (default)."""
    print("=== Memory Cache Example ===\n")

    # Create DAG with cached nodes
    dag = DAG("user_pipeline")

    # Add cached nodes - results will be stored in memory
    dag.add_node("fetch_user", simulate_expensive_api_call)
    dag.nodes["fetch_user"].cached = True
    dag.nodes["fetch_user"].cache_ttl = 60  # Cache for 60 seconds

    dag.add_node("process", process_user_data)
    dag.nodes["process"].cached = True

    dag.add_node("report", generate_report)

    # Connect nodes
    dag.connect("fetch_user", "process", input="user_data")
    dag.connect("process", "report", input="processed_data")

    # First run - all nodes execute
    print("First run (cache miss):")
    start = time.time()
    result1 = dag.run(user_id=123)
    print(f"Time taken: {time.time() - start:.2f}s")
    print(result1)
    print()

    # Second run - cached results used
    print("Second run (cache hit):")
    start = time.time()
    result2 = dag.run(user_id=123)
    print(f"Time taken: {time.time() - start:.2f}s")
    print(result2)
    print()

    # Different input - cache miss again
    print("Third run with different input (cache miss):")
    start = time.time()
    result3 = dag.run(user_id=456)
    print(f"Time taken: {time.time() - start:.2f}s")
    print(result3)
    print()

    # Print cache statistics
    cache = get_cache("memory")
    stats = cache.stats()
    print("\nCache Statistics:")
    print(f"- Hits: {stats['hits']}")
    print(f"- Misses: {stats['misses']}")
    print(f"- Hit Rate: {stats['hit_rate']:.2%}")
    print(f"- Cache Size: {stats['size']} entries")


def disk_cache_example():
    """Example using disk cache for persistence."""
    print("\n\n=== Disk Cache Example ===\n")

    # Register a disk cache
    cache_dir = Path("./cache_example")
    register_cache("persistent", DiskCache(cache_dir))

    # Create DAG with disk-cached nodes
    dag = DAG("persistent_pipeline")

    # Use the decorator syntax for cached nodes
    @dag.cached_node(cache_backend="persistent", cache_ttl=3600)
    def fetch_data(data_id: str) -> dict:
        """Fetch data from slow source."""
        print(f"Fetching data {data_id} from slow source...")
        time.sleep(2)
        return {"id": data_id, "value": f"data_{data_id}"}

    @dag.cached_node(cache_backend="persistent")
    def transform_data(data: dict) -> dict:
        """Transform the data."""
        print(f"Transforming data {data['id']}...")
        time.sleep(1)
        return {"id": data["id"], "transformed": data["value"].upper()}

    # Connect nodes
    dag.connect("fetch_data", "transform_data", input="data")

    # Run pipeline
    print("First run (will be slow):")
    start = time.time()
    result = dag.run(data_id="report_2024")
    print(f"Time taken: {time.time() - start:.2f}s")
    print(f"Result: {result}")

    # Simulate program restart by creating new DAG
    print("\nSimulating program restart...")
    dag2 = DAG("persistent_pipeline")

    # Re-register functions and cache
    register_cache("persistent", DiskCache(cache_dir))

    @dag2.cached_node(cache_backend="persistent", cache_ttl=3600)
    def fetch_data2(data_id: str) -> dict:
        print(f"Fetching data {data_id} from slow source...")
        time.sleep(2)
        return {"id": data_id, "value": f"data_{data_id}"}

    @dag2.cached_node(cache_backend="persistent")
    def transform_data2(data: dict) -> dict:
        print(f"Transforming data {data['id']}...")
        time.sleep(1)
        return {"id": data["id"], "transformed": data["value"].upper()}

    dag2.nodes["fetch_data2"].name = "fetch_data"  # Match original names
    dag2.nodes["transform_data2"].name = "transform_data"
    dag2.connect("fetch_data", "transform_data", input="data")

    # Run again - should use disk cache
    print("\nSecond run after restart (should use disk cache):")
    start = time.time()
    result2 = dag2.run(data_id="report_2024")
    print(f"Time taken: {time.time() - start:.2f}s")
    print(f"Result: {result2}")

    # Clean up
    import shutil

    shutil.rmtree(cache_dir, ignore_errors=True)
    print("\nCleaned up disk cache")


def conditional_caching_example():
    """Example with conditional caching based on results."""
    print("\n\n=== Conditional Caching Example ===\n")

    dag = DAG("conditional_cache")

    @dag.node
    def check_cache_status(request_type: str) -> bool:
        """Determine if we should use cache."""
        # Don't cache real-time requests
        return request_type != "realtime"

    @dag.cached_node(cache_ttl=300)  # 5 minute cache
    def fetch_cached_data(query: str) -> dict:
        """Fetch data with caching."""
        print(f"Fetching cached data for query: {query}")
        time.sleep(1)
        return {"query": query, "data": f"cached_result_{query}", "cached": True}

    @dag.node
    def fetch_realtime_data(query: str) -> dict:
        """Fetch real-time data without caching."""
        print(f"Fetching real-time data for query: {query}")
        time.sleep(0.5)
        return {"query": query, "data": f"realtime_result_{query}", "cached": False}

    # Connect conditional flow
    dag.connect(
        "check_cache_status", "fetch_cached_data", output="true", input="use_cache"
    )
    dag.connect(
        "check_cache_status",
        "fetch_realtime_data",
        output="false",
        input="use_realtime",
    )

    # This is a simplified example - in practice you'd use a more sophisticated
    # routing mechanism to handle the conditional flow properly

    print("Demonstrating cache control:")
    print("- Standard requests use cache")
    print("- Realtime requests bypass cache")


def main():
    """Run all caching examples."""
    memory_cache_example()
    disk_cache_example()
    conditional_caching_example()

    print("\n=== Cache Benefits ===")
    print("1. Performance: Avoid re-computing expensive operations")
    print("2. Cost Savings: Reduce API calls and resource usage")
    print("3. Resilience: Disk cache survives program restarts")
    print("4. Flexibility: Different backends for different needs")
    print("5. Transparency: Caching is automatic once configured")


if __name__ == "__main__":
    main()
