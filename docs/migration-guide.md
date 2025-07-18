# Migration Guide

This guide helps you migrate from other workflow orchestration tools to fast-dag.

## Table of Contents

- [From Apache Airflow](#from-apache-airflow)
- [From Prefect](#from-prefect)
- [From Luigi](#from-luigi)
- [From Celery](#from-celery)
- [From Dagster](#from-dagster)
- [From Kedro](#from-kedro)
- [General Migration Tips](#general-migration-tips)
- [Feature Comparison](#feature-comparison)

## From Apache Airflow

### Core Concepts Mapping

| Airflow | fast-dag | Notes |
|---------|----------|-------|
| DAG | DAG | Similar concept, different API |
| Task/Operator | Node | Nodes are simpler, function-based |
| TaskInstance | Node execution | Handled internally |
| XCom | Context.results | Automatic result passing |
| Variable | Context.metadata | Shared state |
| Connection | Config in nodes | No central connection management |

### DAG Definition

**Airflow:**
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'my_dag',
    default_args=default_args,
    description='A simple DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

def extract(**context):
    return {'data': [1, 2, 3]}

def transform(**context):
    ti = context['ti']
    data = ti.xcom_pull(task_ids='extract')
    return {'transformed': [x * 2 for x in data['data']]}

def load(**context):
    ti = context['ti']
    data = ti.xcom_pull(task_ids='transform')
    print(f"Loading: {data}")

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag,
)

extract_task >> transform_task >> load_task
```

**fast-dag:**
```python
from fast_dag import DAG

dag = DAG("my_dag", description="A simple DAG")

@dag.node(retry=1, retry_delay=300.0)  # 5 minutes
def extract():
    return {'data': [1, 2, 3]}

@dag.node(retry=1, retry_delay=300.0)
def transform(data: dict):
    return {'transformed': [x * 2 for x in data['data']]}

@dag.node
def load(transformed: dict):
    print(f"Loading: {transformed}")

# Connections are explicit
dag.connect("extract", "transform", input="data")
dag.connect("transform", "load", input="transformed")

# Or use operator syntax
# dag.nodes["extract"] >> dag.nodes["transform"] >> dag.nodes["load"]
```

### Scheduling

fast-dag doesn't include built-in scheduling. Use external schedulers:

```python
# With cron
# Add to crontab:
# 0 0 * * * /usr/bin/python /path/to/run_dag.py

# run_dag.py
from fast_dag import DAG
import logging

def run_daily_dag():
    dag = create_my_dag()
    try:
        result = dag.run()
        logging.info(f"DAG completed: {result}")
    except Exception as e:
        logging.error(f"DAG failed: {e}")
        # Send alerts
        raise

if __name__ == "__main__":
    run_daily_dag()
```

Or use APScheduler:
```python
from apscheduler.schedulers.blocking import BlockingScheduler
from fast_dag import DAG

scheduler = BlockingScheduler()

@scheduler.scheduled_job('interval', days=1)
def run_workflow():
    dag = create_my_dag()
    dag.run()

scheduler.start()
```

### Dynamic Task Generation

**Airflow:**
```python
for i in range(10):
    task = PythonOperator(
        task_id=f'process_{i}',
        python_callable=process_item,
        op_kwargs={'item_id': i},
        dag=dag
    )
    previous_task >> task
```

**fast-dag:**
```python
# Create nodes dynamically
for i in range(10):
    @dag.node(name=f"process_{i}")
    def process(item_id: int = i):
        return process_item(item_id)
    
    if i > 0:
        dag.connect(f"process_{i-1}", f"process_{i}")
```

### Sensors and Triggers

**Airflow:**
```python
from airflow.sensors.filesystem import FileSensor

wait_for_file = FileSensor(
    task_id='wait_for_file',
    filepath='/data/input.csv',
    poke_interval=30,
    dag=dag
)
```

**fast-dag:**
```python
import time
import os

@dag.node(retry=10, retry_delay=30.0)
def wait_for_file(filepath: str = '/data/input.csv'):
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Waiting for {filepath}")
    return filepath

# Or use async
@dag.node
async def wait_for_file_async(filepath: str):
    while not os.path.exists(filepath):
        await asyncio.sleep(30)
    return filepath
```

## From Prefect

### Core Concepts Mapping

| Prefect | fast-dag | Notes |
|---------|----------|-------|
| Flow | DAG | Similar concept |
| Task | Node | Functions as nodes |
| Parameter | Input parameters | Direct function parameters |
| State | Context.metadata | Execution state |
| Result | Node returns | Automatic result handling |

### Flow Definition

**Prefect:**
```python
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def extract():
    return [1, 2, 3, 4, 5]

@task
def transform(data):
    return [x * 2 for x in data]

@task
def load(data):
    print(f"Loading {data}")

@flow(name="ETL Pipeline")
def etl_pipeline():
    raw_data = extract()
    transformed_data = transform(raw_data)
    load(transformed_data)

# Run the flow
if __name__ == "__main__":
    etl_pipeline()
```

**fast-dag:**
```python
from fast_dag import DAG

dag = DAG("ETL Pipeline")

@dag.cached_node(ttl=3600)  # 1 hour cache
def extract():
    return [1, 2, 3, 4, 5]

@dag.node
def transform(data):
    return [x * 2 for x in data]

@dag.node
def load(data):
    print(f"Loading {data}")

# Connect nodes
dag.connect("extract", "transform", input="data")
dag.connect("transform", "load", input="data")

# Run the DAG
if __name__ == "__main__":
    result = dag.run()
```

### Async Support

**Prefect:**
```python
import asyncio
from prefect import flow, task

@task
async def fetch_data(url):
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        return response.json()

@flow
async def async_flow():
    data = await fetch_data("https://api.example.com")
    return data
```

**fast-dag:**
```python
@dag.node
async def fetch_data(url: str):
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        return response.json()

# fast-dag automatically handles async nodes
result = await dag.run_async()
```

### Mapping and Dynamic Tasks

**Prefect:**
```python
@task
def process_item(item):
    return item * 2

@flow
def dynamic_flow(items):
    results = process_item.map(items)
    return results

dynamic_flow([1, 2, 3, 4, 5])
```

**fast-dag:**
```python
@dag.node
def process_items(items: list):
    # Process in parallel within node
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(lambda x: x * 2, items))
    return results

# Or create dynamic nodes
def create_item_processor(item_id):
    @dag.node(name=f"process_item_{item_id}")
    def process():
        return item_id * 2
    return process

for i in range(5):
    create_item_processor(i)
```

## From Luigi

### Core Concepts Mapping

| Luigi | fast-dag | Notes |
|---------|----------|-------|
| Task | Node | Similar concept |
| Requires | Dependencies | Explicit connections |
| Output | Return value | Direct returns |
| Parameter | Function args | Native Python |
| Target | Return value | No file targets needed |

### Task Definition

**Luigi:**
```python
import luigi
import pandas as pd

class ExtractData(luigi.Task):
    date = luigi.DateParameter()
    
    def output(self):
        return luigi.LocalTarget(f'data/raw_{self.date}.csv')
    
    def run(self):
        df = pd.read_sql(f"SELECT * FROM sales WHERE date = '{self.date}'", conn)
        df.to_csv(self.output().path, index=False)

class TransformData(luigi.Task):
    date = luigi.DateParameter()
    
    def requires(self):
        return ExtractData(self.date)
    
    def output(self):
        return luigi.LocalTarget(f'data/transformed_{self.date}.csv')
    
    def run(self):
        df = pd.read_csv(self.input().path)
        df['amount'] = df['amount'] * 1.1
        df.to_csv(self.output().path, index=False)

class LoadData(luigi.Task):
    date = luigi.DateParameter()
    
    def requires(self):
        return TransformData(self.date)
    
    def run(self):
        df = pd.read_csv(self.input().path)
        df.to_sql('sales_transformed', conn, if_exists='append')

if __name__ == '__main__':
    luigi.run(['LoadData', '--date', '2024-01-01'])
```

**fast-dag:**
```python
from fast_dag import DAG
import pandas as pd
from datetime import date

dag = DAG("sales_pipeline")

@dag.node
def extract_data(target_date: date):
    df = pd.read_sql(f"SELECT * FROM sales WHERE date = '{target_date}'", conn)
    return df

@dag.node
def transform_data(df: pd.DataFrame):
    df['amount'] = df['amount'] * 1.1
    return df

@dag.node
def load_data(df: pd.DataFrame):
    df.to_sql('sales_transformed', conn, if_exists='append')
    return {"status": "success", "rows": len(df)}

# Connect pipeline
dag.connect("extract_data", "transform_data", input="df")
dag.connect("transform_data", "load_data", input="df")

# Run with parameters
if __name__ == "__main__":
    result = dag.run(inputs={"target_date": date(2024, 1, 1)})
```

### File Dependencies

Luigi uses file targets for dependencies. In fast-dag, use return values or explicit file handling:

```python
# Luigi style file dependencies in fast-dag
@dag.node
def create_file(filename: str):
    with open(filename, 'w') as f:
        f.write("data")
    return filename

@dag.node
def process_file(filename: str):
    if not os.path.exists(filename):
        raise FileNotFoundError(f"Required file {filename} not found")
    
    with open(filename) as f:
        data = f.read()
    
    return process(data)

dag.connect("create_file", "process_file", input="filename")
```

## From Celery

### Core Concepts Mapping

| Celery | fast-dag | Notes |
|---------|----------|-------|
| Task | Node | Similar async execution |
| Chain | Connected nodes | Explicit connections |
| Group | Parallel nodes | Parallel execution mode |
| Chord | Multi-input node | ALL node type |
| Canvas | DAG | Workflow definition |

### Task Definition

**Celery:**
```python
from celery import Celery, chain, group, chord

app = Celery('tasks', broker='redis://localhost:6379')

@app.task
def extract():
    return {'data': [1, 2, 3]}

@app.task
def transform(data):
    return [x * 2 for x in data['data']]

@app.task
def load(results):
    print(f"Loading: {results}")
    return "Success"

# Create workflow
workflow = chain(extract.s(), transform.s(), load.s())

# Execute
result = workflow.apply_async()
```

**fast-dag:**
```python
from fast_dag import DAG

dag = DAG("celery_migration")

@dag.node
def extract():
    return {'data': [1, 2, 3]}

@dag.node
def transform(data: dict):
    return [x * 2 for x in data['data']]

@dag.node
def load(results: list):
    print(f"Loading: {results}")
    return "Success"

# Connect nodes
dag.connect("extract", "transform", input="data")
dag.connect("transform", "load", input="results")

# Execute (use async for similar behavior to Celery)
result = await dag.run_async()
```

### Parallel Execution

**Celery:**
```python
# Parallel tasks
job = group(
    process_item.s(item) for item in items
)
result = job.apply_async()

# Chord (parallel then callback)
callback = chord(
    (process_item.s(item) for item in items),
    combine_results.s()
)
result = callback.apply_async()
```

**fast-dag:**
```python
# Parallel nodes
for i, item in enumerate(items):
    @dag.node(name=f"process_{i}")
    def process():
        return process_item(item)

# Combine results with ALL node
@dag.all()
def combine_results(**results):
    return merge_all(results.values())

# Connect all processors to combiner
for i in range(len(items)):
    dag.connect(f"process_{i}", "combine_results")

# Run in parallel mode
result = dag.run(mode="parallel")
```

### Distributed Execution

fast-dag doesn't provide built-in distributed execution like Celery. For distributed workflows:

```python
# Option 1: Use fast-dag with Celery
from celery import Celery

app = Celery('distributed_dag')

@app.task
def run_dag_node(node_name, inputs):
    # Create DAG
    dag = create_dag()
    # Run specific node
    return dag.run_node(node_name, inputs)

# Option 2: Use Dask
import dask

@dask.delayed
def run_node(node_func, inputs):
    return node_func(**inputs)

# Build Dask graph from DAG
dask_graph = dag.to_dask_graph()
result = dask_graph.compute()
```

## From Dagster

### Core Concepts Mapping

| Dagster | fast-dag | Notes |
|---------|----------|-------|
| Pipeline/Job | DAG | Main workflow container |
| Op/Solid | Node | Computational unit |
| IOManager | Manual I/O | No automatic I/O management |
| Resource | Config/Context | Shared configuration |
| Asset | Node output | Materialized as return values |

### Pipeline Definition

**Dagster:**
```python
from dagster import job, op, Out, In

@op(out=Out(int))
def get_number():
    return 5

@op(ins={"number": In(int)}, out=Out(int))
def multiply_by_two(number):
    return number * 2

@op(ins={"number": In(int)})
def print_number(number):
    print(f"The result is {number}")

@job
def simple_pipeline():
    print_number(multiply_by_two(get_number()))

result = simple_pipeline.execute_in_process()
```

**fast-dag:**
```python
from fast_dag import DAG

dag = DAG("simple_pipeline")

@dag.node
def get_number() -> int:
    return 5

@dag.node
def multiply_by_two(number: int) -> int:
    return number * 2

@dag.node
def print_number(number: int) -> None:
    print(f"The result is {number}")

# Connect nodes
dag.connect("get_number", "multiply_by_two", input="number")
dag.connect("multiply_by_two", "print_number", input="number")

result = dag.run()
```

### Resources and Configuration

**Dagster:**
```python
from dagster import resource, job, op

@resource
def database_resource(init_context):
    return DatabaseConnection(init_context.resource_config["conn_string"])

@op(required_resource_keys={"database"})
def query_database(context):
    return context.resources.database.query("SELECT * FROM users")

@job(resource_defs={"database": database_resource})
def database_pipeline():
    query_database()

result = database_pipeline.execute_in_process(
    run_config={
        "resources": {
            "database": {
                "config": {"conn_string": "postgresql://localhost/mydb"}
            }
        }
    }
)
```

**fast-dag:**
```python
from fast_dag import DAG, Context

dag = DAG("database_pipeline")

# Configuration approach
class Config:
    def __init__(self):
        self.database = None
    
    def setup(self, conn_string):
        self.database = DatabaseConnection(conn_string)

config = Config()

@dag.node
def query_database(context: Context) -> list:
    # Access shared resources through context
    db = context.metadata["config"].database
    return db.query("SELECT * FROM users")

# Setup and run
config.setup("postgresql://localhost/mydb")
result = dag.run(
    context=Context(metadata={"config": config})
)
```

## From Kedro

### Core Concepts Mapping

| Kedro | fast-dag | Notes |
|---------|----------|-------|
| Pipeline | DAG | Workflow definition |
| Node | Node | Same concept |
| DataCatalog | Manual I/O | No built-in catalog |
| Parameters | Config/Context | Configuration handling |
| Hook | Hook/Middleware | Lifecycle management |

### Pipeline Definition

**Kedro:**
```python
from kedro.pipeline import Pipeline, node

def process_data(raw_data):
    return raw_data.dropna()

def create_features(clean_data):
    return add_features(clean_data)

def train_model(features, parameters):
    model = RandomForest(**parameters)
    return model.fit(features)

pipeline = Pipeline([
    node(
        func=process_data,
        inputs="raw_data",
        outputs="clean_data",
        name="process"
    ),
    node(
        func=create_features,
        inputs="clean_data",
        outputs="features",
        name="features"
    ),
    node(
        func=train_model,
        inputs=["features", "parameters"],
        outputs="model",
        name="train"
    )
])
```

**fast-dag:**
```python
from fast_dag import DAG

dag = DAG("ml_pipeline")

@dag.node
def process_data(raw_data):
    return raw_data.dropna()

@dag.node
def create_features(clean_data):
    return add_features(clean_data)

@dag.node
def train_model(features, parameters):
    model = RandomForest(**parameters)
    return model.fit(features)

# Connect pipeline
dag.connect("process_data", "create_features", input="clean_data")
dag.connect("create_features", "train_model", input="features")

# Run with inputs
result = dag.run(inputs={
    "raw_data": load_data(),
    "parameters": {"n_estimators": 100}
})
```

### Data Catalog Pattern

Implement Kedro-like data catalog in fast-dag:

```python
class DataCatalog:
    def __init__(self):
        self.datasets = {}
    
    def register(self, name, loader, saver=None):
        self.datasets[name] = {
            "loader": loader,
            "saver": saver
        }
    
    def load(self, name):
        return self.datasets[name]["loader"]()
    
    def save(self, name, data):
        if self.datasets[name]["saver"]:
            self.datasets[name]["saver"](data)

# Setup catalog
catalog = DataCatalog()
catalog.register(
    "raw_data",
    loader=lambda: pd.read_csv("data/raw.csv"),
    saver=lambda df: df.to_csv("data/raw.csv")
)

# Use in DAG
@dag.node
def load_from_catalog(dataset_name: str, context: Context):
    catalog = context.metadata["catalog"]
    return catalog.load(dataset_name)

# Run with catalog
result = dag.run(
    context=Context(metadata={"catalog": catalog}),
    inputs={"dataset_name": "raw_data"}
)
```

## General Migration Tips

### 1. Start Simple

Begin with a small workflow to understand fast-dag patterns:

```python
# Minimal example
from fast_dag import DAG

dag = DAG("test_migration")

@dag.node
def step1():
    return {"data": "test"}

@dag.node
def step2(data):
    return f"Processed: {data}"

dag.connect("step1", "step2", input="data")
result = dag.run()
```

### 2. Map Concepts Gradually

Create a mapping table for your specific use case:

```python
# Concept mapping helper
class ConceptMapper:
    def __init__(self, source_tool):
        self.source_tool = source_tool
        self.mappings = {
            "airflow": {
                "DAG": "DAG",
                "task": "node",
                "xcom": "context.results"
            },
            "prefect": {
                "flow": "DAG",
                "task": "node",
                "parameter": "input"
            }
        }
    
    def convert(self, concept):
        return self.mappings[self.source_tool].get(concept, concept)
```

### 3. Handle Tool-Specific Features

Some features need workarounds:

```python
# Scheduling (use external scheduler)
import schedule

def run_dag():
    dag = create_my_dag()
    dag.run()

schedule.every().day.at("00:00").do(run_dag)

# Monitoring (add custom monitoring)
@dag.node
def monitored_node(data):
    start = time.time()
    result = process(data)
    send_metric("process_time", time.time() - start)
    return result

# UI (use visualization)
mermaid_diagram = dag.visualize(backend="mermaid")
# Serve diagram via web interface
```

### 4. Testing Strategy

Maintain tests during migration:

```python
import pytest

def test_migrated_workflow():
    # Test individual nodes
    result = my_node(test_input)
    assert result == expected_output
    
    # Test full workflow
    dag = create_dag()
    result = dag.run(inputs=test_inputs)
    assert result == expected_result
    
    # Test error handling
    with pytest.raises(ValueError):
        dag.run(inputs=invalid_inputs)
```

### 5. Incremental Migration

Migrate piece by piece:

```python
# Phase 1: Core logic
def legacy_task():
    # Existing code
    pass

@dag.node
def new_node():
    # Call legacy code
    return legacy_task()

# Phase 2: Full conversion
@dag.node
def fully_migrated_node():
    # Native fast-dag implementation
    pass
```

## Feature Comparison

| Feature | Airflow | Prefect | Luigi | Celery | Dagster | Kedro | fast-dag |
|---------|---------|---------|-------|---------|---------|-------|----------|
| Python-first | ⚪ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Type hints | ⚪ | ✅ | ⚪ | ⚪ | ✅ | ⚪ | ✅ |
| Async support | ⚪ | ✅ | ⚪ | ✅ | ⚪ | ⚪ | ✅ |
| Built-in scheduler | ✅ | ✅ | ✅ | ⚪ | ✅ | ⚪ | ⚪ |
| Distributed | ✅ | ✅ | ⚪ | ✅ | ✅ | ⚪ | ⚪ |
| UI included | ✅ | ✅ | ✅ | ✅ | ✅ | ⚪ | ⚪ |
| Lightweight | ⚪ | ⚪ | ✅ | ⚪ | ⚪ | ⚪ | ✅ |
| FSM support | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ✅ |
| Visualization | ✅ | ✅ | ✅ | ⚪ | ✅ | ✅ | ✅ |
| Caching | ⚪ | ✅ | ⚪ | ✅ | ✅ | ✅ | ✅ |
| Pure Python | ⚪ | ✅ | ✅ | ⚪ | ⚪ | ✅ | ✅ |

## Summary

fast-dag focuses on simplicity and performance, making it ideal for:
- Embedded workflows in applications
- Data processing pipelines
- ML training pipelines
- Business process automation
- Microservice orchestration

Trade-offs when migrating:
- No built-in scheduler (use cron, APScheduler, etc.)
- No built-in UI (use visualization exports)
- No distributed execution (integrate with Celery/Dask if needed)
- Simpler API with less magic

Benefits:
- Lightweight and fast
- Type-safe with full hints
- Native async support
- FSM support for stateful workflows
- Easy to test and debug
- Minimal dependencies

Choose fast-dag when you need a simple, fast, and flexible workflow engine that integrates well with existing Python code.