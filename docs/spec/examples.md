# Examples

This document provides practical examples of using fast-dag for various workflow patterns.

## Basic Examples

### Simple Data Pipeline

```python
from fast_dag import DAG

# Create workflow
pipeline = DAG("data_processing")

@pipeline.node
def load_data(filepath: str) -> dict:
    """Load data from CSV file"""
    import pandas as pd
    df = pd.read_csv(filepath)
    return {"data": df, "rows": len(df)}

@pipeline.node
def clean_data(data: dict) -> dict:
    """Clean and validate data"""
    df = data["data"]
    # Remove nulls
    df = df.dropna()
    # Remove duplicates
    df = df.drop_duplicates()
    return {"cleaned_data": df, "rows_after": len(df)}

@pipeline.node
def analyze_data(cleaned_data: dict) -> dict:
    """Perform analysis"""
    df = cleaned_data["cleaned_data"]
    return {
        "mean": df.mean(),
        "std": df.std(),
        "summary": df.describe()
    }

# Connect nodes
load_data >> clean_data >> analyze_data

# Execute
result = pipeline.run(filepath="sales_data.csv")
print(result)
```

### Conditional Workflow

```python
from fast_dag import DAG, ConditionalReturn

workflow = DAG("quality_check_pipeline")

@workflow.node
def load_and_validate(filepath: str) -> dict:
    """Load and perform initial validation"""
    data = load_file(filepath)
    return {
        "data": data,
        "is_valid": len(data) > 0,
        "row_count": len(data)
    }

@workflow.condition()
def check_quality(data: dict) -> ConditionalReturn:
    """Branch based on data quality"""
    quality_score = calculate_quality(data["data"])
    return ConditionalReturn(
        condition=quality_score > 0.8,
        value=data
    )

@workflow.node
def process_good_data(data: dict) -> dict:
    """Process high-quality data"""
    return {"status": "processed", "data": transform(data)}

@workflow.node
def handle_poor_data(data: dict) -> dict:
    """Handle low-quality data"""
    return {"status": "needs_review", "data": data}

# Connect with conditional branching
load_and_validate >> check_quality
check_quality.on_true >> process_good_data
check_quality.on_false >> handle_poor_data

# Execute
result = workflow.run(filepath="input.csv")
```

### Parallel Processing

```python
from fast_dag import DAG

parallel_dag = DAG("parallel_processing")

@parallel_dag.node
def split_data(data: list) -> dict:
    """Split data into chunks"""
    chunk_size = len(data) // 4
    return {
        "chunk1": data[:chunk_size],
        "chunk2": data[chunk_size:2*chunk_size],
        "chunk3": data[2*chunk_size:3*chunk_size],
        "chunk4": data[3*chunk_size:]
    }

# Define parallel processors
for i in range(1, 5):
    @parallel_dag.node(name=f"process_chunk_{i}")
    def process_chunk(chunk: list) -> dict:
        """Process data chunk"""
        return {"processed": [transform(item) for item in chunk]}

@parallel_dag.node
def merge_results(
    processed_1: dict,
    processed_2: dict,
    processed_3: dict,
    processed_4: dict
) -> list:
    """Merge processed chunks"""
    result = []
    for p in [processed_1, processed_2, processed_3, processed_4]:
        result.extend(p["processed"])
    return result

# Connect for parallel execution
split_data >> [f"process_chunk_{i}" for i in range(1, 5)] >> merge_results

# Execute with parallel mode
result = parallel_dag.run(
    data=large_dataset,
    mode="parallel",
    max_workers=4
)
```

## Advanced Examples

### Async Web Scraping Pipeline

```python
from fast_dag import DAG
import aiohttp
import asyncio

scraper = DAG("web_scraper")

@scraper.node
async def fetch_urls(seed_url: str) -> list[str]:
    """Fetch list of URLs to scrape"""
    async with aiohttp.ClientSession() as session:
        async with session.get(seed_url) as response:
            data = await response.json()
            return data["urls"]

@scraper.node
async def scrape_page(url: str) -> dict:
    """Scrape individual page"""
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            html = await response.text()
            return {
                "url": url,
                "content": parse_html(html),
                "status": response.status
            }

@scraper.node
def aggregate_results(scraped_data: list[dict]) -> dict:
    """Aggregate scraped data"""
    successful = [d for d in scraped_data if d["status"] == 200]
    failed = [d for d in scraped_data if d["status"] != 200]
    
    return {
        "total": len(scraped_data),
        "successful": len(successful),
        "failed": len(failed),
        "data": successful
    }

# Dynamic parallel scraping
@scraper.node
async def parallel_scrape(urls: list[str]) -> list[dict]:
    """Scrape multiple URLs in parallel"""
    tasks = [scrape_page(url) for url in urls]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Filter out exceptions
    return [r for r in results if not isinstance(r, Exception)]

# Connect
fetch_urls >> parallel_scrape >> aggregate_results

# Execute asynchronously
async def main():
    result = await scraper.run_async(seed_url="https://api.example.com/urls")
    print(f"Scraped {result['successful']} pages successfully")

asyncio.run(main())
```

### Machine Learning Pipeline

```python
from fast_dag import DAG
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

ml_pipeline = DAG("ml_training")

@ml_pipeline.node
def load_dataset(dataset_name: str) -> dict:
    """Load ML dataset"""
    X, y = load_sklearn_dataset(dataset_name)
    return {"X": X, "y": y, "shape": X.shape}

@ml_pipeline.node
def preprocess(X: np.ndarray, y: np.ndarray) -> dict:
    """Preprocess data"""
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    
    # Scale features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    return {
        "X_train": X_train_scaled,
        "X_test": X_test_scaled,
        "y_train": y_train,
        "y_test": y_test,
        "scaler": scaler
    }

@ml_pipeline.node
def train_models(preprocessed: dict) -> dict:
    """Train multiple models"""
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.svm import SVC
    from sklearn.neural_network import MLPClassifier
    
    models = {
        "random_forest": RandomForestClassifier(n_estimators=100),
        "svm": SVC(kernel="rbf"),
        "neural_net": MLPClassifier(hidden_layer_sizes=(100, 50))
    }
    
    trained = {}
    for name, model in models.items():
        model.fit(preprocessed["X_train"], preprocessed["y_train"])
        trained[name] = model
    
    return trained

@ml_pipeline.node
def evaluate_models(models: dict, preprocessed: dict) -> dict:
    """Evaluate and compare models"""
    from sklearn.metrics import accuracy_score, classification_report
    
    results = {}
    for name, model in models.items():
        y_pred = model.predict(preprocessed["X_test"])
        results[name] = {
            "accuracy": accuracy_score(preprocessed["y_test"], y_pred),
            "report": classification_report(preprocessed["y_test"], y_pred)
        }
    
    # Select best model
    best_model = max(results.items(), key=lambda x: x[1]["accuracy"])
    
    return {
        "results": results,
        "best_model": best_model[0],
        "best_accuracy": best_model[1]["accuracy"]
    }

# Connect pipeline
load_dataset >> preprocess >> train_models >> evaluate_models

# Execute
result = ml_pipeline.run(dataset_name="iris")
print(f"Best model: {result['best_model']} with accuracy: {result['best_accuracy']}")
```

## FSM Examples

### Retry Logic State Machine

```python
from fast_dag import FSM, FSMReturn

retry_fsm = FSM("retry_logic", max_cycles=10)

@retry_fsm.state(initial=True)
def attempt_operation(context) -> FSMReturn:
    """Try to perform operation"""
    try:
        result = perform_risky_operation()
        return FSMReturn(
            next_state="success",
            value=result
        )
    except Exception as e:
        retry_count = context.get("retry_count", 0)
        if retry_count >= 3:
            return FSMReturn(
                next_state="failure",
                value=str(e)
            )
        else:
            context.metadata["retry_count"] = retry_count + 1
            return FSMReturn(
                next_state="wait_and_retry",
                value=str(e)
            )

@retry_fsm.state
def wait_and_retry(context) -> FSMReturn:
    """Wait before retrying"""
    import time
    retry_count = context.metadata.get("retry_count", 1)
    wait_time = 2 ** retry_count  # Exponential backoff
    time.sleep(wait_time)
    
    return FSMReturn(next_state="attempt_operation")

@retry_fsm.state(terminal=True)
def success(result: Any) -> FSMReturn:
    """Operation succeeded"""
    return FSMReturn(value={"status": "success", "result": result}, stop=True)

@retry_fsm.state(terminal=True)
def failure(error: str) -> FSMReturn:
    """Operation failed after retries"""
    return FSMReturn(value={"status": "failed", "error": error}, stop=True)

# Execute
result = retry_fsm.run()
```

### Data Processing State Machine

```python
from fast_dag import FSM, FSMReturn

processor = FSM("data_processor", max_cycles=100)

@processor.state(initial=True)
def receive_data(context) -> FSMReturn:
    """Receive incoming data"""
    data = get_next_batch()
    
    if data is None:
        return FSMReturn(next_state="idle")
    
    context.metadata["batch_size"] = len(data)
    return FSMReturn(
        next_state="validate",
        value=data
    )

@processor.state
def validate(data: list, context) -> FSMReturn:
    """Validate data batch"""
    errors = validate_batch(data)
    
    if errors:
        context.metadata["validation_errors"] = errors
        return FSMReturn(
            next_state="error_handling",
            value=data
        )
    
    return FSMReturn(
        next_state="process",
        value=data
    )

@processor.state
def process(data: list) -> FSMReturn:
    """Process valid data"""
    processed = transform_batch(data)
    return FSMReturn(
        next_state="store",
        value=processed
    )

@processor.state
def store(processed_data: list) -> FSMReturn:
    """Store processed data"""
    store_results(processed_data)
    return FSMReturn(next_state="receive_data")

@processor.state
def error_handling(data: list, context) -> FSMReturn:
    """Handle validation errors"""
    errors = context.metadata.get("validation_errors", [])
    
    # Try to fix errors
    fixed_data = attempt_fix(data, errors)
    
    if fixed_data:
        return FSMReturn(
            next_state="process",
            value=fixed_data
        )
    else:
        # Log errors and skip batch
        log_errors(errors)
        return FSMReturn(next_state="receive_data")

@processor.state
def idle(context) -> FSMReturn:
    """Wait for new data"""
    import time
    time.sleep(5)  # Wait 5 seconds
    
    if should_stop():
        return FSMReturn(stop=True)
    
    return FSMReturn(next_state="receive_data")

# Run processor
processor.run()
```

## Integration Examples

### FastAPI Integration

```python
from fastapi import FastAPI, BackgroundTasks
from fast_dag import DAG
import uuid

app = FastAPI()

# Define workflow
etl_pipeline = DAG("etl_pipeline")

@etl_pipeline.node
async def extract_data(source: str) -> dict:
    """Extract data from source"""
    data = await fetch_from_source(source)
    return {"raw_data": data}

@etl_pipeline.node
def transform_data(raw_data: dict) -> dict:
    """Transform data"""
    transformed = apply_transformations(raw_data)
    return {"transformed": transformed}

@etl_pipeline.node
async def load_data(transformed: dict) -> dict:
    """Load into destination"""
    result = await write_to_destination(transformed)
    return {"status": "completed", "records": len(transformed)}

# Connect
extract_data >> transform_data >> load_data

# API endpoints
@app.post("/run-etl")
async def run_etl(source: str, background_tasks: BackgroundTasks):
    """Trigger ETL pipeline"""
    job_id = str(uuid.uuid4())
    
    # Run in background
    background_tasks.add_task(
        etl_pipeline.run_async,
        source=source
    )
    
    return {"job_id": job_id, "status": "started"}

@app.get("/pipeline-status/{job_id}")
async def get_status(job_id: str):
    """Get pipeline execution status"""
    # Implementation depends on job tracking system
    return {"job_id": job_id, "status": "running"}
```

### Celery Integration

```python
from celery import Celery
from fast_dag import DAG

celery_app = Celery("tasks", broker="redis://localhost:6379")

# Define workflow
analysis_pipeline = DAG("analysis")

@analysis_pipeline.node
def fetch_data(query: str) -> dict:
    """Fetch data based on query"""
    return {"data": execute_query(query)}

@analysis_pipeline.node
def analyze(data: dict) -> dict:
    """Perform analysis"""
    return {"results": perform_analysis(data)}

@analysis_pipeline.node
def generate_report(results: dict) -> str:
    """Generate report"""
    return create_report(results)

# Connect
fetch_data >> analyze >> generate_report

# Celery task
@celery_app.task
def run_analysis_pipeline(query: str):
    """Run analysis pipeline as Celery task"""
    result = analysis_pipeline.run(query=query)
    return result

# Usage
task = run_analysis_pipeline.delay("SELECT * FROM sales")
```

## Testing Workflows

### Unit Testing Nodes

```python
import pytest
from fast_dag import DAG, Node

def test_node_execution():
    """Test individual node execution"""
    def add_numbers(a: int, b: int) -> int:
        return a + b
    
    node = Node(add_numbers)
    result = node.execute({"a": 2, "b": 3})
    assert result == 5

def test_node_with_context():
    """Test node with context injection"""
    def process_with_context(data: str, context) -> str:
        previous = context.get("previous", "")
        return f"{previous} -> {data}"
    
    node = Node(process_with_context)
    context = Context(results={"previous": "start"})
    result = node.execute({"data": "end"}, context)
    assert result == "start -> end"
```

### Integration Testing Workflows

```python
import pytest
from fast_dag import DAG

@pytest.fixture
def sample_workflow():
    """Create test workflow"""
    dag = DAG("test_pipeline")
    
    @dag.node
    def step1(input: int) -> int:
        return input * 2
    
    @dag.node
    def step2(value: int) -> int:
        return value + 10
    
    step1 >> step2
    return dag

def test_workflow_execution(sample_workflow):
    """Test complete workflow execution"""
    result = sample_workflow.run(input=5)
    
    # Check final result
    assert result == 20  # (5 * 2) + 10
    
    # Check intermediate results
    assert sample_workflow["step1"] == 10
    assert sample_workflow["step2"] == 20

def test_workflow_validation(sample_workflow):
    """Test workflow validation"""
    errors = sample_workflow.validate()
    assert len(errors) == 0
```

## Performance Optimization

### Caching Results

```python
from fast_dag import DAG
from functools import lru_cache

cached_pipeline = DAG("cached_pipeline")

@cached_pipeline.node
@lru_cache(maxsize=128)
def expensive_computation(input_data: tuple) -> dict:
    """Cache expensive computations"""
    # Convert to tuple for hashability
    result = perform_expensive_operation(input_data)
    return {"result": result}

@cached_pipeline.node
def prepare_input(data: list) -> tuple:
    """Prepare input for caching"""
    # Convert to immutable type
    return tuple(sorted(data))

prepare_input >> expensive_computation
```

### Memory-Efficient Streaming

```python
from fast_dag import DAG
from typing import Generator

streaming_dag = DAG("streaming_pipeline")

@streaming_dag.node
def read_large_file(filepath: str) -> Generator[str, None, None]:
    """Stream file line by line"""
    with open(filepath, "r") as f:
        for line in f:
            yield line.strip()

@streaming_dag.node
def process_stream(lines: Generator[str, None, None]) -> Generator[dict, None, None]:
    """Process streaming data"""
    for line in lines:
        if line:  # Skip empty lines
            yield {"processed": transform_line(line)}

@streaming_dag.node
def aggregate_stream(
    records: Generator[dict, None, None]
) -> dict:
    """Aggregate streaming results"""
    total = 0
    count = 0
    
    for record in records:
        total += record.get("value", 0)
        count += 1
    
    return {"total": total, "count": count, "average": total / count if count > 0 else 0}

# Connect
read_large_file >> process_stream >> aggregate_stream

# Process large file efficiently
result = streaming_dag.run(filepath="huge_dataset.txt")
```