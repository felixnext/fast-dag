# Examples Gallery

This gallery showcases real-world examples of fast-dag workflows, from simple data pipelines to complex state machines.

## Table of Contents

- [Data Processing](#data-processing)
  - [ETL Pipeline](#etl-pipeline)
  - [Real-time Stream Processing](#real-time-stream-processing)
  - [Batch Processing](#batch-processing)
- [Machine Learning](#machine-learning)
  - [ML Training Pipeline](#ml-training-pipeline)
  - [Model Serving](#model-serving)
  - [AutoML Workflow](#automl-workflow)
- [Business Workflows](#business-workflows)
  - [Order Processing FSM](#order-processing-fsm)
  - [Approval Workflow](#approval-workflow)
  - [Document Processing](#document-processing)
- [DevOps & Infrastructure](#devops--infrastructure)
  - [CI/CD Pipeline](#cicd-pipeline)
  - [Infrastructure Provisioning](#infrastructure-provisioning)
  - [Monitoring & Alerting](#monitoring--alerting)
- [Integration Examples](#integration-examples)
  - [REST API Backend](#rest-api-backend)
  - [Event-Driven Architecture](#event-driven-architecture)
  - [Microservice Orchestration](#microservice-orchestration)

## Data Processing

### ETL Pipeline

A complete Extract-Transform-Load pipeline with error handling and monitoring.

```python
from fast_dag import DAG, Context
import pandas as pd
from datetime import datetime
import logging

# Setup
logger = logging.getLogger(__name__)
etl_dag = DAG("customer_etl_pipeline")

# Extract Phase
@etl_dag.node(retry=3, retry_delay=5.0)
def extract_customers() -> pd.DataFrame:
    """Extract customer data from multiple sources"""
    # Source 1: Database
    db_customers = pd.read_sql(
        "SELECT * FROM customers WHERE updated_at > ?",
        connection,
        params=[datetime.now() - timedelta(days=1)]
    )
    
    # Source 2: API
    api_customers = fetch_api_customers()
    
    # Source 3: CSV file
    csv_customers = pd.read_csv("daily_customers.csv")
    
    # Combine sources
    all_customers = pd.concat([db_customers, api_customers, csv_customers])
    logger.info(f"Extracted {len(all_customers)} customers")
    
    return all_customers

@etl_dag.node(retry=2)
def extract_transactions() -> pd.DataFrame:
    """Extract transaction data"""
    transactions = pd.read_sql(
        "SELECT * FROM transactions WHERE date >= CURRENT_DATE - INTERVAL '7 days'",
        connection
    )
    return transactions

# Transform Phase
@etl_dag.node
def clean_customer_data(customers: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize customer data"""
    # Remove duplicates
    customers = customers.drop_duplicates(subset=['customer_id'])
    
    # Standardize names
    customers['name'] = customers['name'].str.title().str.strip()
    
    # Validate email
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    customers = customers[customers['email'].str.match(email_pattern)]
    
    # Fill missing values
    customers['country'] = customers['country'].fillna('Unknown')
    customers['created_at'] = pd.to_datetime(customers['created_at'])
    
    return customers

@etl_dag.node
def enrich_customer_data(
    customers: pd.DataFrame,
    transactions: pd.DataFrame
) -> pd.DataFrame:
    """Enrich customers with transaction summary"""
    # Calculate transaction metrics
    transaction_summary = transactions.groupby('customer_id').agg({
        'amount': ['sum', 'mean', 'count'],
        'date': ['min', 'max']
    }).reset_index()
    
    transaction_summary.columns = [
        'customer_id', 'total_spent', 'avg_transaction',
        'transaction_count', 'first_transaction', 'last_transaction'
    ]
    
    # Merge with customers
    enriched = customers.merge(
        transaction_summary,
        on='customer_id',
        how='left'
    )
    
    # Calculate customer lifetime value (CLV)
    enriched['clv_score'] = (
        enriched['total_spent'] * 0.4 +
        enriched['transaction_count'] * 10 +
        (datetime.now() - enriched['first_transaction']).dt.days * 0.1
    )
    
    # Segment customers
    enriched['segment'] = pd.cut(
        enriched['clv_score'],
        bins=[0, 100, 500, 1000, float('inf')],
        labels=['Bronze', 'Silver', 'Gold', 'Platinum']
    )
    
    return enriched

# Load Phase
@etl_dag.node(timeout=60.0)
def validate_data(enriched_customers: pd.DataFrame) -> pd.DataFrame:
    """Validate data before loading"""
    required_columns = [
        'customer_id', 'name', 'email', 'segment', 'clv_score'
    ]
    
    # Check required columns
    missing_cols = set(required_columns) - set(enriched_customers.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Check data types
    if not pd.api.types.is_numeric_dtype(enriched_customers['clv_score']):
        raise TypeError("CLV score must be numeric")
    
    # Check for nulls in critical fields
    null_counts = enriched_customers[required_columns].isnull().sum()
    if null_counts.any():
        logger.warning(f"Null values found: {null_counts[null_counts > 0]}")
    
    return enriched_customers

@etl_dag.node
def load_to_warehouse(validated_customers: pd.DataFrame, context: Context) -> dict:
    """Load data to data warehouse"""
    # Add metadata
    validated_customers['etl_timestamp'] = datetime.now()
    validated_customers['etl_version'] = context.metadata.get('version', '1.0')
    
    # Load to staging
    validated_customers.to_sql(
        'customers_staging',
        warehouse_connection,
        if_exists='replace',
        index=False
    )
    
    # Run merge procedure
    warehouse_connection.execute("""
        MERGE customers AS target
        USING customers_staging AS source
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN UPDATE SET ...
        WHEN NOT MATCHED THEN INSERT ...
    """)
    
    # Record metrics
    metrics = {
        'records_processed': len(validated_customers),
        'load_timestamp': datetime.now().isoformat(),
        'segments': validated_customers['segment'].value_counts().to_dict()
    }
    
    return metrics

@etl_dag.node
def send_notifications(metrics: dict, context: Context) -> None:
    """Send completion notifications"""
    # Send email summary
    send_email(
        to=context.metadata.get('notification_email', 'data-team@company.com'),
        subject=f"ETL Pipeline Completed - {metrics['load_timestamp']}",
        body=f"""
        ETL Pipeline Summary:
        - Records Processed: {metrics['records_processed']}
        - Segments: {metrics['segments']}
        - Duration: {context.metrics.get('execution_time', 0):.2f}s
        """
    )
    
    # Send metrics to monitoring
    send_metrics_to_datadog({
        'etl.records_processed': metrics['records_processed'],
        'etl.execution_time': context.metrics.get('execution_time', 0)
    })

# Connect the pipeline
etl_dag.connect("extract_customers", "clean_customer_data", input="customers")
etl_dag.connect("clean_customer_data", "enrich_customer_data", input="customers")
etl_dag.connect("extract_transactions", "enrich_customer_data", input="transactions")
etl_dag.connect("enrich_customer_data", "validate_data", input="enriched_customers")
etl_dag.connect("validate_data", "load_to_warehouse", input="validated_customers")
etl_dag.connect("load_to_warehouse", "send_notifications", input="metrics")

# Run with monitoring
if __name__ == "__main__":
    try:
        result = etl_dag.run(
            context=Context(metadata={"version": "2.0", "run_id": str(uuid.uuid4())}),
            error_strategy="fail_fast"
        )
        print(f"ETL completed successfully: {result}")
    except Exception as e:
        logger.error(f"ETL failed: {e}")
        send_alert(f"ETL Pipeline Failed: {e}")
        raise
```

### Real-time Stream Processing

Process streaming data with windowing and aggregation.

```python
from fast_dag import DAG
import asyncio
from collections import defaultdict
from datetime import datetime, timedelta
import json

stream_dag = DAG("realtime_analytics")

# Window management
class TimeWindow:
    def __init__(self, duration_seconds=60):
        self.duration = timedelta(seconds=duration_seconds)
        self.windows = defaultdict(list)
    
    def add(self, timestamp, data):
        window_start = timestamp.replace(second=0, microsecond=0)
        self.windows[window_start].append(data)
        self._cleanup(timestamp)
    
    def get_complete_windows(self, current_time):
        complete = []
        for window_start, data in list(self.windows.items()):
            if current_time - window_start > self.duration:
                complete.append((window_start, data))
                del self.windows[window_start]
        return complete
    
    def _cleanup(self, current_time):
        cutoff = current_time - self.duration * 2
        old_windows = [w for w in self.windows if w < cutoff]
        for w in old_windows:
            del self.windows[w]

# Stream processing nodes
@stream_dag.node
async def consume_events(stream_config: dict) -> AsyncIterator[dict]:
    """Consume events from Kafka/Kinesis/etc"""
    async with EventStreamConsumer(stream_config) as consumer:
        async for message in consumer:
            event = json.loads(message.value)
            event['timestamp'] = datetime.fromisoformat(event['timestamp'])
            yield event

@stream_dag.node
def parse_and_validate(event: dict) -> dict | None:
    """Parse and validate incoming events"""
    # Schema validation
    required_fields = ['user_id', 'event_type', 'timestamp', 'properties']
    if not all(field in event for field in required_fields):
        logger.warning(f"Invalid event: {event}")
        return None
    
    # Parse event type
    event['category'] = event['event_type'].split('.')[0]
    event['action'] = event['event_type'].split('.')[1] if '.' in event['event_type'] else 'unknown'
    
    # Extract metrics
    if event['category'] == 'purchase':
        event['revenue'] = event['properties'].get('amount', 0)
    
    return event

@stream_dag.node
def window_aggregator(valid_events: AsyncIterator[dict]) -> AsyncIterator[dict]:
    """Aggregate events in time windows"""
    window = TimeWindow(duration_seconds=60)
    
    async for event in valid_events:
        if event:
            window.add(event['timestamp'], event)
        
        # Check for complete windows
        complete_windows = window.get_complete_windows(datetime.now())
        for window_start, events in complete_windows:
            # Aggregate window data
            aggregated = {
                'window_start': window_start,
                'window_end': window_start + window.duration,
                'event_count': len(events),
                'unique_users': len(set(e['user_id'] for e in events)),
                'events_by_type': defaultdict(int),
                'revenue': sum(e.get('revenue', 0) for e in events),
                'categories': defaultdict(int)
            }
            
            for event in events:
                aggregated['events_by_type'][event['event_type']] += 1
                aggregated['categories'][event['category']] += 1
            
            yield aggregated

@stream_dag.node
async def enrich_with_user_data(aggregated: dict) -> dict:
    """Enrich aggregated data with user information"""
    # Get unique users in window
    user_ids = aggregated['unique_users']
    
    # Batch fetch user data
    user_data = await fetch_user_profiles(user_ids)
    
    # Calculate user segments
    segments = defaultdict(int)
    for user in user_data:
        segments[user.get('segment', 'unknown')] += 1
    
    aggregated['user_segments'] = dict(segments)
    aggregated['premium_user_ratio'] = segments.get('premium', 0) / len(user_ids) if user_ids else 0
    
    return aggregated

@stream_dag.node
def calculate_metrics(enriched_window: dict) -> dict:
    """Calculate business metrics"""
    metrics = {
        'timestamp': enriched_window['window_end'],
        'events_per_minute': enriched_window['event_count'],
        'unique_users_per_minute': enriched_window['unique_users'],
        'revenue_per_minute': enriched_window['revenue'],
        'avg_revenue_per_user': enriched_window['revenue'] / enriched_window['unique_users'] if enriched_window['unique_users'] > 0 else 0,
        'engagement_score': calculate_engagement_score(enriched_window),
        'top_events': sorted(
            enriched_window['events_by_type'].items(),
            key=lambda x: x[1],
            reverse=True
        )[:5]
    }
    
    # Detect anomalies
    metrics['anomalies'] = detect_anomalies(metrics, historical_data)
    
    return metrics

@stream_dag.node
async def publish_metrics(metrics: dict) -> None:
    """Publish metrics to various destinations"""
    # Send to time-series database
    await influxdb_client.write({
        'measurement': 'realtime_analytics',
        'time': metrics['timestamp'],
        'fields': {
            'events_per_minute': metrics['events_per_minute'],
            'revenue_per_minute': metrics['revenue_per_minute'],
            'unique_users': metrics['unique_users_per_minute']
        }
    })
    
    # Update dashboard
    await redis_client.setex(
        'dashboard:current_metrics',
        60,  # TTL
        json.dumps(metrics)
    )
    
    # Check alerts
    if metrics['anomalies']:
        await send_alert({
            'type': 'anomaly_detected',
            'metrics': metrics,
            'anomalies': metrics['anomalies']
        })
    
    # Broadcast to WebSocket subscribers
    await websocket_broadcast({
        'type': 'metrics_update',
        'data': metrics
    })

# Connect stream processing pipeline
stream_dag.connect("consume_events", "parse_and_validate", input="event")
stream_dag.connect("parse_and_validate", "window_aggregator", input="valid_events")
stream_dag.connect("window_aggregator", "enrich_with_user_data", input="aggregated")
stream_dag.connect("enrich_with_user_data", "calculate_metrics", input="enriched_window")
stream_dag.connect("calculate_metrics", "publish_metrics", input="metrics")

# Run stream processor
async def run_stream_processor():
    # Start with stream configuration
    stream_config = {
        'bootstrap_servers': 'localhost:9092',
        'topic': 'user_events',
        'group_id': 'analytics_processor'
    }
    
    # Run continuously
    while True:
        try:
            await stream_dag.run_async(
                inputs={'stream_config': stream_config},
                mode="streaming"  # Special mode for continuous processing
            )
        except Exception as e:
            logger.error(f"Stream processing error: {e}")
            await asyncio.sleep(5)  # Backoff before retry
```

## Machine Learning

### ML Training Pipeline

Complete machine learning pipeline with experiment tracking.

```python
from fast_dag import DAG, Context
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score
import mlflow
import joblib

ml_dag = DAG("ml_training_pipeline")

@ml_dag.node
def load_training_data(data_path: str) -> pd.DataFrame:
    """Load and perform initial data checks"""
    data = pd.read_parquet(data_path)
    
    print(f"Loaded {len(data)} records with {len(data.columns)} features")
    print(f"Target distribution:\n{data['target'].value_counts()}")
    
    # Check for data drift
    if check_data_drift(data, reference_data):
        logger.warning("Data drift detected!")
    
    return data

@ml_dag.node
def feature_engineering(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create features for model training"""
    data = raw_data.copy()
    
    # Time-based features
    data['hour'] = pd.to_datetime(data['timestamp']).dt.hour
    data['day_of_week'] = pd.to_datetime(data['timestamp']).dt.dayofweek
    data['is_weekend'] = data['day_of_week'].isin([5, 6]).astype(int)
    
    # Aggregated features
    user_stats = data.groupby('user_id').agg({
        'amount': ['mean', 'std', 'count'],
        'session_duration': ['mean', 'max']
    })
    user_stats.columns = ['_'.join(col) for col in user_stats.columns]
    data = data.merge(user_stats, on='user_id', how='left')
    
    # Interaction features
    data['amount_per_duration'] = data['amount'] / (data['session_duration'] + 1)
    data['high_value_user'] = (data['amount_mean'] > data['amount'].quantile(0.75)).astype(int)
    
    # Encode categorical variables
    categorical_cols = ['category', 'user_segment', 'device_type']
    data = pd.get_dummies(data, columns=categorical_cols, drop_first=True)
    
    return data

@ml_dag.node
def prepare_training_data(
    engineered_data: pd.DataFrame,
    test_size: float = 0.2,
    random_state: int = 42
) -> dict:
    """Split and scale data for training"""
    # Separate features and target
    feature_cols = [col for col in engineered_data.columns if col not in ['target', 'user_id', 'timestamp']]
    X = engineered_data[feature_cols]
    y = engineered_data['target']
    
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state, stratify=y
    )
    
    # Scale features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    return {
        'X_train': X_train_scaled,
        'X_test': X_test_scaled,
        'y_train': y_train,
        'y_test': y_test,
        'feature_names': feature_cols,
        'scaler': scaler
    }

@ml_dag.node
def train_model(
    prepared_data: dict,
    context: Context
) -> dict:
    """Train model with hyperparameter tuning"""
    # Start MLflow run
    mlflow.start_run(run_name=f"training_{context.metadata.get('run_id', 'default')}")
    
    # Log parameters
    mlflow.log_param("test_size", 0.2)
    mlflow.log_param("features_count", len(prepared_data['feature_names']))
    
    # Define parameter grid
    param_grid = {
        'n_estimators': [100, 200, 300],
        'max_depth': [10, 20, None],
        'min_samples_split': [2, 5, 10],
        'min_samples_leaf': [1, 2, 4]
    }
    
    # Grid search with cross-validation
    rf = RandomForestClassifier(random_state=42, n_jobs=-1)
    grid_search = GridSearchCV(
        rf, param_grid, cv=5, 
        scoring='roc_auc', 
        verbose=1, 
        n_jobs=-1
    )
    
    # Train model
    grid_search.fit(prepared_data['X_train'], prepared_data['y_train'])
    
    # Log best parameters
    mlflow.log_params(grid_search.best_params_)
    mlflow.log_metric("cv_best_score", grid_search.best_score_)
    
    # Get feature importance
    feature_importance = pd.DataFrame({
        'feature': prepared_data['feature_names'],
        'importance': grid_search.best_estimator_.feature_importances_
    }).sort_values('importance', ascending=False)
    
    # Log feature importance
    mlflow.log_dict(
        feature_importance.head(20).to_dict(),
        "feature_importance_top20.json"
    )
    
    mlflow.end_run()
    
    return {
        'model': grid_search.best_estimator_,
        'cv_results': grid_search.cv_results_,
        'feature_importance': feature_importance,
        'best_params': grid_search.best_params_
    }

@ml_dag.node
def evaluate_model(
    model_data: dict,
    prepared_data: dict,
    context: Context
) -> dict:
    """Evaluate model performance"""
    model = model_data['model']
    X_test = prepared_data['X_test']
    y_test = prepared_data['y_test']
    
    # Make predictions
    y_pred = model.predict(X_test)
    y_pred_proba = model.predict_proba(X_test)[:, 1]
    
    # Calculate metrics
    metrics = {
        'accuracy': model.score(X_test, y_test),
        'roc_auc': roc_auc_score(y_test, y_pred_proba),
        'classification_report': classification_report(y_test, y_pred, output_dict=True)
    }
    
    # Start new MLflow run for evaluation
    mlflow.start_run(run_name=f"evaluation_{context.metadata.get('run_id', 'default')}")
    
    # Log metrics
    mlflow.log_metric("test_accuracy", metrics['accuracy'])
    mlflow.log_metric("test_roc_auc", metrics['roc_auc'])
    
    # Log confusion matrix
    from sklearn.metrics import confusion_matrix
    cm = confusion_matrix(y_test, y_pred)
    mlflow.log_dict({"confusion_matrix": cm.tolist()}, "confusion_matrix.json")
    
    # Create evaluation plots
    create_evaluation_plots(y_test, y_pred, y_pred_proba)
    mlflow.log_artifacts("plots/", artifact_path="evaluation_plots")
    
    mlflow.end_run()
    
    return metrics

@ml_dag.node
def save_model(
    model_data: dict,
    prepared_data: dict,
    metrics: dict,
    context: Context
) -> dict:
    """Save model and artifacts"""
    # Create model package
    model_package = {
        'model': model_data['model'],
        'scaler': prepared_data['scaler'],
        'feature_names': prepared_data['feature_names'],
        'metrics': metrics,
        'metadata': {
            'version': context.metadata.get('model_version', '1.0'),
            'trained_at': datetime.now().isoformat(),
            'run_id': context.metadata.get('run_id'),
            'git_commit': get_git_commit_hash()
        }
    }
    
    # Save to file
    model_path = f"models/model_{context.metadata.get('run_id', 'latest')}.pkl"
    joblib.dump(model_package, model_path)
    
    # Register in model registry
    mlflow.sklearn.log_model(
        model_data['model'],
        "model",
        registered_model_name="customer_churn_model",
        input_example=prepared_data['X_train'][:5]
    )
    
    # Deploy if metrics meet threshold
    if metrics['roc_auc'] > 0.85:
        deploy_model(model_path, "production")
        status = "deployed"
    else:
        status = "saved_not_deployed"
    
    return {
        'model_path': model_path,
        'status': status,
        'deployment_info': {
            'endpoint': f"https://api.company.com/models/{context.metadata.get('run_id')}",
            'version': context.metadata.get('model_version', '1.0')
        }
    }

# Connect ML pipeline
ml_dag.connect("load_training_data", "feature_engineering", input="raw_data")
ml_dag.connect("feature_engineering", "prepare_training_data", input="engineered_data")
ml_dag.connect("prepare_training_data", "train_model", input="prepared_data")
ml_dag.connect("train_model", "evaluate_model", input="model_data")
ml_dag.connect("prepare_training_data", "evaluate_model", input="prepared_data")
ml_dag.connect("model_data", "save_model", input="model_data")
ml_dag.connect("prepared_data", "save_model", input="prepared_data")
ml_dag.connect("evaluate_model", "save_model", input="metrics")

# Run training
if __name__ == "__main__":
    context = Context(metadata={
        'run_id': str(uuid.uuid4()),
        'model_version': '2.0',
        'experiment_name': 'customer_churn_v2'
    })
    
    result = ml_dag.run(
        inputs={'data_path': 's3://ml-data/training/latest.parquet'},
        context=context,
        mode='sequential'  # Ensure proper order for ML pipeline
    )
    
    print(f"Model training completed: {result}")
```

## Business Workflows

### Order Processing FSM

Stateful order processing with complex business logic.

```python
from fast_dag import FSM, FSMContext
from fast_dag.core.types import FSMReturn
from enum import Enum
from datetime import datetime, timedelta
import asyncio

class OrderStatus(Enum):
    PENDING = "pending"
    PAYMENT_PROCESSING = "payment_processing"
    PAYMENT_FAILED = "payment_failed"
    PAID = "paid"
    PREPARING = "preparing"
    READY = "ready"
    DELIVERING = "delivering"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"
    REFUNDED = "refunded"

# Create FSM
order_fsm = FSM(
    "order_processing",
    max_cycles=50,
    terminal_states={
        OrderStatus.DELIVERED.value,
        OrderStatus.CANCELLED.value,
        OrderStatus.REFUNDED.value
    }
)

@order_fsm.state(initial=True)
async def pending(order: dict) -> FSMReturn:
    """Initial order state - validate and prepare for payment"""
    print(f"Processing order {order['id']} for ${order['total']}")
    
    # Validate order
    validation = await validate_order(order)
    if not validation['valid']:
        return FSMReturn(
            next_state=OrderStatus.CANCELLED.value,
            value={
                **order,
                'cancellation_reason': validation['reason'],
                'cancelled_at': datetime.now()
            }
        )
    
    # Check inventory
    inventory_check = await check_inventory(order['items'])
    if not inventory_check['available']:
        # Try to restock
        restock_success = await attempt_restock(inventory_check['missing_items'])
        if not restock_success:
            return FSMReturn(
                next_state=OrderStatus.CANCELLED.value,
                value={
                    **order,
                    'cancellation_reason': 'out_of_stock',
                    'missing_items': inventory_check['missing_items']
                }
            )
    
    # Reserve inventory
    reservation_id = await reserve_inventory(order['items'])
    order['reservation_id'] = reservation_id
    
    return FSMReturn(
        next_state=OrderStatus.PAYMENT_PROCESSING.value,
        value=order
    )

@order_fsm.state
async def payment_processing(order: dict) -> FSMReturn:
    """Process payment with retry logic"""
    retry_count = order.get('payment_retry_count', 0)
    
    try:
        # Process payment
        payment_result = await process_payment({
            'amount': order['total'],
            'currency': order['currency'],
            'payment_method': order['payment_method'],
            'customer_id': order['customer_id']
        })
        
        if payment_result['status'] == 'success':
            order['payment_id'] = payment_result['transaction_id']
            order['paid_at'] = datetime.now()
            
            # Send confirmation
            await send_order_confirmation(order)
            
            return FSMReturn(
                next_state=OrderStatus.PAID.value,
                value=order
            )
        
        elif payment_result['status'] == 'requires_action':
            # 3D Secure or additional verification needed
            order['payment_action_required'] = payment_result['action']
            return FSMReturn(
                next_state=OrderStatus.PAYMENT_PROCESSING.value,
                value=order
            )
        
        else:
            # Payment failed
            order['payment_error'] = payment_result.get('error')
            order['payment_retry_count'] = retry_count + 1
            
            if retry_count < 3:
                # Retry payment
                await asyncio.sleep(5 * (retry_count + 1))  # Exponential backoff
                return FSMReturn(
                    next_state=OrderStatus.PAYMENT_PROCESSING.value,
                    value=order
                )
            else:
                return FSMReturn(
                    next_state=OrderStatus.PAYMENT_FAILED.value,
                    value=order
                )
    
    except Exception as e:
        logger.error(f"Payment processing error: {e}")
        order['payment_error'] = str(e)
        return FSMReturn(
            next_state=OrderStatus.PAYMENT_FAILED.value,
            value=order
        )

@order_fsm.state
async def payment_failed(order: dict) -> FSMReturn:
    """Handle failed payment"""
    # Release inventory reservation
    await release_inventory(order['reservation_id'])
    
    # Notify customer
    await send_payment_failed_notification(order)
    
    # Check if customer wants to retry
    retry_requested = await wait_for_retry_request(
        order['id'],
        timeout=timedelta(hours=24)
    )
    
    if retry_requested:
        # Reset for retry
        order['payment_retry_count'] = 0
        order['retry_requested_at'] = datetime.now()
        return FSMReturn(
            next_state=OrderStatus.PAYMENT_PROCESSING.value,
            value=order
        )
    else:
        return FSMReturn(
            next_state=OrderStatus.CANCELLED.value,
            value={
                **order,
                'cancellation_reason': 'payment_failed',
                'cancelled_at': datetime.now()
            }
        )

@order_fsm.state
async def paid(order: dict) -> FSMReturn:
    """Order paid - prepare for fulfillment"""
    # Allocate to fulfillment center
    fulfillment_center = await allocate_fulfillment_center(order)
    order['fulfillment_center_id'] = fulfillment_center['id']
    order['estimated_preparation_time'] = fulfillment_center['estimated_time']
    
    # Create fulfillment tasks
    tasks = await create_fulfillment_tasks(order)
    order['fulfillment_tasks'] = tasks
    
    # Start preparation
    await notify_fulfillment_center(order)
    
    return FSMReturn(
        next_state=OrderStatus.PREPARING.value,
        value=order
    )

@order_fsm.state
async def preparing(order: dict) -> FSMReturn:
    """Order being prepared"""
    # Check preparation status
    status = await check_preparation_status(order['fulfillment_tasks'])
    
    if status['completed']:
        # Quality check
        qc_result = await perform_quality_check(order)
        if not qc_result['passed']:
            # Redo preparation
            order['qc_failures'] = order.get('qc_failures', 0) + 1
            if order['qc_failures'] < 3:
                await restart_preparation(order)
                return FSMReturn(
                    next_state=OrderStatus.PREPARING.value,
                    value=order
                )
            else:
                # Too many failures - escalate
                await escalate_to_manager(order, "Multiple QC failures")
                return FSMReturn(
                    next_state=OrderStatus.CANCELLED.value,
                    value={**order, 'cancellation_reason': 'qc_failure'}
                )
        
        order['prepared_at'] = datetime.now()
        return FSMReturn(
            next_state=OrderStatus.READY.value,
            value=order
        )
    
    elif status['delayed']:
        # Handle delays
        await notify_customer_delay(order, status['new_estimate'])
        order['estimated_ready_time'] = status['new_estimate']
    
    # Still preparing
    return FSMReturn(
        next_state=OrderStatus.PREPARING.value,
        value=order
    )

@order_fsm.state
async def ready(order: dict) -> FSMReturn:
    """Order ready for delivery"""
    # Assign delivery
    delivery = await assign_delivery_partner(order)
    
    if not delivery:
        # No delivery partners available
        wait_time = order.get('delivery_wait_time', 0) + 5
        if wait_time < 30:  # Wait up to 30 minutes
            await asyncio.sleep(60 * 5)  # Wait 5 minutes
            order['delivery_wait_time'] = wait_time
            return FSMReturn(
                next_state=OrderStatus.READY.value,
                value=order
            )
        else:
            # Escalate
            await escalate_delivery_issue(order)
            # Try alternative delivery
            delivery = await assign_alternative_delivery(order)
    
    order['delivery_partner_id'] = delivery['partner_id']
    order['tracking_number'] = delivery['tracking_number']
    order['estimated_delivery'] = delivery['estimated_time']
    
    # Notify customer
    await send_delivery_notification(order)
    
    return FSMReturn(
        next_state=OrderStatus.DELIVERING.value,
        value=order
    )

@order_fsm.state
async def delivering(order: dict) -> FSMReturn:
    """Order in transit"""
    # Track delivery
    tracking = await get_delivery_status(order['tracking_number'])
    
    if tracking['status'] == 'delivered':
        # Verify delivery
        verification = await verify_delivery(order, tracking)
        if verification['confirmed']:
            order['delivered_at'] = tracking['delivered_at']
            order['delivery_signature'] = tracking.get('signature')
            order['delivery_photo'] = tracking.get('photo_url')
            
            # Send confirmation
            await send_delivery_confirmation(order)
            
            return FSMReturn(
                next_state=OrderStatus.DELIVERED.value,
                value=order
            )
        else:
            # Delivery dispute
            await handle_delivery_dispute(order, verification)
            order['dispute_reason'] = verification['reason']
    
    elif tracking['status'] == 'failed':
        # Delivery failed
        retry_count = order.get('delivery_retry_count', 0)
        if retry_count < 2:
            # Schedule redelivery
            order['delivery_retry_count'] = retry_count + 1
            new_delivery = await schedule_redelivery(order)
            order['estimated_delivery'] = new_delivery['estimated_time']
        else:
            # Return to sender
            await initiate_return_to_sender(order)
            return FSMReturn(
                next_state=OrderStatus.CANCELLED.value,
                value={**order, 'cancellation_reason': 'delivery_failed'}
            )
    
    elif tracking['status'] == 'exception':
        # Handle exceptions (damaged, lost, etc.)
        await handle_delivery_exception(order, tracking['exception'])
    
    # Still delivering
    return FSMReturn(
        next_state=OrderStatus.DELIVERING.value,
        value=order
    )

@order_fsm.state(terminal=True)
async def delivered(order: dict) -> FSMReturn:
    """Order successfully delivered"""
    # Start post-delivery process
    await start_post_delivery_process(order)
    
    # Schedule feedback request
    await schedule_feedback_request(order, delay=timedelta(days=1))
    
    # Update customer metrics
    await update_customer_metrics(order)
    
    return FSMReturn(
        stop=True,
        value={
            'status': 'completed',
            'order': order,
            'message': f"Order {order['id']} delivered successfully"
        }
    )

@order_fsm.state(terminal=True)
async def cancelled(order: dict) -> FSMReturn:
    """Order cancelled"""
    # Process cancellation
    if order.get('payment_id'):
        # Refund if paid
        refund = await process_refund(order)
        order['refund_id'] = refund['id']
        order['refund_amount'] = refund['amount']
    
    # Release inventory
    if order.get('reservation_id'):
        await release_inventory(order['reservation_id'])
    
    # Notify customer
    await send_cancellation_notification(order)
    
    # Analytics
    await record_cancellation_analytics(order)
    
    return FSMReturn(
        stop=True,
        value={
            'status': 'cancelled',
            'order': order,
            'reason': order.get('cancellation_reason', 'unknown')
        }
    )

# Helper function to run order processing
async def process_order(order_data: dict):
    """Process an order through the FSM"""
    context = FSMContext(metadata={
        'order_id': order_data['id'],
        'customer_id': order_data['customer_id'],
        'start_time': datetime.now()
    })
    
    try:
        result = await order_fsm.run_async(
            order=order_data,
            context=context
        )
        
        # Log completion
        logger.info(f"Order {order_data['id']} completed: {result}")
        
        # Send metrics
        await send_order_metrics({
            'order_id': order_data['id'],
            'total_time': (datetime.now() - context.metadata['start_time']).total_seconds(),
            'final_state': order_fsm.current_state,
            'state_transitions': len(order_fsm.state_history)
        })
        
        return result
        
    except Exception as e:
        logger.error(f"Order processing failed: {e}")
        await alert_operations_team(order_data, str(e))
        raise

# Example usage
if __name__ == "__main__":
    order = {
        'id': 'ORD-123456',
        'customer_id': 'CUST-789',
        'items': [
            {'sku': 'PROD-001', 'quantity': 2, 'price': 29.99},
            {'sku': 'PROD-002', 'quantity': 1, 'price': 49.99}
        ],
        'total': 109.97,
        'currency': 'USD',
        'payment_method': {'type': 'credit_card', 'token': 'tok_xxx'},
        'shipping_address': {
            'street': '123 Main St',
            'city': 'San Francisco',
            'state': 'CA',
            'zip': '94105'
        }
    }
    
    asyncio.run(process_order(order))
```

## Integration Examples

### REST API Backend

Expose DAG workflows as REST APIs.

```python
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from fast_dag import DAG
import uuid
from typing import Optional
import asyncio

# Create API models
class WorkflowRequest(BaseModel):
    workflow_name: str
    inputs: dict
    async_execution: bool = False
    webhook_url: Optional[str] = None

class WorkflowResponse(BaseModel):
    execution_id: str
    status: str
    result: Optional[dict] = None
    error: Optional[str] = None

# Create FastAPI app
app = FastAPI(title="Workflow API", version="1.0")

# Workflow registry
workflow_registry = {}

# Execution storage
executions = {}

# Define a sample workflow
data_processing_dag = DAG("data_processing")

@data_processing_dag.node
async def validate_input(data: dict) -> dict:
    if not data.get('source'):
        raise ValueError("Missing required field: source")
    return data

@data_processing_dag.node
async def fetch_data(validated: dict) -> list:
    # Simulate data fetching
    await asyncio.sleep(1)
    return [{"id": i, "value": i * 10} for i in range(10)]

@data_processing_dag.node
async def process_data(data: list) -> dict:
    return {
        "count": len(data),
        "sum": sum(item["value"] for item in data),
        "average": sum(item["value"] for item in data) / len(data)
    }

# Connect workflow
data_processing_dag.connect("validate_input", "fetch_data", input="validated")
data_processing_dag.connect("fetch_data", "process_data", input="data")

# Register workflow
workflow_registry["data_processing"] = data_processing_dag

# API Endpoints
@app.post("/workflows/execute", response_model=WorkflowResponse)
async def execute_workflow(
    request: WorkflowRequest,
    background_tasks: BackgroundTasks
):
    """Execute a workflow"""
    # Validate workflow exists
    if request.workflow_name not in workflow_registry:
        raise HTTPException(status_code=404, detail="Workflow not found")
    
    # Generate execution ID
    execution_id = str(uuid.uuid4())
    
    # Get workflow
    workflow = workflow_registry[request.workflow_name]
    
    if request.async_execution:
        # Execute asynchronously
        executions[execution_id] = {"status": "running"}
        background_tasks.add_task(
            run_workflow_async,
            execution_id,
            workflow,
            request.inputs,
            request.webhook_url
        )
        
        return WorkflowResponse(
            execution_id=execution_id,
            status="running"
        )
    else:
        # Execute synchronously
        try:
            result = await workflow.run_async(inputs=request.inputs)
            executions[execution_id] = {
                "status": "completed",
                "result": result
            }
            
            return WorkflowResponse(
                execution_id=execution_id,
                status="completed",
                result=result
            )
        except Exception as e:
            executions[execution_id] = {
                "status": "failed",
                "error": str(e)
            }
            
            return WorkflowResponse(
                execution_id=execution_id,
                status="failed",
                error=str(e)
            )

@app.get("/workflows/status/{execution_id}", response_model=WorkflowResponse)
async def get_execution_status(execution_id: str):
    """Get workflow execution status"""
    if execution_id not in executions:
        raise HTTPException(status_code=404, detail="Execution not found")
    
    execution = executions[execution_id]
    
    return WorkflowResponse(
        execution_id=execution_id,
        status=execution["status"],
        result=execution.get("result"),
        error=execution.get("error")
    )

@app.get("/workflows")
async def list_workflows():
    """List available workflows"""
    workflows = []
    for name, dag in workflow_registry.items():
        workflows.append({
            "name": name,
            "description": dag.description,
            "nodes": list(dag.nodes.keys()),
            "inputs": get_workflow_inputs(dag)
        })
    
    return {"workflows": workflows}

@app.get("/workflows/{workflow_name}/visualize")
async def visualize_workflow(workflow_name: str, format: str = "mermaid"):
    """Get workflow visualization"""
    if workflow_name not in workflow_registry:
        raise HTTPException(status_code=404, detail="Workflow not found")
    
    workflow = workflow_registry[workflow_name]
    
    if format == "mermaid":
        diagram = workflow.visualize(backend="mermaid")
        return {"format": "mermaid", "diagram": diagram}
    else:
        raise HTTPException(status_code=400, detail="Unsupported format")

# Background task runner
async def run_workflow_async(
    execution_id: str,
    workflow: DAG,
    inputs: dict,
    webhook_url: Optional[str]
):
    """Run workflow in background"""
    try:
        result = await workflow.run_async(inputs=inputs)
        executions[execution_id] = {
            "status": "completed",
            "result": result
        }
        
        # Call webhook if provided
        if webhook_url:
            await call_webhook(webhook_url, {
                "execution_id": execution_id,
                "status": "completed",
                "result": result
            })
            
    except Exception as e:
        executions[execution_id] = {
            "status": "failed",
            "error": str(e)
        }
        
        if webhook_url:
            await call_webhook(webhook_url, {
                "execution_id": execution_id,
                "status": "failed",
                "error": str(e)
            })

# WebSocket support for real-time updates
from fastapi import WebSocket
from typing import Dict

class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
    
    async def connect(self, websocket: WebSocket, execution_id: str):
        await websocket.accept()
        self.active_connections[execution_id] = websocket
    
    def disconnect(self, execution_id: str):
        if execution_id in self.active_connections:
            del self.active_connections[execution_id]
    
    async def send_update(self, execution_id: str, message: dict):
        if execution_id in self.active_connections:
            websocket = self.active_connections[execution_id]
            await websocket.send_json(message)

manager = ConnectionManager()

@app.websocket("/ws/{execution_id}")
async def websocket_endpoint(websocket: WebSocket, execution_id: str):
    """WebSocket for real-time execution updates"""
    await manager.connect(websocket, execution_id)
    
    try:
        # Send current status
        if execution_id in executions:
            await websocket.send_json(executions[execution_id])
        
        # Keep connection alive
        while True:
            await websocket.receive_text()
            
    except Exception:
        manager.disconnect(execution_id)

# Health check
@app.get("/health")
async def health_check():
    return {"status": "healthy", "workflows": len(workflow_registry)}

# Run with: uvicorn api:app --reload
```

This examples gallery demonstrates the versatility of fast-dag across different domains. Each example includes production-ready patterns for error handling, monitoring, and scalability.

For more detailed examples, check the [examples/](https://github.com/felixnext/fast-dag/tree/main/examples) directory in the repository.