import pandas as pd
import json
import os
from kafka import KafkaProducer

# --- CONFIGURATION ---
BOOTSTRAP_SERVERS = ['host.docker.internal:9092'] 
BASE_DIR = '/usr/local/airflow/include/producers'
DATA_DIR = os.path.join(BASE_DIR, 'data')
STATE_FILE = os.path.join(BASE_DIR, 'producer_state.json')

FILE_CONFIG = {
    "products.csv":  {"topic": "chocolate_products", "key": "product_id"},
    "stores.csv":    {"topic": "chocolate_stores",   "key": "store_id"},
    "customers.csv": {"topic": "chocolate_customers", "key": "customer_id"},
    "sales.csv":     {"topic": "chocolate_sales",    "key": "order_id"}
}

def get_next_month(current_month_str):
    """Increments YYYY-MM to the next month."""
    year, month = map(int, current_month_str.split('-'))
    if month == 12:
        return f"{year + 1}-01"
    else:
        return f"{year}-{month + 1:02d}"

def produce_batch(batch_id):
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        acks=1,
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        # CRITICAL FOR DUPLICATES: Serialize the key to enable Kafka Log Compaction
        key_serializer=lambda k: str(k).encode('utf-8'), 
        request_timeout_ms=60000 
    )
    
    # 1. Load or Initialize State
    state = {"target_month": "2023-01", "retry_count": 0, "max_retries": 3}
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r', encoding='utf-8-sig') as f:
            loaded_state = json.load(f)
            # Ensure we are using the new state logic format
            if 'target_month' in loaded_state:
                state = loaded_state

    target_month = state['target_month']

    # 2. The XCom Payload
    ingestion_metadata = {
        "batch_id": batch_id,
        "period": target_month, # For your Airflow email reporting
        "active_topics": [],
        "counts": {}, 
        "total_new_records": 0,
        "status": "success"
    }

    # 3. Filter Sales First (The Sieve)
    sales_path = os.path.join(DATA_DIR, 'sales.csv')
    df_sales = pd.read_csv(sales_path)
    
    # Convert M/D/YYYY to datetime, then filter by YYYY-MM
    df_sales['order_date'] = pd.to_datetime(df_sales['order_date'])
    monthly_sales = df_sales[df_sales['order_date'].dt.strftime('%Y-%m') == target_month]

    # 4. Handle Empty Data (Wait or Increment)
    if monthly_sales.empty:
        if state['retry_count'] < state['max_retries']:
            state['retry_count'] += 1
            ingestion_metadata["status"] = f"waiting_for_data (Attempt {state['retry_count']})"
        else:
            state['target_month'] = get_next_month(target_month)
            state['retry_count'] = 0
            ingestion_metadata["status"] = "skipped_month_max_retries"

        with open(STATE_FILE, 'w', encoding='utf-8') as f:
            json.dump(state, f)
        producer.close()
        return ingestion_metadata

    # 5. Extract Active IDs for this batch
    active_ids_map = {
        "customers.csv": set(monthly_sales['customer_id'].dropna()),
        "products.csv": set(monthly_sales['product_id'].dropna()),
        "stores.csv": set(monthly_sales['store_id'].dropna())
    }

    # 6. Produce Data
    for file_name, config in FILE_CONFIG.items():
        path = os.path.join(DATA_DIR, file_name)
        if not os.path.exists(path):
            continue
        
        topic_name = config['topic']
        key_col = config['key']

        # Determine which dataframe to produce
        if file_name == 'sales.csv':
            df_to_produce = monthly_sales.copy()
            # Convert timestamp back to string for JSON serialization
            df_to_produce['order_date'] = df_to_produce['order_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        else:
            # Filter dimension tables based on the active IDs from sales
            df_dim = pd.read_csv(path)
            active_ids = active_ids_map.get(file_name, set())
            df_to_produce = df_dim[df_dim[key_col].isin(active_ids)].copy()

        record_count = len(df_to_produce)
        if record_count == 0:
            continue

        for _, row in df_to_produce.iterrows():
            message = row.to_dict()
            message['batch_id'] = batch_id 
            
            # Send message WITH the key to enforce log compaction
            producer.send(topic_name, key=message[key_col], value=message)
        
        # Update XCom Data
        ingestion_metadata["active_topics"].append(topic_name)
        ingestion_metadata["counts"][topic_name] = record_count
        ingestion_metadata["total_new_records"] += record_count

    producer.flush() 
    producer.close()
    
    # 7. Update State for the next run
    state['target_month'] = get_next_month(target_month)
    state['retry_count'] = 0
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f)
        
    # 8. Return payload to Airflow
    return ingestion_metadata