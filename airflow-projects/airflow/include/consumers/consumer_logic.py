import json
import csv
import os
import duckdb
import uuid
from kafka import KafkaConsumer

# --- CONFIGURATION ---
STG_DIR = '/usr/local/airflow/include/STG'
DB_PATH = os.path.join(STG_DIR, 'chocolate_warehouse.duckdb')
BOOTSTRAP_SERVERS = ['host.docker.internal:9092']

TABLE_CONFIG = {
    'chocolate_sales':     {'table': 'raw_sales',     'pk': 'order_id'},
    'chocolate_products':  {'table': 'raw_products',  'pk': 'product_id'},
    'chocolate_customers': {'table': 'raw_customers', 'pk': 'customer_id'},
    'chocolate_stores':    {'table': 'raw_stores',    'pk': 'store_id'}
}

def consume_to_duckdb(**kwargs):
    os.makedirs(STG_DIR, exist_ok=True)
    
    # Generate a unique group ID for every run to bypass the Kafka Coordinator bug
    fixed_group = "CSV_To_DuckDB_V4"
    print(f"🚀 Starting consumer with fixed group: {fixed_group}")

    # # Generate a unique group ID for every run to bypass the Kafka Coordinator bug
    # unique_group = f"chocolate_run_{uuid.uuid4().hex[:8]}"
    # print(f"🚀 Starting consumer with throwaway group: {unique_group}")

    # 1. Initialize Consumer 
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            enable_auto_commit=True,      # No need to commit, we use a new group every time
            group_id=fixed_group, 
            api_version=(2, 8, 0),         
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=10000 
        )
    except Exception as e:
        print(f"❌ Failed to connect to Kafka: {e}")
        return None

    all_topics = consumer.topics()
    target_topics = [t for t in all_topics if t in TABLE_CONFIG]
    
    if not target_topics:
        print("⚠️ No valid chocolate topics found. Exiting.")
        consumer.close()
        return None

    consumer.subscribe(target_topics)
    print(f"🎯 Subscribed to topics: {target_topics}")

    files = {}
    writers = {}
    topic_counts = {}
    processed_count = 0

    print("📥 Downloading messages (Waiting 10s for the stream to finish)...")
    for message in consumer:
        topic = message.topic
        data = message.value
        data['source_system'] = f"kafka_{topic}"

        if topic not in files:
            csv_path = os.path.join(STG_DIR, f"{topic}_temp.csv")
            files[topic] = open(csv_path, 'w', newline='', encoding='utf-8')
            writers[topic] = csv.DictWriter(files[topic], fieldnames=data.keys())
            writers[topic].writeheader()
            topic_counts[topic] = 0 
        
        writers[topic].writerow(data)
        topic_counts[topic] += 1
        processed_count += 1

    consumer.close()
    for f in files.values():
        f.close()

    if processed_count == 0:
        print("ℹ️ No messages found in the cluster. Exiting cleanly.")
        return None

    print(f"\n💾 Downloaded {processed_count} total messages to temp CSVs.")

    # 2. Load CSVs into DuckDB with Exactly-Once Logic
    con = duckdb.connect(DB_PATH)
    consumer_report = {} 
    
    for topic in files.keys():
        csv_path = os.path.join(STG_DIR, f"{topic}_temp.csv")
        cfg = TABLE_CONFIG[topic]
        table_name = cfg['table']
        pk_column = cfg['pk']
        
        if os.path.exists(csv_path):
            print(f"   -> Safely Upserting {topic} into '{table_name}'...")
            
            con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM read_csv_auto('{csv_path}') WHERE 1=0")
            
            try:
                con.execute(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({pk_column})")
            except:
                pass 
                
            # This ignores data we've already ingested, keeping the DB clean
            con.execute(f"INSERT OR IGNORE INTO {table_name} SELECT * FROM read_csv_auto('{csv_path}')")
            
            # Record the count for the email
            consumer_report[topic] = topic_counts[topic]
            
            os.remove(csv_path)
            print(f"   🗑️ Deleted temp file: {csv_path}")

    con.close()
    print(f"\n🏁 All data loaded! Report generated: {consumer_report}")
    
    return consumer_report