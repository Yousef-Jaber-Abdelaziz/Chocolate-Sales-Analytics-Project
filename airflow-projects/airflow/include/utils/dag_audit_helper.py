from airflow.providers.postgres.hooks.postgres import PostgresHook

def log_bronze_audit(batch_run_id, dag_name, status, records_extracted, records_loaded, last_watermark, duration_seconds, error_message=None):
    """Inserts a detailed ingestion record into the bronze_audit_log table."""
    
    # ✅ NEW: If the consumer passed us a dictionary of table counts, sum them up!
    if isinstance(records_loaded, dict):
        records_loaded = sum(records_loaded.values())
    elif not isinstance(records_loaded, int):
        records_loaded = 0  # Fallback just in case it's empty

    pg_hook = PostgresHook(postgres_conn_id='chocolate_postgres') 
    
    insert_sql = """
        INSERT INTO audit.bronze_audit_log 
        (batch_run_id, dag_name, status, records_extracted, records_loaded, last_watermark, duration_seconds, error_message)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    parameters = (batch_run_id, dag_name, status, records_extracted, records_loaded, last_watermark, duration_seconds, error_message)
    pg_hook.run(insert_sql, parameters=parameters)
    print(f"✅ Bronze Audit log inserted into 'audit' schema: {records_loaded} records successfully recorded.")


def log_gold_audit(batch_run_id, dag_name, status, records_transferred, error_message=None):
    """Inserts a detailed record into the Gold DWH audit table."""
    
    # Clean up the XCom value just in case it's missing or badly formatted
    if not isinstance(records_transferred, int):
        try:
            records_transferred = int(records_transferred)
        except (ValueError, TypeError):
            records_transferred = 0

    # Use your working connection
    pg_hook = PostgresHook(postgres_conn_id='chocolate_postgres') 
    
    insert_sql = """
        INSERT INTO audit.gold_audit_log 
        (batch_run_id, dag_name, status, records_transferred, error_message)
        VALUES (%s, %s, %s, %s, %s);
    """
    
    parameters = (batch_run_id, dag_name, status, records_transferred, error_message)
    pg_hook.run(insert_sql, parameters=parameters)
    print(f"✅ Gold Audit log inserted into 'audit' schema: {records_transferred} records successfully transferred to Postgres.")