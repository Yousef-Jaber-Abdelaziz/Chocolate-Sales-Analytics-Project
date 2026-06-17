from airflow import DAG
from airflow.sdk import Asset
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from datetime import datetime, timedelta

# Import your logic functions
from include.producers.producer_logic import produce_batch
from include.consumers.consumer_logic import consume_to_duckdb

# ✅ 1. Added log_bronze_audit to your imports
from include.utils.dag_helpers import decide_next_step, SUCCESS_EMAIL_HTML, WARNING_EMAIL_HTML
from include.utils.dag_audit_helper import log_bronze_audit

# --- CONFIGURATION ---
MY_EMAIL = 'gabery686p@gmail.com'

duckdb_bronze_asset = Asset("duckdb://chocolate_warehouse/raw_bronze") 

default_args = {
    'owner': 'yousef',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': [MY_EMAIL],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=15),
}

with DAG(
    'Chocolate_Ingestion',
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 4, 28),
    catchup=False,
    render_template_as_native_obj=True, 
    tags=['bronze', 'chocolate_factory']
) as dag:

    produce_task = PythonOperator(
        task_id='produce_to_kafka',
        python_callable=produce_batch,
        op_kwargs={'batch_id': "{{ run_id }}"}
    )

    branch_task = BranchPythonOperator(
        task_id='check_for_new_data',
        python_callable=decide_next_step
    )

    consume_task = PythonOperator(
        task_id='consume_to_duckdb',
        python_callable=consume_to_duckdb,
        op_kwargs={'ingestion_report': "{{ ti.xcom_pull(task_ids='produce_to_kafka') }}"},
        outlets=[duckdb_bronze_asset]
    )

 
    # ==========================================
    # ✅ UPDATED: The Bronze Audit Task
    # ==========================================
    audit_success_task = PythonOperator(
        task_id='log_bronze_success',
        python_callable=log_bronze_audit,
        op_kwargs={
            'batch_run_id': '{{ run_id }}',
            'dag_name': '{{ dag.dag_id }}',
            'status': 'SUCCESS',
            
            # 1. Pull extracted count & watermark from the PRODUCER's metadata
            'records_extracted': "{{ ti.xcom_pull(task_ids='produce_to_kafka').get('total_new_records', 0) }}",
            'last_watermark': "{{ ti.xcom_pull(task_ids='produce_to_kafka').get('period', 'N/A') }}",
            
            # 2. Pull the raw report dictionary from the CONSUMER
            'records_loaded': "{{ ti.xcom_pull(task_ids='consume_to_duckdb') }}",
            
            # 3. Defaulting to 0 since we aren't tracking time in the consumer
            'duration_seconds': 0, 
            'error_message': None
        }
    )

    send_success_email = EmailOperator(
        task_id='send_success_email',
        conn_id='smtp_default',
        from_email=MY_EMAIL,
        to=MY_EMAIL,
        subject='[SUCCESS] Pipeline Report: Bronze Layer Ingestion',
        html_content=SUCCESS_EMAIL_HTML 
    )

    send_warning_email = EmailOperator(
        task_id='send_warning_email',
        conn_id='smtp_default',
        from_email=MY_EMAIL,
        to=MY_EMAIL,
        subject='[NOTICE] Pipeline Skipped: No New Source Data',
        html_content=WARNING_EMAIL_HTML 
    )

    # --- DAG WORKFLOW ---
    produce_task >> branch_task
    branch_task >> [consume_task, send_warning_email]
    consume_task >> [send_success_email, audit_success_task]