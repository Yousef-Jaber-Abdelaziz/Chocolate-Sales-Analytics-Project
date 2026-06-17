from airflow import DAG
from airflow.sdk import Asset  # ✅ Updated for Airflow 3.x
from airflow.providers.standard.operators.bash import BashOperator    # ✅ Updated import
from airflow.providers.standard.operators.python import PythonOperator # ✅ Updated import
from airflow.providers.smtp.operators.smtp import EmailOperator
  # ✅ New import for emails
from include.utils.dag_helpers import transfer_duckdb_to_postgres
from include.utils.dag_helpers import truncate_duckdb_staging  
from datetime import datetime
import pandas as pd
import duckdb
from sqlalchemy import create_engine
from include.utils.dag_audit_helper import log_gold_audit



# ==========================================
# 2. DAG CONFIGURATION & ALERTS
# ==========================================
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'email': ['your_actual_email@example.com'], # ⚠️ Change this to your real email
    'email_on_failure': True,                   # ✅ Airflow will auto-email on ANY task failure
    'email_on_retry': False,
    'retries': 0,                               # Set to 0 so it fails and emails immediately
}

duckdb_bronze_asset = Asset("duckdb://chocolate_warehouse/raw_bronze")

# --- DAG DEFINITION ---
with DAG(
    dag_id='Chocolate_Staging_and_Transfer',
    default_args=default_args,            # ✅ Attach the default args here
    schedule=[duckdb_bronze_asset],       # ✅ Asset instead of Dataset
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['dbt', 'duckdb', 'postgres'],
) as dag:

    # 1. Run OBT First
    run_obt = BashOperator(
        task_id='run_obt_model',
        bash_command="cd /usr/local/airflow/include/dbt/chocolate_duckdb && dbt run --select obt_chocolate_denormalized",
        pool='STG_pool',
    )

    # 2. Run STG Models in Parallel
    run_stg_sales = BashOperator(
        task_id='run_stg_fact_sales',
        bash_command="cd /usr/local/airflow/include/dbt/chocolate_duckdb && dbt run --select stg_fact_sales",
        pool='STG_pool',
    )
    
    run_stg_customers = BashOperator(
        task_id='run_stg_dim_customers',
        bash_command="cd /usr/local/airflow/include/dbt/chocolate_duckdb && dbt run --select stg_dim_customers",
        pool='STG_pool',
    )

    run_stg_stores = BashOperator(
        task_id='run_stg_dim_stores',
        bash_command="cd /usr/local/airflow/include/dbt/chocolate_duckdb && dbt run --select stg_dim_stores",
        pool='STG_pool',
    )
    
    run_stg_locations = BashOperator(
        task_id='run_stg_dim_locations',
        bash_command="cd /usr/local/airflow/include/dbt/chocolate_duckdb && dbt run --select stg_dim_locations",
        pool='STG_pool',
    )
    
    run_stg_products = BashOperator(
        task_id='run_stg_dim_products',
        bash_command="cd /usr/local/airflow/include/dbt/chocolate_duckdb && dbt run --select stg_dim_products",
        pool='STG_pool',
    )

    # 3. Transfer Task
    transfer_to_postgres = PythonOperator(
        task_id='transfer_stg_to_postgres',
        python_callable=transfer_duckdb_to_postgres,
    )

    # 4. dbt Postgres Tasks
    DBT_PROJECT_DIR = "/usr/local/airflow/include/dbt/chocolate_postgres"
    
    build_calendar_dim = BashOperator(
        task_id="build_calendar_dim",
        bash_command="dbt run-operation setup_dim_calendar",
        cwd=DBT_PROJECT_DIR,
    )
    
    run_dbt_snapshots = BashOperator(
        task_id="run_dbt_customers_snapshots",
        bash_command="dbt snapshot",
        cwd=DBT_PROJECT_DIR,
    )
    
    run_dwh_products = BashOperator(
        task_id="run_dwh_products",
        bash_command="dbt run --select dim_products",
        cwd=DBT_PROJECT_DIR,
    )
    
    run_dwh_locations = BashOperator(
        task_id="run_dwh_locations",
        bash_command="dbt run --select dim_locations",
        cwd=DBT_PROJECT_DIR,
    )
    
    run_dwh_stores = BashOperator(
        task_id="run_dwh_stores",
        bash_command="dbt run --select dim_stores",
        cwd=DBT_PROJECT_DIR,
    )
    
    run_dwh_sales = BashOperator(
        task_id="run_dwh_fact_sales",
        bash_command="dbt run --select fact_sales",
        cwd=DBT_PROJECT_DIR,
    )

    # ==========================================
    # 5. NEW POST-PROCESSING TASKS
    # ==========================================
    cleanup_duckdb = PythonOperator(
        task_id='truncate_duckdb_staging',
        python_callable=truncate_duckdb_staging,
        op_kwargs={
            'duckdb_path': '/usr/local/airflow/include/STG/chocolate_warehouse.duckdb' 
        }
    )

    notify_success = EmailOperator(
        task_id='send_success_email',
        conn_id='smtp_default',
        from_email='gabery686p@gmail.com',
        to='gabery686p@gmail.com',  
        subject='✅ SUCCESS: Chocolate DWH Pipeline Completed',
        html_content="""
            <h3>Pipeline Run Successful</h3>
            <p>The Chocolate Data Warehouse has been successfully updated.</p>
            <ul>
                <li>All staging data processed and transferred to Postgres.</li>
                <li>dbt transformations and fact tables built successfully.</li>
                <li>DuckDB staging database truncated and ready for the next batch.</li>
            </ul>
        """
    )

    audit_success_task = PythonOperator(
        task_id='log_gold_success',
        python_callable=log_gold_audit,
        op_kwargs={
            'batch_run_id': '{{ run_id }}',
            'dag_name': '{{ dag.dag_id }}',
            'status': 'SUCCESS',
            # We grab the total records transferred via XCom!
            'records_transferred': "{{ ti.xcom_pull(task_ids='transfer_stg_to_postgres') }}",
            'error_message': None
        }
    )


    # --- THE DEPENDENCY GRAPH ---
    run_obt >> [
        run_stg_sales, 
        run_stg_customers, 
        run_stg_stores, 
        run_stg_locations, 
        run_stg_products
    ] >> transfer_to_postgres >> [
        build_calendar_dim, 
        run_dbt_snapshots, 
        run_dwh_products, 
        run_dwh_locations, 
        run_dwh_stores
    ] >> run_dwh_sales >> cleanup_duckdb >> [notify_success, audit_success_task]
