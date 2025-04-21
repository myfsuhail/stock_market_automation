from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from utils.fetch_current_stock_data_daily_parallelly import (
    fetch_stock_ids_from_db,
    truncate_table_postgres,
    fetch_historical_data,
    load_into_file
)
import pendulum

# Set the timezone to IST
local_tz = pendulum.timezone("Asia/Kolkata")

# DAG definition
def run_stock_data_pipeline():
    # Truncate tables
    truncate_table_postgres('current_stock_data_daily')
    from_date = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
    to_date = datetime.now().strftime('%Y-%m-%d')

    print("Fetching Stock Ids")
    stock_list = fetch_stock_ids_from_db()
    print("Stock IDs are fetched")
    stock_list = sorted(set(stock_list))

    success_stock_list = []
    lock = Lock()

    while True:
        considered_stock_list = list(set(stock_list) - set(success_stock_list))
        considered_stock_list = sorted(set(considered_stock_list))

        if not considered_stock_list:
            break

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(fetch_historical_data, stock_id, from_date, to_date, success_stock_list, lock) for stock_id in considered_stock_list]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error in parallel execution: {e}")

# Create the Airflow DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 9, tzinfo=local_tz),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'delta_stock_data_pipeline',
    default_args=default_args,
    description='A DAG to fetch stock data and store in PostgreSQL and DuckDB',
    schedule_interval='0,30 8-17 * * *',  # Run every minute
    catchup=False,  # Do not backfill missed runs
    max_active_runs=1,  # Only one active run at a time
)

# Task to run the pipeline
run_pipeline_task = PythonOperator(
    task_id='fetch_stock_data_and_load_to_postgres',
    python_callable=run_stock_data_pipeline,
    dag=dag,
)

# DBT command task after fetch tasks
dbt_task = BashOperator(
    task_id='run_dbt_command',
    bash_command='cd /opt/airflow/stock_automate && dbt run -s +sma_strategy+ --exclude ema_vs_sma_compare --log-path /opt/airflow/logs --target-path /opt/airflow/logs',  # Adjust the path to your DBT project
    dag=dag,
)

run_pipeline_task >> dbt_task
