from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from utils.fetch_historical_stock_data_daily_parallelly import (
    fetch_stock_ids_from_db,
    truncate_table_postgres,
    fetch_historical_data,
    load_into_file
)

# DAG definition
def run_stock_data_pipeline():
    # Truncate tables
    truncate_table_postgres('historical_stock_data_daily')

    from_date = '2015-01-01'
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

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(fetch_historical_data, stock_id, from_date, to_date, success_stock_list, lock) for stock_id in considered_stock_list]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error in parallel execution: {e}")

# Create the Airflow DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'historical_stock_data_pipeline',
    default_args=default_args,
    description='A DAG to fetch stock data and store in PostgreSQL and DuckDB',
    schedule_interval=None,  # Run every minute
    catchup=False,  # Do not backfill missed runs
    max_active_runs=1,  # Only one active run at a time
)

# Task to run the pipeline
run_pipeline_task = PythonOperator(
    task_id='fetch_stock_data_and_load_to_postgres',
    python_callable=run_stock_data_pipeline,
    dag=dag,
)

run_pipeline_task
