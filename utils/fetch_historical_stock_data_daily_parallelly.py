from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import pandas as pd
from datetime import datetime
import os
import psycopg2
import duckdb
import cloudscraper


def fetch_stock_ids_from_db():
    try:
        conn = psycopg2.connect(
            dbname="buy_sell_buddy",
            user="admin",
            password="password",
            host="postgres_duckdb",
            port=5432
        )
        cursor = conn.cursor()
        query = """
        with halal_stock_cte as materialized (
        	select distinct upper(trim(nse_code)) as nse_code
        	from buy_sell_buddy.public.halal_stocks hs
        	where halal_ind = 'True'
        ),
        asset_info_cte as materialized (
        	select 
        		SUBSTRING(uid FROM ':(.*)$') as uid,
        		pair_id, 
        		SUBSTRING(primary_uid FROM ':(.*)$') as primary_uid,
        		ticker, 
        		stock_name
        	from buy_sell_buddy.public.assets_info ai
        ),
        final_halal_stock_pair_id as (
        select distinct b.pair_id
        from halal_stock_cte a
        inner join asset_info_cte b
        on (a.nse_code = b.uid) or (a.nse_code = b.primary_uid)
        )
        select * from final_halal_stock_pair_id
        where pair_id not in (
        	select distinct pair_id from buy_sell_buddy.public.historical_stock_data_daily
        )       
        """
        cursor.execute(query)
        stock_ids = [row[0] for row in cursor.fetchall()]
        conn.close()
        return stock_ids
    except Exception as e:
        print(f"Error fetching stock IDs: {e}")
        return []


def truncate_table_postgres(table_name):
    try:
        conn = psycopg2.connect(
            dbname="buy_sell_buddy",
            user="admin",
            password="password",
            host="postgres_duckdb",
            port=5432
        )
        cursor = conn.cursor()
        truncate_query = f"TRUNCATE TABLE {table_name}"
        cursor.execute(truncate_query)
        conn.commit()
        print(f"Table {table_name} truncated successfully in PostgreSQL.")
        conn.close()
    except Exception as e:
        print(f"Error truncating table in PostgreSQL: {e}")


def truncate_table_duckdb(duckdb_path, table_name):
    try:
        conn = duckdb.connect(duckdb_path)
        truncate_query = f"DELETE FROM {table_name}"
        conn.execute(truncate_query)
        print(f"Table {table_name} truncated successfully in DuckDB.")
        conn.close()
    except Exception as e:
        print(f"Error truncating table in DuckDB: {e}")


def load_csv_to_postgres(n, file_path, table_name='historical_stock_data_daily'):
    try:
        conn = psycopg2.connect(
            dbname="buy_sell_buddy",
            user="admin",
            password="password",
            host="postgres_duckdb",
            port=5432
        )
        cursor = conn.cursor()
        df = pd.read_csv(file_path)

        df.columns = [
            "direction_color", "row_date", "row_date_raw", "row_date_timestamp",
            "last_close", "last_open", "last_max", "last_min", "volume", "volume_raw",
            "change_percent", "last_close_raw", "last_open_raw", "last_max_raw",
            "last_min_raw", "change_percent_raw"
        ]

        current_time = datetime.now()
        for _, row in df.iterrows():
            insert_query = f"""
            INSERT INTO {table_name} (
                pair_id, direction_color, row_date, row_date_raw, row_date_timestamp, 
                last_close, last_open, last_max, last_min, 
                volume, volume_raw, change_percent, 
                last_close_raw, last_open_raw, last_max_raw, last_min_raw, change_percent_raw, record_created_on
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (
                n, row["direction_color"], row["row_date"], row["row_date_raw"], row["row_date_timestamp"], 
                row["last_close"], row["last_open"], row["last_max"], row["last_min"], 
                row["volume"], row["volume_raw"], row["change_percent"], 
                row["last_close_raw"], row["last_open_raw"], row["last_max_raw"], 
                row["last_min_raw"], row["change_percent_raw"], current_time
            ))

        conn.commit()
        print("Data loaded successfully into PostgreSQL.")
        conn.close()
    except Exception as e:
        print(f"Error loading data into PostgreSQL for Stock Pair ID {n}: {e}")


def load_csv_to_duckdb(n, file_path, duckdb_path='/app/stock_automate/buy_sell_buddy.duckdb', table_name='historical_stock_data_daily'):
    try:
        conn = duckdb.connect(duckdb_path)
        df = pd.read_csv(file_path)

        df.columns = [
            "direction_color", "row_date", "row_date_raw", "row_date_timestamp",
            "last_close", "last_open", "last_max", "last_min", "volume", "volume_raw",
            "change_percent", "last_close_raw", "last_open_raw", "last_max_raw",
            "last_min_raw", "change_percent_raw"
        ]

        # Add pair_id as the first column and record_created_on as the last column
        df.insert(0, 'pair_id', n)
        df['record_created_on'] = datetime.now()

        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df LIMIT 0")
        conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")
        print(f"Data loaded successfully into DuckDB for Stock Pair ID {n}.")
        conn.close()
    except Exception as e:
        print(f"Error loading data into DuckDB for Stock Pair ID {n}: {e}")


def load_into_file(response, n):
    try:
        data = response.json().get('data', [])
        if data:
            df = pd.DataFrame(data)
            file_path = f'historical_data_{n}.csv'
            df.to_csv(file_path, index=False)
            load_csv_to_postgres(n, file_path)
            load_csv_to_duckdb(n, file_path)
            os.remove(file_path)
            success_stock_list.append(n)
            with lock:  # Ensure thread-safe appending
                success_stock_list.append(n)
        else:
            print(f"No data available for Stock Pair ID {n}.")
    except Exception as e:
        print(f"Error processing response for Stock Pair ID {n}: {e}")


def fetch_historical_data(n, from_date, to_date):
    scraper = cloudscraper.create_scraper()

    url = f"https://api.investing.com/api/financialdata/historical/{n}"
    params = {
        "start-date": from_date,
        "end-date": to_date,
        "time-frame": "Daily",
        "add-missing-rows": "false"
    }
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        'Domain-id': 'in',
        'Referer': 'https://www.investing.com/',
        'Origin': 'https://www.investing.com/',
    }

    try:
        response = scraper.get(url, headers=headers, params=params)
        if response.status_code == 200:
            print(f"Read Success for Stock Pair Id {n}")
            load_into_file(response, n)
        else:
            print(f"Failed to fetch data for Stock Pair ID {n}: {response.status_code}")
    except Exception as e:
        print(f"Error fetching data for Stock Pair ID {n}: {e}")


if __name__ == '__main__':
    # Truncate tables at the start
    truncate_table_postgres('historical_stock_data_daily')
    truncate_table_duckdb('/app/stock_automate/buy_sell_buddy.duckdb', 'historical_stock_data_daily')

    from_date = '2015-01-01'
    to_date = datetime.now().strftime('%Y-%m-%d')

    stock_list = fetch_stock_ids_from_db()
    stock_list = sorted(set(stock_list))

    success_stock_list = []
    lock = Lock()

    # Repeat until all stocks are processed
    while True:
        considered_stock_list = list(set(stock_list) - set(success_stock_list))
        considered_stock_list = sorted(set(considered_stock_list))

        if not considered_stock_list:  # If no stocks left to process, exit the loop
            break

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(fetch_historical_data, stock_id, from_date, to_date) for stock_id in considered_stock_list]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error in parallel execution: {e}")
