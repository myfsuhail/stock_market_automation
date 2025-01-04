from concurrent.futures import ThreadPoolExecutor, as_completed
from curl_cffi import requests
import pandas as pd
from datetime import datetime, timedelta
import brotli
import time
import requests
import os
import sys
import cloudscraper
import psycopg2


def fetch_stock_ids_from_db():
    try:
        # Replace with your database credentials
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
        """
        cursor.execute(query)
        stock_ids = [row[0] for row in cursor.fetchall()]
        conn.close()
        return stock_ids
    except Exception as e:
        print(f"Error fetching stock IDs: {e}")
        return []


def load_csv_to_postgres(n, file_path, table_name='current_stock_data_weekly'):
    try:
        conn = psycopg2.connect(
            dbname="buy_sell_buddy",
            user="admin",
            password="password",
            host="postgres_duckdb",
            port=5432
        )
        cursor = conn.cursor()

        # Read the CSV file
        df = pd.read_csv(file_path)

        # Rename columns to match the PostgreSQL table
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
        print("Data loaded successfully.")
        conn.close()
    except Exception as e:
        print(f"Error loading data for Stock Pair ID {n}: {e}")


def load_into_file(response, n):
    try:
        data = response.json().get('data', [])
        if data:
            df = pd.DataFrame(data)
            file_path = f'historical_data_{n}.csv'
            df.to_csv(file_path, index=False)
            load_csv_to_postgres(n, file_path)
            os.remove(file_path)  # Cleanup the temporary CSV file
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
        "time-frame": "Weekly",
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


def truncate_table():
    try:
        conn = psycopg2.connect(
            dbname="buy_sell_buddy",
            user="admin",
            password="password",
            host="postgres_duckdb",
            port=5432
        )
        cursor = conn.cursor()
        truncate_query = "truncate table current_stock_data_weekly"
        cursor.execute(truncate_query)
        conn.commit()
        print("Table current_stock_data_weekly truncated successfully.")
        conn.close()
    except Exception as e:
        print(f"Error truncating table: {e}")


if __name__ == '__main__':
    # Truncate table at the start of the script
    truncate_table()

    from_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    to_date = datetime.now().strftime('%Y-%m-%d')

    stock_list = fetch_stock_ids_from_db()
    stock_list = sorted(set(stock_list))

    # Use ThreadPoolExecutor to process stocks in parallel
    with ThreadPoolExecutor(max_workers=5) as executor:  # Adjust max_workers based on your system capacity
        futures = [executor.submit(fetch_historical_data, stock_id, from_date, to_date) for stock_id in stock_list]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error in parallel execution: {e}")
