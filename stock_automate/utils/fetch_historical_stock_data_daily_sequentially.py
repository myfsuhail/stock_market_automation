from curl_cffi import requests
import pandas as pd
from datetime import datetime
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
            dbname="airflow",
            user="airflow",
            password="airflow",
            host="postgres",
            port=5432
        )
        cursor = conn.cursor()
        # Replace `your_table_name` and `your_column_name` with actual table and column names
        query = """select distinct pair_id from airflow.public.assets_info
                where pair_id not in (
                select distinct pair_id from airflow.public.historical_stock_data_daily
                )"""
        cursor.execute(query)
        # Fetch all rows and extract the first column (stock IDs)
        stock_ids = [row[0] for row in cursor.fetchall()]
        conn.close()
        return stock_ids
    except Exception as e:
        print(f"Error fetching stock IDs: {e}")
        return []


def load_csv_to_postgres(n, file_path, table_name='historical_stock_data_daily'):
    try:
        # Replace with your database credentials
        conn = psycopg2.connect(
            dbname="buy_sell_buddy",
            user="admin",
            password="password",
            host="postgres",
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

        # Iterate through the dataframe and insert rows into the PostgreSQL table
        # Add the `id` and `created_at` columns
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


        # Commit the transaction and close the connection
        conn.commit()
        print("Data loaded successfully.")
        conn.close()
    except Exception as e:
        print(f"Error loading data: {e}")
        sys.exit(1)


def load_into_file(response):
    response_content = ''

    if response.headers.get('Content-Encoding') == 'br':
        response_content = response.text
    else:
        response_content = response.text

    try:
        data = response.json().get('data',[])
    except ValueError as e:
        print(f"Error parsing JSON: {e}")
        return

    if data:
        df = pd.DataFrame(data)
        df.to_csv('historical_data.csv', index=False)
        print("Data saved to historical_data.csv")
    else:
        print("No data available in the response.")


def fetch_historical_data(list, from_date, to_date):

    scraper = cloudscraper.create_scraper()

    for n in list:
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


        # Use requests session
        with requests.Session() as session:

            response = scraper.get(url, headers=headers, params=params)

            if response.status_code == 200:
                print(f"Read Success for Stock Pair Id {n}")
                load_into_file(response)
                load_csv_to_postgres(n, 'historical_data.csv')
                #print("Sleeping for 3 seconds")
                #time.sleep(3)
            else:
                print(f"Failed to fetch data: {response.status_code}, Error: {response.text}")
                sys.exit(1)

if __name__ == '__main__':

    from_date = '1990-01-01'
    to_date = datetime.now().strftime('%Y-%m-%d')  # Get the current date in 'YYYY-MM-DD' format

    stock_list = {'17984': 'ADEL'}
    stock_list = ['17984']

    stock_list = fetch_stock_ids_from_db()
    stock_list = sorted(set(stock_list))

    try:
        datetime.strptime(from_date, "%Y-%m-%d")
        datetime.strptime(to_date, "%Y-%m-%d")
    except ValueError as e:
        print(f"Invalid date format: {e}")
        sys.exit(1)

    
    fetch_historical_data(stock_list, from_date, to_date)