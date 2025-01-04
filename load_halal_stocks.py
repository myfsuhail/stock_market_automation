import pandas as pd
from bs4 import BeautifulSoup
import duckdb

def html_to_duckdb(html_content, table_name, db_file_path):
    # Parse HTML using BeautifulSoup
    soup = BeautifulSoup(html_content, "html.parser")

    # Extract headers
    headers = [header.get_text(strip=True).lower() for header in soup.select("table thead tr th")]

    headers = ['halal_ind', 'stock_name', 'bse_code', 'nse_code', 'industry', 'more']

    # Extract rows data
    rows = []
    for row in soup.select("table tbody tr"):
        cells = row.find_all("td")
        rows.append([
            "True" if cells[0].find("img") and "yes.jpg" in cells[0].find("img")["src"] else "False",
            cells[1].get_text(strip=True),
            cells[2].get_text(strip=True),
            cells[3].get_text(strip=True),
            cells[4].get_text(strip=True),
            "" if cells[5].find("a") else ""
        ])

    # Create DataFrame
    df = pd.DataFrame(rows, columns=headers)

    # Drop the column named 'more' && Add a new column 'record_created_on' with the current timestamp
    df = df.drop(columns=['more'])
    df['record_created_on'] = pd.Timestamp.now()

    # Connect to DuckDB and load DataFrame
    conn = duckdb.connect(db_file_path)

    # Write the DataFrame to DuckDB using DuckDB's built-in function
    conn.execute(f"CREATE OR REPLACE TABLE halal_stocks AS SELECT * FROM df")

    # Close the connection
    conn.close()

    print(f"Data loaded into the '{table_name}' table in DuckDB.")

# Example usage
if __name__ == "__main__":
    with open("./source_data/halal_stocks.html", "r", encoding="utf-8") as file:
        html_content = file.read()

    db_file_path = "/app/stock_automate/buy_sell_buddy.duckdb"  # Path to DuckDB database file
    table_name = "halal_stocks"

    html_to_duckdb(html_content, table_name, db_file_path)
