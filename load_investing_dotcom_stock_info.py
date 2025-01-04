import json
import pandas as pd
import duckdb

# Load the data from data.json
with open('./source_data/investing_dotcom_stock_info.json', 'r') as file:
    data = json.load(file)

# Extract only the "asset" details from each entry in "rows"
assets = [row["asset"] for row in data["rows"]]

# Define the specific columns you want to keep
columns_to_keep = ["uid", "pairID", "ticker", "name", "primary", "path", "sector"]

# Convert the list of assets to a DataFrame, selecting only the desired columns
df = pd.DataFrame(assets)[columns_to_keep]

# Define new headers for renaming
rename_headers = {
    "uid": "uid",
    "pairID": "pair_id",
    "ticker": "ticker",
    "name": "stock_name",
    "primary": "primary_uid",
    "path": "path",
    "sector": "sector"
}

# Rename the columns
df = df.rename(columns=rename_headers)

# Add main sector from "data" key (2nd element in the list)
main_sector = []
for row in data["rows"]:
    # Safely access the 2th element in the data list
    m_sector = row["data"][1]["raw"] if len(row["data"]) > 1 and "raw" in row["data"][1] else None
    main_sector.append(m_sector)

# Add secondary sector from "data" key (2nd element in the list)
sub_sector = []
for row in data["rows"]:
    # Safely access the 2th element in the data list
    s_sector = row["data"][2]["raw"] if len(row["data"]) > 2 and "raw" in row["data"][2] else None
    sub_sector.append(s_sector)

# Add market cap from "data" key (4th element in the list)
market_caps = []
for row in data["rows"]:
    # Safely access the 4th element in the data list
    market_cap = row["data"][3]["raw"] if len(row["data"]) > 3 and "raw" in row["data"][3] else None
    market_caps.append(market_cap)

# Add fields to the DataFrame
df["market_cap"] = market_caps
df["primary_sector"] = main_sector
df["secondary_sector"] = sub_sector

# Add a new column with the current timestamp
df['record_created_on'] = pd.Timestamp.now()

# Path to the DuckDB database file
db_file_path = "/app/stock_automate/buy_sell_buddy.duckdb"

# Connect to DuckDB
conn = duckdb.connect(db_file_path)

# Write the DataFrame to DuckDB using DuckDB's built-in function
conn.execute(f"CREATE OR REPLACE TABLE assets_info AS SELECT * FROM df")

# Close the connection
conn.close()

print("Filtered data has been loaded into the 'assets_info' table in DuckDB.")
