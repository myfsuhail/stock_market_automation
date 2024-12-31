import json
import pandas as pd
from sqlalchemy import create_engine

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

# Add a new column with the current timestamp
df['record_created_on'] = pd.Timestamp.now()

# Database connection URL
database_url = "postgresql://admin:password@postgres:5432/buy_sell_buddy"

# Create a SQLAlchemy engine
engine = create_engine(database_url)

# Load DataFrame into PostgreSQL
df.to_sql('assets_info', engine, if_exists='append', index=False)

print("Filtered data has been loaded into the 'assets_info' table in PostgreSQL.")
