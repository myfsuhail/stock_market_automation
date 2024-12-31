import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine

def html_to_postgres(html_content, table_name, db_url):
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

    # Create a SQLAlchemy engine
    engine = create_engine(db_url)

    # Load DataFrame into PostgreSQL
    df.to_sql(table_name, engine, if_exists='append', index=False)

    print(f"Data loaded into the '{table_name}' table in PostgreSQL.")

# Example usage
if __name__ == "__main__":
    with open("./source_data/halal_stocks.html", "r", encoding="utf-8") as file:
        html_content = file.read()

    database_url = "postgresql://admin:password@postgres:5432/buy_sell_buddy"
    table_name = "halal_stocks"

    html_to_postgres(html_content, table_name, database_url)
