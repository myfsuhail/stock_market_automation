import pandas as pd
from bs4 import BeautifulSoup

def html_to_csv(html_content, csv_filename):
    # Parse HTML using BeautifulSoup
    soup = BeautifulSoup(html_content, "html.parser")

    # Extract headers
    headers = [header.get_text(strip=True) for header in soup.select("table thead tr th")]

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

    # Create DataFrame and save to CSV
    df = pd.DataFrame(rows, columns=headers)
    
    df.to_csv(csv_filename, index=False)
    
    print(f"Data saved to {csv_filename}")


with open("halal_stocks.html", "r", encoding="utf-8") as file:
    html_content = file.read()

html_to_csv(html_content, "halal_stocks_info.csv")
