import json

# Load the data from data.json
with open('./source_data/investing_dotcom_stock_info.json', 'r') as file:
    data = json.load(file)

# Extract only the "asset" details from each entry in "rows"
assets = [entry["asset"] for entry in data["rows"] if "asset" in entry]

# Prepare the new data structure
filtered_data = {"assets": assets}

# Write the filtered data to finalData.json
with open('final_data.json', 'w') as file:
    json.dump(filtered_data, file, indent=4)

print("Filtered data has been saved to final_data.json.")
