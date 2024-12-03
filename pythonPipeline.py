import json
import pandas as pd
import numpy as np
import re
from bs4 import BeautifulSoup
from word2number import w2n
from pymongo import MongoClient

with open("../pythonProject/sample_data.json") as f:
    data = json.load(f)

    print(data)

df = pd.DataFrame(data)

# Step 1: Data Exploration
print("Columns:", df.columns)
print("Missing Values:\n", df.isnull().sum())
print("Sample Data:\n", df.head())

# Step 2: Data Cleaning

# Function to clean and standardize price
def clean_price(price):
    if isinstance(price, str):
        # First, handle price in text format (e.g., "one hundred fifty")
        try:
            # Try to convert written numbers to numeric value
            price = w2n.word_to_num(price.lower())
            return float(price)
        except ValueError:
            # If the conversion fails, proceed with cleaning numeric values
            price = price.replace("$", "").replace(",", "").strip()
            if price.isdigit():
                return float(price)
            else:
                try:
                    return float(re.sub(r"[^\d.]", "", price))  # Remove non-numeric characters
                except:
                    return np.nan
    return price

# Clean Price
df['Price'] = df['Price'].apply(clean_price)


# Function to clean Date format
# Function to clean and normalize DateAdded
def parse_date(date):
    try:
        # Handle timestamp format
        if isinstance(date, (int, float)):
            return pd.to_datetime(date, unit='ms')  # Convert timestamp in milliseconds

        # Try to parse common date string formats
        formats = ["%Y/%m/%d", "%d-%m-%Y", "%m/%d/%y", "%B %d, %Y", "%Y-%d-%m"]
        for fmt in formats:
            try:
                return pd.to_datetime(date, format=fmt)
            except ValueError:
                continue

        # If none of the formats work, attempt a flexible parsing
        return pd.to_datetime(date, errors='coerce')
    except Exception as e:
        print(f"Error parsing date: {date}, Error: {e}")
        return pd.NaT  # Return Not-a-Time for invalid dates


# Apply the parsing function to the DateAdded column
df['DateAdded'] = df['DateAdded'].apply(parse_date)

# Fill missing dates with a placeholder or an inferred value (optional)
df['DateAdded'] = df['DateAdded'].fillna(pd.Timestamp("1970-01-01"))  # Example placeholder

# Fill missing Quantity values (using 0 for missing quantities)
df['Quantity'] = df['Quantity'].replace("", np.nan)
df['Quantity'] = df['Quantity'].fillna(0).astype(int)

# Function to remove HTML tags from 'data' field using BeautifulSoup
def remove_html_tags(text):
    if isinstance(text, str):
        return BeautifulSoup(text, "html.parser").get_text()
    return text

# Apply cleaning function to 'data' column to remove HTML tags
df['data'] = df['data'].apply(remove_html_tags)

# Step 3: Handling Typos in Brand
brand_corrections = {"Dysen": "Dyson"}  # Correcting typo in 'Dysen' to 'Dyson'
df['Brand'] = df['Brand'].replace(brand_corrections)

# Step 4: Extract Attributes (if needed)
# Extracting attributes from the 'Attributes' column and expanding them into separate columns
attributes_df = df['Attributes'].apply(pd.Series)
df = pd.concat([df, attributes_df], axis=1)

# Step 5: Standardize and Clean Description
def clean_description(description):
    if isinstance(description, str):
        return description.strip()  # Remove leading/trailing whitespaces
    return description

df['Description'] = df['Description'].apply(clean_description)

# Step 6: Feature Engineering
# Add a column for the length of the product description
df['DescriptionLength'] = df['Description'].apply(lambda x: len(x.split()) if pd.notnull(x) else 0)

# Check the cleaned data
print("Cleaned and Transformed Data:\n", df.head())



#df.to_json("cleaned_product_data.json", orient="records", lines=False)
parsed_json = df.to_json(orient="records")  # This returns a JSON string
parsed_json = json.loads(parsed_json)

try:
    client = MongoClient("mongodb://localhost:27017/")  # Update with your MongoDB URI if needed
    db = client["product_database"]  # Create or use a database
    collection = db["cleaned_products"]  # Create or use a collection

    # Iterate through the cleaned data for upsert
    for record in parsed_json:
        # Define a unique key (e.g., a ProductID or Name) to identify duplicates
        filter_condition = {"_id": record.get("ProductID", None)}  # Use ProductID as the unique identifier
        if "ProductID" in record:
            record["_id"] = record.pop("ProductID")  # Ensure ProductID becomes the MongoDB `_id` field

        # Perform upsert: Insert if not present, update if present
        collection.update_one(filter_condition, {"$set": record}, upsert=True)

    print("Data successfully upserted to MongoDB.")
except Exception as e:
    print("An error occurred:", e)
finally:
    client.close()
