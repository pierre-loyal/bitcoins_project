import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import sqlite3

# Load environment variables from .env file
load_dotenv()

# Get the API key from environment variable
api_key = os.getenv('ALPHAVANTAGE_API_KEY')
if api_key is None:
    raise ValueError("No API key found. Please set ALPHAVANTAGE_API_KEY in your .env file.")

# Fetch bitcoin data function
def fetch_bitcoin_data():
    symbol = 'BTC'
    market = 'USD'
    function = 'DIGITAL_CURRENCY_DAILY'  # For daily data

    # Construct the URL for the API request
    url = f'https://www.alphavantage.co/query?function={function}&symbol={symbol}&market={market}&apikey={api_key}'

    # Make the API request and get the JSON response
    response = requests.get(url).json()

    # Check if the response contains the expected data
    if 'Time Series (Digital Currency Daily)' not in response:
        raise ValueError("Unexpected response format: 'Time Series (Digital Currency Daily)' key not found.")

    # Extract the 'Time Series (Digital Currency Daily)' data
    time_series_daily = response.get('Time Series (Digital Currency Daily)', {})

    # Convert the time series data to a DataFrame
    df = pd.DataFrame.from_dict(time_series_daily, orient='index')

    # Print the DataFrame to inspect its structure
    print(df.head())  # This will show the first few rows and columns of the DataFrame

    # Rename the columns
    df.columns = ['open', 'high', 'low', 'close', 'volume']

    # Convert the index to datetime
    df.index = pd.to_datetime(df.index)

    # Convert relevant columns to float for storing in the database
    df = df.astype(float)

    # Filter the data for the past 3 months
    three_months_ago = datetime.now() - timedelta(days=90)
    df = df[df.index >= three_months_ago]

    return df

# Function to insert the data into the SQLite database
def insert_bitcoin_data_into_db(df):
    # Connect to the SQLite database
    conn = sqlite3.connect('bitcoin_database.db')
    cur = conn.cursor()

    # Insert the data from the DataFrame into the table
    for index, row in df.iterrows():
        cur.execute('''
        INSERT OR REPLACE INTO bitcoin_api_data (date, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (index.strftime('%Y-%m-%d'), row['open'], row['high'], row['low'], row['close'], row['volume']))

    # Commit the transaction and close the connection
    conn.commit()
    cur.close()
    conn.close()

    print("Data inserted into the database successfully.")

# Fetch and insert the data
if __name__ == "__main__":
    bitcoin_data = fetch_bitcoin_data()
    insert_bitcoin_data_into_db(bitcoin_data)

