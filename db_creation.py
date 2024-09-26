import sqlite3
import os

# Connect to the SQLite database (will create the file if it doesn't exist)
conn = sqlite3.connect('bitcoin_database.db')  # This creates the database locally
cur = conn.cursor()

# Create the bitcoin_data_aws table if it doesn't exist
cur.execute('''
CREATE TABLE IF NOT EXISTS bitcoin_api_data(
    date TEXT PRIMARY KEY,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume FLOAT
)
''')

# Create the bitcoin_news_aws table if it doesn't exist
cur.execute('''
CREATE TABLE IF NOT EXISTS bitcoin_news(
    date TEXT,
    title TEXT PRIMARY KEY,
    UNIQUE (date, title)
)
''')

# Commit the changes and close the connection
conn.commit()
cur.close()
conn.close()

print("Database and tables created successfully.")

