import requests
from bs4 import BeautifulSoup
import sqlite3
from dotenv import load_dotenv
import os
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

def fetch_bitcoin_articles():
    # List to store dictionaries with date and article title
    articles_data = []

    # Updated URL for articles from the 25th of September 2024
    base_url = "https://www.ft.com/search?expandRefinements=true&q=bitcoin&dateFrom=2024-01-01&dateTo=2024-09-25"

     # Start with the first page
    page_number = 1
    total_pages = None

    while True:
        # Construct the URL for the current page
        url = f"{base_url}&page={page_number}"
        
        # Fetch the page content
        response = requests.get(url)
        
        # Check if the page exists
        if response.status_code != 200:
            break
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Find all article tags on the current page
        articles = soup.find_all("a", class_="js-teaser-heading-link")
        
        # Find all date tags on the current page
        dates = soup.find_all("time", class_="o-teaser__timestamp-date")

        # Add each bitcoin article and its date to the list
        for article, date in zip(articles, dates):
            if "bitcoin" in article.text.lower():  # Use lower() to handle case sensitivity
                articles_data.append({
                    "date": date["datetime"],
                    "title": article.text.strip()
                })
        
        # Extract the total number of pages if not already known
        if total_pages is None:
            pagination_info = soup.find("span", class_="search-pagination__page")
            if pagination_info:
                total_pages = int(pagination_info.text.split()[-1])

        # Break the loop if the current page is the last one
        if total_pages and page_number >= total_pages:
            break
        
        # Move to the next page
        page_number += 1

    return articles_data

def insert_articles_into_db(articles_data):
    # Connect to the SQLite database
    conn = sqlite3.connect('bitcoin_database.db')
    cur = conn.cursor()

    # Insert the data from the list into the table
    for article in articles_data:
        cur.execute('''
        INSERT OR REPLACE INTO bitcoin_news (date, title)
        VALUES (?, ?)
        ''', (article['date'], article['title']))

    # Commit the transaction and close the connection
    conn.commit()
    cur.close()
    conn.close()

    print("Articles inserted into the database successfully.")

# Fetch and insert the articles
if __name__ == "__main__":
    bitcoin_articles = fetch_bitcoin_articles()
    insert_articles_into_db(bitcoin_articles)