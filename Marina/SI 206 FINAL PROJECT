import requests
import sqlite3
from datetime import datetime

# Database file name
DB_NAME = "stock_data.db"

# API base URL and token
API_TOKEN = "MJ61gfmue5eWaca8G1C1GhnBd0YDLFcn1soRB30H"
API_URL = "https://api.stockdata.org/v1/news/all"

# Stock symbols to fetch sentiment for
STOCK_SYMBOLS = ["AAPL", "NVDA", "MSFT", "AMZN", "META"]

def fetch_stock_sentiment(api_token, symbols, limit=25):
    sentiments = []
    
    # Calculate per-symbol limit to distribute across symbols
    per_symbol_limit = max(1, limit // len(symbols))
    
    for symbol in symbols:
        params = {
            "api_token": api_token,
            "symbols": symbol,
            "filter_entities": "true",
            "language": "en",
            "limit": per_symbol_limit
        }
        try:
            response = requests.get(API_URL, params=params)
            response.raise_for_status()
            data = response.json()
            
            symbol_sentiments = []
            for article in data.get("data", []):
                for entity in article.get("entities", []):
                    if entity.get("symbol") == symbol and entity.get("sentiment_score") is not None:
                        symbol_sentiments.append((symbol, entity["sentiment_score"]))
                        if len(symbol_sentiments) >= per_symbol_limit:
                            break
                if len(symbol_sentiments) >= per_symbol_limit:
                    break
            
            sentiments.extend(symbol_sentiments)

        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
    
    return sentiments[:limit]

def setup_database():
    """
    Create the SQLite database and tables if they don't exist.
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Stocks (
        stock_id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT UNIQUE
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Sentiments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        stock_id INTEGER,
        sentiment_score REAL,
        retrieved_at TEXT,
        UNIQUE(stock_id, sentiment_score, retrieved_at),
        FOREIGN KEY (stock_id) REFERENCES Stocks (stock_id)
    )
    """)
    
    conn.commit()
    conn.close()

def store_data(sentiments):
    """
    Store stock sentiment data in the SQLite database.

    Args:
        sentiments (list): A list of tuples containing stock symbol and sentiment score.

    Returns:
        int: Number of new entries inserted
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    inserted_count = 0
    retrieval_time = datetime.now().isoformat()
    
    for symbol, sentiment in sentiments:
        # Insert or ignore stock ticker
        cursor.execute("INSERT OR IGNORE INTO Stocks (ticker) VALUES (?)", (symbol,))
        
        # Get stock_id for the symbol
        cursor.execute("SELECT stock_id FROM Stocks WHERE ticker = ?", (symbol,))
        stock_id = cursor.fetchone()[0]
        
        # Insert sentiment with unique constraints
        try:
            cursor.execute("""
            INSERT OR IGNORE INTO Sentiments (stock_id, sentiment_score, retrieved_at)
            VALUES (?, ?, ?)
            """, (stock_id, sentiment, retrieval_time))
            
            if cursor.rowcount > 0:  # Only count if a new row was added
                inserted_count += 1
        except sqlite3.IntegrityError:
            # This helps prevent duplicate entries
            pass
    
    conn.commit()
    conn.close()
    print(f"Inserted {inserted_count} new entries")
    return inserted_count

def display_data():
    """
    Display data from the database.
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM Sentiments")
    total_entries = cursor.fetchone()[0]
    print(f"\nTotal entries in database: {total_entries}")
    
    cursor.execute("""
    SELECT Stocks.ticker, Sentiments.sentiment_score, Sentiments.retrieved_at
    FROM Sentiments
    JOIN Stocks ON Sentiments.stock_id = Stocks.stock_id
    ORDER BY Sentiments.retrieved_at DESC
    LIMIT 25
    """)
    data = cursor.fetchall()
    print("\n=== Most Recent Sentiment Data ===")
    for row in data:
        print(f"Stock: {row[0]}, Sentiment Score: {row[1]:.4f}, Retrieved At: {row[2]}")
    
    cursor.execute("""
    SELECT ticker, COUNT(*) as entry_count
    FROM Stocks
    JOIN Sentiments ON Stocks.stock_id = Sentiments.stock_id
    GROUP BY ticker
    ORDER BY entry_count DESC
    """)
    stock_coverage = cursor.fetchall()
    print("\n=== Stock Coverage ===")
    for stock, count in stock_coverage:
        print(f"{stock}: {count} entries")
    
    conn.close()

if __name__ == "__main__":
    print("Fetching sentiment data...")
    
    setup_database()
    all_sentiments = fetch_stock_sentiment(API_TOKEN, STOCK_SYMBOLS, limit=25)

    print(f"Fetched a total of {len(all_sentiments)} sentiment entries")
    
    if all_sentiments:
        print("Storing data to the database...")
        store_data(all_sentiments)
        display_data()
    else:
        print("No data fetched.")