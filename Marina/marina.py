import requests
import sqlite3
import time 

# Define API and database details
API_URL = "https://api.polygon.io/v2/reference/news"
API_KEY = "p0i66s1388GS9jpK9pCrib2s6Gdm7ZYU"
DB_NAME = "PolygonSentimentDatabase.db"
STOCK_SYMBOLS = ["NVDA", "AAPL", "AMZN", "MSFT", "META"]

def get_stock_id(ticker):
    """Map ticker to stock_id"""
    stock_mapping = {
        "NVDA": 1,
        "AAPL": 2,
        "AMZN": 3,
        "MSFT": 4,
        "META": 5
    }
    return stock_mapping.get(ticker, 0)

def fetch_data_from_api(ticker, limit=25):
    """Fetch data from the Polygon API for a specific ticker"""
    url = f"{API_URL}?ticker={ticker}&limit={limit}&apiKey={API_KEY}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def create_database(db_name):
    """Create database and tables if they don't exist"""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Stocks (
            stock_id INTEGER PRIMARY KEY,
            ticker TEXT UNIQUE
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS SentimentData (
            stock_id INTEGER,
            date TEXT,
            sentiment_score INTEGER,
            title TEXT,
            description TEXT,
            article_url TEXT,
            PRIMARY KEY (stock_id, article_url),
            FOREIGN KEY (stock_id) REFERENCES Stocks(stock_id)
        )
    ''')

    # Insert stock tickers into the Stocks table
    for ticker in STOCK_SYMBOLS:
        cursor.execute('INSERT OR IGNORE INTO Stocks (ticker) VALUES (?)', (ticker,))
    conn.commit()
    return conn, cursor

def sentiment_mapping(sentiment):
    """Map sentiment strings to numeric scores"""
    mapping = {"positive": 1, "neutral": 0, "negative": -1}
    return mapping.get(sentiment.lower(), 0)

def insert_25_new_rows(cursor, conn, stock_id, articles):
    """Insert exactly 25 rows for a specific stock"""
    cursor.execute('SELECT COUNT(*) FROM SentimentData WHERE stock_id = ?', (stock_id,))
    current_count = cursor.fetchone()[0]

    if current_count >= 25:
        print(f"Already have 25 rows for stock_id {stock_id}. Skipping.")
        return False

    rows_to_insert = []
    for article in articles:
        if len(rows_to_insert) == 25:
            break

        date = article.get("published_utc", "").split("T")[0]
        title = article.get("title", "")
        description = article.get("description", "")
        article_url = article.get("article_url", "")
        insights = article.get("insights", [])

        if insights:
            sentiment = insights[0].get("sentiment", "neutral")
            sentiment_score = sentiment_mapping(sentiment)
            row = (stock_id, date, sentiment_score, title, description, article_url)
            rows_to_insert.append(row)

    if rows_to_insert:
        #print(rows_to_insert)
        cursor.executemany('''
            INSERT OR IGNORE INTO SentimentData (
                stock_id, date, sentiment_score, title, description, article_url
            ) VALUES (?, ?, ?, ?, ?, ?)
        ''', rows_to_insert)
        conn.commit()
        print(f"Inserted {len(rows_to_insert)} new rows for stock_id {stock_id}")
        return True

    print(f"No new rows to insert for stock_id {stock_id}")
    return False

def insert_remaining_rows(cursor, conn, stock_id, articles):
    """
    Inserts all remaining rows for a given stock into the database.
    """
    rows_to_insert = []

    for article in articles:
        date = article.get("published_utc", "").split("T")[0]
        title = article.get("title", "")
        description = article.get("description", "")
        article_url = article.get("article_url", "")
        insights = article.get("insights", [])
        
        if insights:
            sentiment = insights[0].get("sentiment", "neutral")
            sentiment_score = sentiment_mapping(sentiment)
            row = (stock_id, date, sentiment_score, title, description, article_url)
            rows_to_insert.append(row)

    if rows_to_insert:
        cursor.executemany('''
            INSERT OR IGNORE INTO SentimentData (
                stock_id, date, sentiment_score, title, description, article_url
            ) VALUES (?, ?, ?, ?, ?, ?)
        ''', rows_to_insert)
        conn.commit()
        print(f"Inserted {len(rows_to_insert)} rows for stock_id {stock_id}")

def main():
    # Create database connection
    conn, cursor = create_database(DB_NAME)

    # Determine which stock to process based on row counts
    for stock_ticker in STOCK_SYMBOLS:
        stock_id = get_stock_id(stock_ticker)

        # Check how many rows are already inserted for this stock
        cursor.execute('SELECT COUNT(*) FROM SentimentData WHERE stock_id = ?', (stock_id,))
        current_count = cursor.fetchone()[0]

        # If less than 25 rows, fetch and insert new data
        if current_count < 25:
            print(f"Fetching data for {stock_ticker} (stock_id {stock_id})...")
            try:
                data = fetch_data_from_api(stock_ticker, limit=25)
                articles = data.get("results", [])
                if insert_25_new_rows(cursor, conn, stock_id, articles):
                    break  # Stop after processing one stock
            except Exception as e:
                print(f"Error fetching or inserting data for {stock_ticker}: {e}")
                continue


    # After ensuring 25 rows per stock, insert all remaining data
    cursor.execute('SELECT COUNT(*) FROM SentimentData')
    total_rows = cursor.fetchone()[0]
    if total_rows >= 125:
        print("125 rows (25 per stock) achieved. Inserting historical data...")
        time.sleep(60)
        for stock_ticker in STOCK_SYMBOLS:
            stock_id = get_stock_id(stock_ticker)
            try:
                data = fetch_data_from_api(stock_ticker, limit=1000)
                articles = data.get("results", [])
                insert_remaining_rows(cursor, conn, stock_id, articles)
            except Exception as e:
                print(f"Error fetching historical data for {stock_ticker}: {e}")

    conn.close()

if __name__ == "__main__":
    main()