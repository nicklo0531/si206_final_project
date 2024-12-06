# import requests
# import sqlite3
# import json

# def fetch_data_from_api(url):
#     response = requests.get(url)
#     response.raise_for_status()
#     return response.json()

# def create_database(db_name):
#     conn = sqlite3.connect(db_name)
#     cursor = conn.cursor()
#     cursor.execute(''' 
#         CREATE TABLE IF NOT EXISTS WeeklyPrices ( 
#             stock_id INTEGER, 
#             date DATE, 
#             open REAL, 
#             high REAL, 
#             low REAL, 
#             close REAL, 
#             volume INTEGER, 
#             PRIMARY KEY (stock_id, date) 
#         ) 
#     ''')
#     cursor.execute('''CREATE TABLE IF NOT EXISTS Stocks ( 
#             id INTEGER PRIMARY KEY AUTOINCREMENT, 
#             ticker varchar(5)
#     )''')
#     tickers = ['NVDA', 'AAPL', 'AMZN', 'MSFT', 'META']
#     for ticker in tickers:
#         cursor.execute('''INSERT or IGNORE INTO Stocks (ticker) VALUES (?)''', (ticker,))
#     conn.commit()
#     return conn, cursor

# def insert_25_rows(cursor, conn, stock_id, data, batch_size=25):
#     cursor.execute('SELECT COUNT(stock_id) FROM WeeklyPrices WHERE stock_id = ?', (stock_id,))
#     result = cursor.fetchone()
#     rows_count = result[0] if result else 0
#     cursor.execute('SELECT COUNT(stock_id) FROM WeeklyPrices')
#     result2 = cursor.fetchone()
#     rows = result2[0]
    
#     if rows == 125:
        
#     if rows_count < 25:
#         start = rows_count  # Start inserting at the next available row
#         rows_to_insert = []
        
#         # Add up to batch_size rows
#         for date, values in list(data.items())[start:start + batch_size]:
#             row = (
#                 stock_id,
#                 date,
#                 float(values["1. open"]),
#                 float(values["2. high"]),
#                 float(values["3. low"]),
#                 float(values["4. close"]),
#                 int(values["5. volume"])
#             )
#             rows_to_insert.append(row)

#         # Insert the rows into the database
#         if rows_to_insert:
#             cursor.executemany('''
#                 INSERT OR IGNORE INTO WeeklyPrices (stock_id, date, open, high, low, close, volume)
#                 VALUES (?, ?, ?, ?, ?, ?, ?)
#             ''', rows_to_insert)
#             conn.commit()
#             print(f"Inserted {len(rows_to_insert)} rows for stock_id {stock_id}")
#         else:
#             print(f"No rows to insert for stock_id {stock_id}")
#         return True  # Rows inserted for this stock_id
#     return False  # No rows inserted because stock already has 25 rows

# def main():
#     # Define the API URLs for each stock
#     stock_symbols = {
#         "NVDA": 1,
#         "AAPL": 2,
#         "AMZN": 3,
#         "MSFT": 4,
#         "META": 5
#     }
#     db_name = 'StockDatabase.db'  # Use a single database for all stocks

#     # Create the database
#     conn, cursor = create_database(db_name)

#     # Loop through each stock symbol and insert data
#     for symbol, stock_id in stock_symbols.items():
#         api_url = f"https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol={symbol}&apikey=YOUR_API_KEY_HERE"

#         try:
#             # Fetch the data for each stock
#             data = fetch_data_from_api(api_url)
#             weekly_data = data.get("Weekly Time Series", {})

#             if not weekly_data:
#                 print(f"No weekly data found for {symbol}")
#             else:
#                 # Try to insert 25 rows for the first stock with fewer than 25 rows
#                 if insert_25_rows(cursor, conn, stock_id, weekly_data):
#                     break  # Exit after inserting 25 rows

#         except Exception as e:
#             print(f"Error fetching data for {symbol}: {e}")

#     # Close the database connection
#     conn.close()
#     print(f"Data inserted into {db_name} successfully.")

# if __name__ == '__main__':
#     main()

import requests
import sqlite3
import json

def fetch_data_from_api(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def create_database(db_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Create WeeklyPrices table
    cursor.execute(''' 
        CREATE TABLE IF NOT EXISTS WeeklyPrices ( 
            stock_id INTEGER, 
            date DATE, 
            open REAL, 
            high REAL, 
            low REAL, 
            close REAL, 
            volume INTEGER, 
            PRIMARY KEY (stock_id, date) 
        ) 
    ''')

    # Create Stocks table
    cursor.execute('''CREATE TABLE IF NOT EXISTS Stocks ( 
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            ticker varchar(5) UNIQUE
    )''')

    # Insert tickers into the Stocks table (if they don't exist already)
    tickers = ['NVDA', 'AAPL', 'AMZN', 'MSFT', 'META']
    for ticker in tickers:
        cursor.execute('''INSERT OR IGNORE INTO Stocks (ticker) VALUES (?)''', (ticker,))
    conn.commit()
    return conn, cursor

def insert_25_rows(cursor, conn, stock_id, data, batch_size=25):
    cursor.execute('SELECT COUNT(stock_id) FROM WeeklyPrices WHERE stock_id = ?', (stock_id,))
    result = cursor.fetchone()
    rows_count = result[0] if result else 0
    
    # If there are less than 25 rows for this stock, insert up to 25 rows
    if rows_count < 25:
        start = rows_count  # Start inserting at the next available row
        rows_to_insert = []
        
        # Add up to batch_size rows
        for date, values in list(data.items())[start:start + batch_size]:
            row = (
                stock_id,
                date,
                float(values["1. open"]),
                float(values["2. high"]),
                float(values["3. low"]),
                float(values["4. close"]),
                int(values["5. volume"])
            )
            rows_to_insert.append(row)

        # Insert the rows into the database
        if rows_to_insert:
            cursor.executemany('''
                INSERT OR IGNORE INTO WeeklyPrices (stock_id, date, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', rows_to_insert)
            conn.commit()
            print(f"Inserted {len(rows_to_insert)} rows for stock_id {stock_id}")
        else:
            print(f"No rows to insert for stock_id {stock_id}")
        return True  # Rows inserted for this stock_id
    return False  # No rows inserted because stock already has 25 rows

def insert_remaining_rows(cursor, conn, stock_id, data):
    cursor.execute('SELECT COUNT(stock_id) FROM WeeklyPrices WHERE stock_id = ?', (stock_id,))
    result = cursor.fetchone()
    rows_count = result[0] if result else 0
    
    # Skip the first 25 rows if they already exist
    if rows_count >= 25:
        start = rows_count  # Start inserting at the next available row
        rows_to_insert = []
        
        # Insert all remaining rows for this stock
        for date, values in list(data.items())[start:]:
            row = (
                stock_id,
                date,
                float(values["1. open"]),
                float(values["2. high"]),
                float(values["3. low"]),
                float(values["4. close"]),
                int(values["5. volume"])
            )
            rows_to_insert.append(row)

        # Insert the rows into the database
        if rows_to_insert:
            cursor.executemany('''
                INSERT OR IGNORE INTO WeeklyPrices (stock_id, date, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', rows_to_insert)
            conn.commit()
            print(f"Inserted {len(rows_to_insert)} rows for stock_id {stock_id}")
        else:
            print(f"No rows to insert for stock_id {stock_id}")
    
def check_if_125_rows(cursor):
    cursor.execute('SELECT COUNT(*) FROM WeeklyPrices')
    total_rows = cursor.fetchone()[0]
    return total_rows == 125

def main():
    # Define the API URLs for each stock
    stock_symbols = {
        "NVDA": 1,
        "AAPL": 2,
        "AMZN": 3,
        "MSFT": 4,
        "META": 5
    }
    
    db_name = 'StockDatabase.db'  # Use a single database for all stocks

    # Create the database and tables
    conn, cursor = create_database(db_name)

    # First, insert the first 125 rows (25 rows per stock)
    for symbol, stock_id in stock_symbols.items():
        api_url = f"https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol={symbol}&apikey=LYPQFWUG9S4RUFSP"

        try:
            # Fetch the data for each stock
            data = fetch_data_from_api(api_url)
            weekly_data = data.get("Weekly Time Series", {})

            if not weekly_data:
                print(f"No weekly data found for {symbol}")
            else:
                # Insert the first 25 rows of data for each stock
                if insert_25_rows(cursor, conn, stock_id, weekly_data):
                    break

        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
    
    # Now check if there are exactly 125 rows in the database
    if check_if_125_rows(cursor):
        print("There are exactly 125 rows in the database. Proceeding to insert remaining data.")
        
        # Insert all remaining rows for each stock
        for symbol, stock_id in stock_symbols.items():
            api_url = f"https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol={symbol}&apikey=LYPQFWUG9S4RUFSP"

            try:
                # Fetch the data for each stock
                data = fetch_data_from_api(api_url)
                weekly_data = data.get("Weekly Time Series", {})

                if not weekly_data:
                    print(f"No weekly data found for {symbol}")
                else:
                    # Insert all remaining rows for each stock
                    insert_remaining_rows(cursor, conn, stock_id, weekly_data)

            except Exception as e:
                print(f"Error fetching data for {symbol}: {e}")
    else:
        print("There are not exactly 125 rows in the database. Exiting.")

    # Close the database connection
    conn.close()
    print(f"Data inserted into {db_name} successfully.")

if __name__ == '__main__':
    main()