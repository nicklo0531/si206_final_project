from bs4 import BeautifulSoup
import os
import requests
import sqlite3

def get_date_value(url):
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        # date (th)
        date_ls = []
        ths = soup.find_all('th', class_='pe-5')
        for th in ths:
            th = th.text
            date_ls.append(th)
        # value (td)
        value_ls = []
        tds = soup.find_all('td', class_='pe-5')
        for td in tds:
            td = td.text
            td = td.strip()
            td = float(td)
            value_ls.append(td)
        combined_data = list(zip(date_ls, value_ls))
    return combined_data


def create_database(db_name):
    # set connection
    conn = sqlite3.connect(os.path.abspath(db_name))
    cur = conn.cursor()
    # create table
    cur.execute("""
                CREATE TABLE IF NOT EXISTS WeeklyEconomicIndex (
                id INTEGER PRIMARY KEY,
                date DATE,
                index_value INTEGER
                )
                """)
    conn.commit()
    return cur, conn


def store_data(cur, conn, data):
    cur.execute('SELECT COUNT(id) FROM WeeklyEconomicIndex')
    current_row = cur.fetchone()[0]
    # store data into the table (if current row is less than 100, insert 25 at a time)
    if current_row < 100:
        start = current_row
        end = start + 25
        rows_to_insert = data[start:end]
        for date, index in rows_to_insert:
            cur.execute("INSERT OR IGNORE INTO WeeklyEconomicIndex (date, index_value) VALUES(?, ?)", (date, index))
        print("Successfully inserted " + str(len(rows_to_insert)) + " rows!")
    # store data into the table (if row is larger than 100, insert the rest)
    if current_row >= 100:
        start = 100
        rows_to_insert = data[start:]
        for date, index in rows_to_insert:
            cur.execute("INSERT OR IGNORE INTO WeeklyEconomicIndex (date, index_value) VALUES(?, ?)", (date, index))
    conn.commit()
    conn.close()


def main (): 
    # data
    url = "https://fred.stlouisfed.org/data/WEI"
    data = get_date_value(url)

    # create database
    curr, conn = create_database("Nick/WeeklyEconomicIndexDB_test4.db")

    # insert data
    store_data(curr, conn, data)


if __name__ == '__main__':
    main()