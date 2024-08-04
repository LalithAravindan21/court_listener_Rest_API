import requests
import sqlite3
import time
import os
from datetime import datetime
import dateutil.parser

def fetch_cases(year, page=1):
    API_URL = "https://www.courtlistener.com/api/rest/v3/opinions/"
    HEADERS = {
        'Authorization': '5b11c153818995e2b311b10b087f6d02774fe680'  
    }
    params = {
        'page': page,
        'date_filed_min': f"{year}-01-01",
        'date_filed_max': f"{year}-12-31"
    }
    response = requests.get(API_URL, headers=HEADERS, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()

def create_table(conn):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS cases (
        id INTEGER PRIMARY KEY,
        date_created TEXT,
        date_modified TEXT,
        opinion_text TEXT
    );
    """
    conn.execute(create_table_sql)
    conn.commit()

def format_date(date_string):
    if date_string:
        date_obj = dateutil.parser.isoparse(date_string)
        return date_obj.strftime('%Y-%m-%d %H:%M:%S')
    return ''

def insert_cases(conn, cases):
    insert_sql = """
    INSERT OR IGNORE INTO cases (id, date_created, date_modified, opinion_text)
    VALUES (?, ?, ?, ?);
    """
    
    for case in cases:
        values = (
            case.get('id', None),
            format_date(case.get('date_created', '')),
            format_date(case.get('date_modified', '')),
            case.get('plain_text', '')
        )
        
        conn.execute(insert_sql, values)
    conn.commit()

def main():
    start_year = 2018
    end_year = 2024

    db_path = 'results.db'
    conn = sqlite3.connect(db_path)
    create_table(conn)

    for year in range(start_year, end_year + 1):
        page = 1
        while True:
            try:
                data = fetch_cases(year, page)
                if 'results' in data and data['results']:
                    insert_cases(conn, data['results'])
                    page += 1
                    print(f"Fetched page {page} for year {year}")
                    if page > 2:
                        break
                else:
                    break
                time.sleep(1)
            except Exception as e:
                print(f"An error occurred: {e}")
                break

    conn.close()
    print("Data fetched and stored in results.db")
    print(f"Database file location: {os.path.abspath(db_path)}")

if __name__ == "__main__":
    main()
