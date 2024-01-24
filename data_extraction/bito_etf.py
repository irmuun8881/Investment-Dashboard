import requests
import mysql.connector
from mysql.connector import Error
from datetime import datetime

# API Details
api_key = "7VKK1LOK7SBHXPRM"  # Replace with your actual API key
api_url = "https://www.alphavantage.co/query?"
function = "TIME_SERIES_DAILY"
symbol = "bito"
datatype = "json"
final_api_url = f"{api_url}function={function}&symbol={symbol}&outputsize=full&apikey={api_key}&datatype={datatype}"

def create_table(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS bito_etf (
        id INT AUTO_INCREMENT PRIMARY KEY,
        timestamp DATE NOT NULL,
        high DECIMAL(10,2) NOT NULL,
        volume BIGINT NOT NULL
    )
    """)

def get_last_update_date(cursor):
    cursor.execute("SELECT MAX(timestamp) FROM bito_etf")
    result = cursor.fetchone()
    return result[0] if result[0] else None

def get_stock_data():
    response = requests.get(final_api_url)
    if response.status_code == 200:
        data = response.json()
        time_series = data["Time Series (Daily)"]
        return time_series
    else:
        print(f"Error: {response.status_code}")
        return None

def filter_new_data(time_series, last_update_date):
    filtered_data = []
    # Convert last_update_date to datetime.datetime if it's not None
    last_update_datetime = datetime.combine(last_update_date, datetime.min.time()) if last_update_date else None
    for date, daily_data in time_series.items():
        current_date = datetime.strptime(date, "%Y-%m-%d")
        # Compare current_date to last_update_datetime
        if last_update_datetime is None or current_date > last_update_datetime:
            filtered_data.append((
                date,
                float(daily_data["2. high"]),
                int(daily_data["5. volume"]),
            ))
    return filtered_data

def insert_data(cursor, data):
    query = """
    INSERT INTO bito_etf (timestamp, high, volume)
    VALUES (%s, %s, %s)
    """
    cursor.executemany(query, data)

def main():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="26438881",  # Replace with your MySQL password
            database="investment"
        )
        cursor = connection.cursor()
        create_table(cursor)
        
        time_series = get_stock_data()
        if time_series:
            last_update_date = get_last_update_date(cursor)
            new_data = filter_new_data(time_series, last_update_date)
            if new_data:
                insert_data(cursor, new_data)
                connection.commit()
            else:
                print("No new data to update.")

    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    main()
