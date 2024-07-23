import sqlite3
import pandas as pd

# Function to create database and table
def create_database(database_name):
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()
    
    # Create trips table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS trips (
        pickup_date DATE PRIMARY KEY,
        total_trips INTEGER,
        average_fare REAL
    )
    ''')
    
    conn.commit()
    conn.close()

# Function to load data into SQLite database
def load_data_into_database(data, database_name):
    conn = sqlite3.connect(database_name)
    
    # Append data to SQLite database
    data.to_sql('trips', conn, if_exists='append', index=False)
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    processed_data_file = 'processed_trip_data_2019.csv'  # Replace with your processed data file
    database_name = 'trip_metrics.db'  # SQLite database name
    
    # Load processed data into pandas DataFrame
    processed_data = pd.read_csv(processed_data_file)
    
    # Create SQLite database and table
    create_database(database_name)
    
    # Load data into SQLite database
    load_data_into_database(processed_data, database_name)
    
    print(f'Data loaded into {database_name} successfully.')
