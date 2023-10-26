from random import random
import psycopg2
import random
from datetime import date, timedelta
# SQL statements to create tables
create_statements = {
    'content_repository': """
        CREATE TABLE IF NOT EXISTS content_repository (
            content_id INTEGER PRIMARY KEY,
            title VARCHAR(255),
            genre VARCHAR(255),
            duration TIMESTAMP,
            content_file_reference VARCHAR(255)
        );
    """,
    
    'user_profiles': """
        CREATE TABLE IF NOT EXISTS user_profiles (
            user_id INTEGER PRIMARY KEY,
            username VARCHAR(255),
            email VARCHAR(255),
            subscription_status BOOLEAN
        );
    """,
    
    'streaming_metadata': """
        CREATE TABLE IF NOT EXISTS streaming_metadata (
            metadata_id INTEGER PRIMARY KEY,
            content_id INTEGER REFERENCES content_repository(content_id),
            bitrates VARCHAR(255),
            server_locations VARCHAR(255),
            network_conditions VARCHAR(255)
        );
    """,
    
    'authentication': """
        CREATE TABLE IF NOT EXISTS authentication (
            user_id INTEGER REFERENCES user_profiles(user_id) PRIMARY KEY,
            credential VARCHAR(255),
            access_permission VARCHAR(255)
        );
    """,
    
    'geolocation': """
        CREATE TABLE IF NOT EXISTS geolocation (
            geolocation_id INTEGER PRIMARY KEY,
            user_id INTEGER REFERENCES user_profiles(user_id),
            user_geolocation_data VARCHAR(255),
            server_locations VARCHAR(255)
        );
    """,
    
    'billing': """
        CREATE TABLE IF NOT EXISTS billing (
            billing_id INTEGER PRIMARY KEY,
            user_id INTEGER REFERENCES user_profiles(user_id),
            payment_information VARCHAR(255),
            subscription_details VARCHAR(255)
        );
    """,
    
    'content_recommendations': """
        CREATE TABLE IF NOT EXISTS content_recommendations (
            recommendation_id INTEGER PRIMARY KEY,
            user_id INTEGER REFERENCES user_profiles(user_id),
            user_preferences VARCHAR(255),
            viewing_history INTEGER REFERENCES content_repository(content_id)
        );
    """,
    
    'viewing_history': """
        CREATE TABLE IF NOT EXISTS viewing_history (
            user_id INTEGER REFERENCES user_profiles(user_id),
            content_id INTEGER REFERENCES content_repository(content_id),
            PRIMARY KEY (user_id, content_id)
        );
    """,
    
    'logging': """
        CREATE TABLE IF NOT EXISTS logging (
            log_id INTEGER PRIMARY KEY,
            user_id INTEGER REFERENCES user_profiles(user_id),
            system_metrics VARCHAR(255),
            user_interactions VARCHAR(255)
        );
    """
}

# Function to create tables in the database
def create_tables(connection_params):
    conn = None
    try:
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()
        for table, statement in create_statements.items():
            cursor.execute(statement)
        conn.commit()
        cursor.close()
    except Exception as error:
        print(f"Error: {error}")
    finally:
        if conn is not None:
            conn.close()

# Function to insert data into a specified table
def insert_data(connection_params, table_name, columns, values):
    conn = None
    try:
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()
        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s' for _ in values])})"
        cursor.execute(sql, values)
        conn.commit()
        cursor.close()
    except Exception as error:
        print(f"Error: {error}")
    finally:
        if conn is not None:
            conn.close()

# Function to delete data from a specified table based on a condition
def delete_data(connection_params, table_name, condition=None):
    conn = None
    try:
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()
        sql = f"DELETE FROM {table_name}"
        if condition:
            sql += f" WHERE {condition}"
        cursor.execute(sql)
        conn.commit()
        cursor.close()
    except Exception as error:
        print(f"Error: {error}")
    finally:
        if conn is not None:
            conn.close()

# Function to delete all data from all tables
def delete_all_data(connection_params):
    conn = None
    try:
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()
        tables_in_order = [
            'logging', 'viewing_history', 'content_recommendations', 
            'billing', 'geolocation', 'authentication', 
            'streaming_metadata', 'user_profiles', 'content_repository'
        ]
        for table in tables_in_order:
            cursor.execute(f"DELETE FROM {table}")
        conn.commit()
        cursor.close()
    except Exception as error:
        print(f"Error: {error}")
    finally:
        if conn is not None:
            conn.close()

# Placeholder for connection parameters
connection_params = {
    'dbname': 'YOUR_DB_NAME',
    'user': 'YOUR_USER_NAME',
    'password': 'YOUR_PASSWORD',
    'host': 'YOUR_HOST',
    'port': 'YOUR_PORT'
}


if __name__ == "__main__":
    create_tables(connection_params)