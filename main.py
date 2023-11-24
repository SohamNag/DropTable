import psycopg2
import csv

# SQL statements to create tables
create_database_statement = "CREATE DATABASE masterdb;"

create_statements = {
    "content_repository": """
        CREATE TABLE IF NOT EXISTS content_repository (
            content_id INTEGER NOT NULL,
            title VARCHAR(255),
            genre VARCHAR(255) NOT NULL,
            duration INTERVAL,
            content_file_reference VARCHAR(255),
            view_count INTEGER,
            PRIMARY KEY (content_id, genre)
        ) PARTITION BY LIST(genre);
    """,
    "user_profiles": """
        CREATE TABLE IF NOT EXISTS user_profiles (
            user_id SERIAL PRIMARY KEY,
            username VARCHAR(255),
            email VARCHAR(255),
            subscription_status BOOLEAN
        );
    """,
    "streaming_metadata": """
        CREATE TABLE IF NOT EXISTS streaming_metadata (
            metadata_id INTEGER NOT NULL,
            content_id INTEGER NOT NULL,
            bit_rates VARCHAR(255),
            server_locations VARCHAR(255) NOT NULL,
            network_conditions VARCHAR(255),
            PRIMARY KEY (metadata_id, server_locations)
        ) PARTITION BY LIST(server_locations);
    """,
    "authentication": """
        CREATE TABLE IF NOT EXISTS authentication (
            user_id INTEGER PRIMARY KEY REFERENCES user_profiles(user_id),
            credential VARCHAR(255),
            access_permission VARCHAR(255)
        );
    """,
    "geolocation": """
        CREATE TABLE IF NOT EXISTS geolocation (
            user_id INTEGER PRIMARY KEY REFERENCES user_profiles(user_id),
            geolocation_id SERIAL,
            user_geolocation_lat DOUBLE PRECISION,
            user_geolocation_long DOUBLE PRECISION
        );
    """,
    "billing": """
        CREATE TABLE IF NOT EXISTS billing (
            billing_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL REFERENCES user_profiles(user_id),
            payment_info VARCHAR(255),
            subscription_details VARCHAR(255) NOT NULL
        ) PARTITION BY LIST(subscription_details);
    """,
    "content_recommendations": """
        CREATE TABLE IF NOT EXISTS content_recommendations (
            recommendation_id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES user_profiles(user_id),
            user_preferences VARCHAR(255),
            content_id INTEGER,
            genre VARCHAR(255),
            FOREIGN KEY (content_id, genre) REFERENCES content_repository (content_id, genre)
        );
    """,
    "user_preferences": """
        CREATE TABLE IF NOT EXISTS user_preferences (
            user_id INTEGER REFERENCES user_profiles(user_id),
            genre_pref VARCHAR(255)
        );
    """,
    "viewing_history": """
        CREATE TABLE IF NOT EXISTS viewing_history (
            user_id INTEGER REFERENCES user_profiles(user_id),
            content_id INTEGER,
            genre VARCHAR(255),
            FOREIGN KEY (content_id, genre) REFERENCES content_repository (content_id, genre)
        );
    """,
    "logging": """
        CREATE TABLE IF NOT EXISTS logging (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES user_profiles(user_id),
            system_metrics VARCHAR(255),
            gender VARCHAR(255),
            ip_address VARCHAR(255),
            email VARCHAR(255)
        );
    """,
    "server_locations": """
        CREATE TABLE IF NOT EXISTS server_locations (
            server_id SERIAL PRIMARY KEY,
            server_location VARCHAR(255)
        );
    """,
}

def create_sequences(connection_params):
    # Define sequence creation statements for each table
    sequence_statements = {
        'content_repository_content_id_seq': "CREATE SEQUENCE IF NOT EXISTS content_repository_content_id_seq;",
        'streaming_metadata_metadata_id_seq': "CREATE SEQUENCE IF NOT EXISTS streaming_metadata_metadata_id_seq;",
        'billing_billing_id_seq': "CREATE SEQUENCE IF NOT EXISTS billing_billing_id_seq;"
    }
    
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()
    
    # Execute each sequence creation statement
    for seq_name, seq_stmt in sequence_statements.items():
        cursor.execute(seq_stmt)
        print(f"Sequence {seq_name} created successfully.")
        
    # Commit the changes
    conn.commit()
    
    # Close the cursor and the connection
    cursor.close()
    conn.close()
    print("All sequences created successfully.")
    
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
        if conn is not None:
            conn.rollback()
    finally:
        if conn is not None:
            conn.close()


# Function to insert data into a specified table from a CSV file
def insert_data_from_csv(connection_params, table_name, csv_path):
    conn = None
    try:
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()

        with open(csv_path, "r") as csv_file:
            csv_reader = csv.reader(csv_file)
            header = next(csv_reader)  # Assuming the first row contains column names

            for row in csv_reader:
                values = tuple(row)
                sql = f"INSERT INTO {table_name} ({', '.join(header)}) VALUES ({', '.join(['%s' for _ in values])})"
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
            "logging",
            "viewing_history",
            "content_recommendations",
            "billing",
            "geolocation",
            "authentication",
            "streaming_metadata",
            "user_profiles",
            "content_repository",
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


# Function to create the database
def create_database(connection_params):
    conn = None
    # Connect to the default 'postgres' database to create a new database
    conn = connect_potsgres("masterdb")
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    # cur.execute(f"CREATE DATABASE {connection_params['dbname']}")
    cur.close()
    conn.close()


def connect_potsgres(dbname):
    """
    Connect to the PostgreSQL using psycopg2 with default database
    Return the connection
    """

    conn = psycopg2.connect(
        host="localhost",
        database=dbname,
        user="postgresadmin",
        password="admin123",  # change this to your password
        port="5001",
    )
    print("this is conn", conn)
    return conn


# Connection parameters for the default 'postgres' database
default_connection_params = {
    "user": "postgresadmin",
    "password": "admin123",
    "host": "localhost",
    "port": "5001",
}

# Placeholder for connection parameters
connection_params = {
    "dbname": "masterdb",
    "user": "postgresadmin",
    "password": "admin123",
    "host": "localhost",
    "port": "5001",
}

unique_genres = [
    'Crime',
    'Comedy',
    'Action',
    'Children',
    'Drama',
    'Adventure',
    'Thriller',
    'Documentary',
    'Horror',
    'Mystery',
    'Romance'
]

server_locations = [
    'Tokyo',
    'Nairobi',
    'Denver',
    'Berlin',
    'Rio',
]

def create_horizontal_partition_for_genre(connection_params, genre):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()
    
    # Format the partition table name to include the genre in a safe way
    # This is a simple example, and for production code, you should ensure
    # that the genre string is safe to include in a SQL statement to prevent SQL injection
    partition_table_name = f"content_repository_{genre.replace(' ', '_').lower()}"
    
    # Create a new partition for the specified genre
    create_partition_statement = f"""
        CREATE TABLE IF NOT EXISTS {partition_table_name} PARTITION OF content_repository
        FOR VALUES IN (%s);
    """
    
    # Execute the create partition statement
    cursor.execute(create_partition_statement, (genre,))
    
    # Commit the changes
    conn.commit()
    
    # Close the cursor and the connection
    cursor.close()
    conn.close()
    
    print(f"Partition for genre '{genre}' created successfully.")

def create_horizontal_partition_for_server_location(connection_params, server_location):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()
    
    # Format the partition table name to include the server location in a safe way
    # This is a simple example; for production code, ensure the server_location string is safe to include in a SQL statement
    partition_table_name = f"streaming_metadata_{server_location.replace(' ', '_').lower()}"
    
    # Create a new partition for the specified server location
    create_partition_statement = f"""
        CREATE TABLE IF NOT EXISTS {partition_table_name} PARTITION OF streaming_metadata
        FOR VALUES IN (%s);
    """
    
    # Execute the create partition statement
    cursor.execute(create_partition_statement, (server_location,))
    
    # Commit the changes
    conn.commit()
    
    # Close the cursor and the connection
    cursor.close()
    conn.close()
    
    print(f"Partition for server location '{server_location}' created successfully.")

def create_horizontal_partition_for_subscription_details(connection_params, subscription_detail):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()
    
    # Format the partition table name to include the subscription detail in a safe way
    partition_table_name = f"billing_{subscription_detail.replace(' ', '_').lower()}"
    
    # Create a new partition for the specified subscription detail
    create_partition_statement = f"""
        CREATE TABLE IF NOT EXISTS {partition_table_name} PARTITION OF billing
        FOR VALUES IN (%s);
    """
    
    # Execute the create partition statement
    cursor.execute(create_partition_statement, (subscription_detail,))
    
    # Commit the changes
    conn.commit()
    
    # Close the cursor and the connection
    cursor.close()
    conn.close()
    
    print(f"Partition for subscription detail '{subscription_detail}' created successfully.")

def drop_all_tables(connection_params):
    conn = None
    try:
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()
        tables_in_order = [
            "logging",
            "viewing_history",
            "content_recommendations",
            "billing",
            "geolocation",
            "authentication",
            "streaming_metadata",
            "user_profiles",
            "content_repository",
            "server_locations",
            "user_preferences"
        ]
        for table in tables_in_order:
            cursor.execute(f"DROP TABLE {table} CASCADE")
        conn.commit()
        cursor.close()
    except Exception as error:
        print(f"Error: {error}")
    finally:
        if conn is not None:
            conn.close()

def query_execute(connection_params):
    conn = None
    try:
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()

        sql = """EXPLAIN ANALYZE CREATE MATERIALIZED VIEW top_genres_by_geolocation1 AS
                SELECT
                unnest(string_to_array(user_preferences.genre_pref, '|')) AS genre,
                COUNT(*) AS genre_count,
                geolocation.geolocation_id,
                ROW_NUMBER() OVER (PARTITION BY geolocation.geolocation_id ORDER BY COUNT(*) DESC) AS row_num
                FROM
                user_preferences
                JOIN
                geolocation ON user_preferences.user_id = geolocation.user_id
                WHERE
                user_preferences.genre_pref <> ''
                GROUP BY
                genre, geolocation.geolocation_id;

                EXPLAIN ANALYZE SELECT geolocation.geolocation_id,subscription_details,count(subscription_details)
                FROM geolocation JOIN billing on billing.user_id = geolocation.user_id
                GROUP BY geolocation.geolocation_id,subscription_details 
                ORDER BY geolocation.geolocation_id,subscription_details DESC;

                EXPLAIN ANALYZE SELECT geolocation.geolocation_id,subscription_details,count(subscription_details)
                FROM geolocation JOIN billing on billing.user_id = geolocation.user_id
                GROUP BY geolocation.geolocation_id,subscription_details 
                ORDER BY geolocation.geolocation_id,subscription_details DESC;
                
                
                EXPLAIN ANALYZE WITH RankedData AS (
                SELECT
                    SUM(duration) as duration,
                    genre,
                    streaming_metadata.server_locations,
                    ROW_NUMBER() OVER (PARTITION BY streaming_metadata.server_locations ORDER BY SUM(duration) DESC) AS row_num
                FROM
                    streaming_metadata
                JOIN
                    content_repository ON content_repository.content_id = streaming_metadata.content_id
                GROUP BY
                    genre, streaming_metadata.server_locations
                )
                SELECT
                    duration,
                    genre,
                    server_locations
                FROM
                    RankedData
                WHERE
                    row_num <= 3
                ORDER BY
                    server_locations, row_num;
                """

        cursor.execute(sql)
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        conn.commit()
        cursor.close()
    except Exception as error:
        print(f"Error: {error}")
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    drop_all_tables(connection_params)
    create_tables(connection_params)
    create_sequences(connection_params)
    for genre in unique_genres:
        create_horizontal_partition_for_genre(connection_params, genre)
    insert_data_from_csv(connection_params, 'content_repository', './datasets/content_repository.csv')
    print("uploaded content_repository")

    insert_data_from_csv(connection_params, 'user_profiles', './datasets/user_profiles.csv')
    print("uploaded user_profiles")

    for server_location in server_locations:
        create_horizontal_partition_for_server_location(connection_params, server_location)
    insert_data_from_csv(connection_params, 'streaming_metadata', './datasets/streaming_metadata.csv')
    print("uploaded streaming_metadata")

    insert_data_from_csv(connection_params, 'authentication', './datasets/authentication.csv')
    print("uploaded authentication")

    insert_data_from_csv(connection_params, 'geolocation', './datasets/geolocation.csv')
    print("uploaded geolocation")

    for tiers in ['tier1', 'tier2', 'tier3']:
        create_horizontal_partition_for_subscription_details(connection_params, tiers)
    insert_data_from_csv(connection_params, 'billing', './datasets/billing.csv')
    print("uploaded billing")

    insert_data_from_csv(connection_params, 'user_preferences', './datasets/user_preferences.csv')
    print("uploaded user_preferences")

    insert_data_from_csv(connection_params, 'viewing_history', './datasets/viewing_history.csv')
    print("uploaded viewing_history")

    insert_data_from_csv(connection_params, 'logging', './datasets/logging.csv')
    print("uploaded logging")

    insert_data_from_csv(connection_params, 'server_locations', './datasets/server_locations.csv')
    print("uploaded server_locations")

    query_execute(connection_params)
    print("Location wise sql query with top 5 genres")
