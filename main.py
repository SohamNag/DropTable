# pylint: disable=C0116
# pylint: disable=C0103
# pylint: disable=C0411
# pylint: disable=W0621
# pylint: disable=C0301
# pylint: disable=W0612
# pylint: disable=W0718
# pylint: disable=W1514

import psycopg2
import csv
from psycopg2 import OperationalError
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

# Function to create sequences in the database
def create_sequences(connection_params):
    # Define sequence creation statements for each table
    sequence_statements = {
        "content_repository_content_id_seq": "CREATE SEQUENCE IF NOT EXISTS content_repository_content_id_seq;",
        "streaming_metadata_metadata_id_seq": "CREATE SEQUENCE IF NOT EXISTS streaming_metadata_metadata_id_seq;",
        "billing_billing_id_seq": "CREATE SEQUENCE IF NOT EXISTS billing_billing_id_seq;",
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
def create_database():
    conn = None
    # Connect to the default 'postgres' database to create a new database
    conn = connect_potsgres("masterdb")
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    # cur.execute(f"CREATE DATABASE {connection_params['dbname']}")
    cur.close()
    conn.close()

# Function to connect to the PostgreSQL database
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

# Function to create a horizontal partition for a specified genre
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

# Function to create a horizontal partition for a specified server location
def create_horizontal_partition_for_server_location(connection_params, server_location):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()

    # Format the partition table name to include the server location in a safe way
    # This is a simple example; for production code, ensure the server_location string is safe to include in a SQL statement
    partition_table_name = (
        f"streaming_metadata_{server_location.replace(' ', '_').lower()}"
    )

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

# Function to create a horizontal partition for a specified subscription detail
def create_horizontal_partition_for_subscription_details(
    connection_params, subscription_detail
):
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

    print(
        f"Partition for subscription detail '{subscription_detail}' created successfully."
    )

# Function to drop all tables
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
            "user_preferences",
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

# Function to execute sql queries
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

# Function to initialise the database
def initialise_db_tables(connection_params):
    drop_all_tables(connection_params)
    create_tables(connection_params)
    create_sequences(connection_params)

# Function to initialise the database with data
def initialise_db_data(connection_params):
    for genre in unique_genres:
        create_horizontal_partition_for_genre(connection_params, genre)

    for server_location in server_locations:
        create_horizontal_partition_for_server_location(
            connection_params, server_location
        )

    for tiers in ["tier1", "tier2", "tier3"]:
        create_horizontal_partition_for_subscription_details(connection_params, tiers)

    for table, filename in datasets.items():
        insert_data_from_csv(connection_params, table, filename)
        print(f"uploaded {table}")

# Function to insert content into the database
def insert_content_from_csv(csv_path):
    '''
    depending on the genre, the data is distributed across the two nodes
    '''
    conn1 = None
    conn2 = None
    
    try:
        conn1 = psycopg2.connect(**connection_params)
        cursor1 = conn1.cursor()
        
        conn2 = psycopg2.connect(**connection_params_2)
        cursor2 = conn2.cursor()
        
        with open(csv_path, "r") as csv_file:
            csv_reader = csv.reader(csv_file)
            header = next(csv_reader)
            
            for row in csv_reader:
                values = tuple(row)
                if (values[2] in unique_genres[:6]):
                    sql = f"INSERT INTO content_repository (content_id, title, genre, duration, content_file_reference, view_count) VALUES (%s, %s, %s, %s, %s, %s)"
                    cursor1.execute(sql, values)
                else:
                    sql = f"INSERT INTO content_repository (content_id, title, genre, duration, content_file_reference, view_count) VALUES (%s, %s, %s, %s, %s, %s)"
                    cursor2.execute(sql, values)
        conn1.commit()
        conn2.commit()
        cursor1.close()
        cursor2.close()
    except Exception as error:
        print(f"Error: {error}")
        if conn1 is not None:
            conn1.rollback()
        if conn2 is not None:
            conn2.rollback()
    finally:
        if conn1 is not None:
            conn1.close()
        if conn2 is not None:
            conn2.close()
        
# Data is distributed across the two nodes based on the genre
def insert_content(content):
    conn_params = None
    if (content["genre"] in unique_genres[:6]):
        conn_params = connection_params
        print("inserting in node 1 \n")
    else:
        conn_params = connection_params_2
        print("inserting in node 2 \n")
    conn = None
    # convert content to tuple
    content = tuple(content.values())
    print(content)
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        sql = f"INSERT INTO content_repository (content_id, title, genre, duration, content_file_reference, view_count) VALUES (%s, %s, %s, %s, %s, %s)"
        cursor.execute(sql, content)
        conn.commit()
        cursor.close()
    except Exception as error:
        print(f"Error: {error}")
        if conn is not None:
            conn.rollback()
    finally:
        if conn is not None:
            conn.close()

def retrieve_content(content_name):
    '''
    Tries to retrieve the content from both the nodes in a round robin fashion.
    '''
    # Create a query string
    query = "SELECT * FROM content_repository WHERE title = %s;"
    
    # Attempt to connect to node1
    try:
        # Establish a connection to the first node
        print("searching in node 1 \n")
        conn = psycopg2.connect(**connection_params)
        cur = conn.cursor()
        cur.execute(query, (content_name,))
        result = cur.fetchone()
        cur.close()
        conn.close()
        if result:
            return result
    except OperationalError as e:
        print(f"Error connecting to the first node: {e}")

    # If node1 fails, attempt to connect to node2
    try:
        # Establish a connection to the second node
        print("searching in node 2 \n")
        conn = psycopg2.connect(**connection_params_2)
        cur = conn.cursor()
        cur.execute(query, (content_name,))
        result = cur.fetchone()
        cur.close()
        conn.close()
        if result:
            return result
    except OperationalError as e:
        print(f"Error connecting to the second node: {e}")
    
    # If both nodes fail, return None or raise an exception
    return None
        
# Connection parameters for the default 'postgres' database
default_connection_params = {
    "user": "postgresadmin",
    "password": "admin123",
    "host": "localhost",
    "port": "5001",
}

# connection parameters
# for node 1
connection_params = {
    "dbname": "masterdb",
    "user": "postgresadmin",
    "password": "admin123",
    "host": "localhost",
    "port": "5001",
}

# for node 2
connection_params_2 = {
    "dbname": "masterdb",
    "user": "postgresadmin",
    "password": "admin123",
    "host": "localhost",
    "port": "5003",
}

unique_genres = [
    "Crime",
    "Comedy",
    "Action",
    "Children",
    "Drama",
    "Adventure",
    "Thriller",
    "Documentary",
    "Horror",
    "Mystery",
    "Romance",
]

server_locations = [
    "Tokyo",
    "Nairobi",
    "Denver",
    "Berlin",
    "Rio",
]

datasets = {
    # "content_repository": "./datasets/content_repository.csv",
    "user_profiles": "./datasets/user_profiles.csv",
    "streaming_metadata": "./datasets/streaming_metadata.csv",
    "authentication": "./datasets/authentication.csv",
    "geolocation": "./datasets/geolocation.csv",
    "billing": "./datasets/billing.csv",
    "user_preferences": "./datasets/user_preferences.csv",
    "viewing_history": "./datasets/viewing_history.csv",
    "logging": "./datasets/logging.csv",
    "server_locations": "./datasets/server_locations.csv",
}

if __name__ == "__main__":

    initialise_db_tables(connection_params)
    initialise_db_tables(connection_params_2)

    initialise_db_data(connection_params)
    initialise_db_data(connection_params_2)
    
    insert_content_from_csv("./datasets/content_repository.csv")
    
    print("Location wise sql query with top 5 genres in node 1")
    query_execute(connection_params)
    print("Location wise sql query with top 5 genres in node 2")
    query_execute(connection_params_2)
    
    comedy_movie = {
        "content_id": 101,
        "title": "ComedyMovie",
        "genre": "Comedy",
        "duration": "1 hour",
        "content_file_reference": "test",
        "view_count": 5130,
    }
    
    mystery_movie = {
        "content_id": 102,
        "title": "MysteryMovie",
        "genre": "Mystery",
        "duration": "2 hours",
        "content_file_reference": "Mystery",
        "view_count": 13340,
    }
    insert_content(comedy_movie)
    insert_content(mystery_movie)
    
    search_contents = ["ComedyMovie", "MysteryMovie", "ExampleMovie"]
    
    for content in search_contents:
        result = retrieve_content(content)
        if result:
            print(f"Content found: {result}")
        else:
            print(f"Content not found: {content}")
    

