import psycopg2
import csv
# SQL statements to create tables
create_database_statement = "CREATE DATABASE masterdb;"



create_statements = {
    'content_repository': """
        CREATE TABLE IF NOT EXISTS content_repository (
            content_id SERIAL,
            title VARCHAR(255),
            genre VARCHAR(255),
            duration INTERVAL,
            content_file_reference VARCHAR(255),
            view_count INTEGER,
            PRIMARY KEY (content_id, genre, view_count)
        ) PARTITION BY LIST (genre);
        CREATE TABLE IF NOT EXISTS content_repository_action PARTITION OF content_repository
        FOR VALUES IN ('Action') PARTITION BY RANGE(view_count);
        CREATE TABLE IF NOT EXISTS content_repository_adventure PARTITION OF content_repository
        FOR VALUES IN ('Adventure') PARTITION BY RANGE(view_count);
        CREATE TABLE IF NOT EXISTS content_repository_children PARTITION OF content_repository
        FOR VALUES IN ('Children') PARTITION BY RANGE(view_count);
        CREATE TABLE IF NOT EXISTS content_repository_comedy PARTITION OF content_repository
        FOR VALUES IN ('Comedy') PARTITION BY RANGE(view_count);
        CREATE TABLE IF NOT EXISTS content_repository_crime PARTITION OF content_repository
        FOR VALUES IN ('Crime') PARTITION BY RANGE(view_count);
        CREATE TABLE IF NOT EXISTS content_repository_documentary PARTITION OF content_repository
        FOR VALUES IN ('Documentary') PARTITION BY RANGE(view_count);
        CREATE TABLE IF NOT EXISTS content_repository_drama PARTITION OF content_repository
        FOR VALUES IN ('Drama') PARTITION BY RANGE(view_count);
        CREATE TABLE IF NOT EXISTS content_repository_fantasy PARTITION OF content_repository
        FOR VALUES IN ('Fantasy') PARTITION BY RANGE(view_count);
        CREATE TABLE IF NOT EXISTS content_repository_horror PARTITION OF content_repository
        FOR VALUES IN ('Horror') PARTITION BY RANGE(view_count);
        CREATE TABLE IF NOT EXISTS content_repository_mystery PARTITION OF content_repository
        FOR VALUES IN ('Mystery') PARTITION BY RANGE(view_count);
        CREATE TABLE IF NOT EXISTS content_repository_romance PARTITION OF content_repository
        FOR VALUES IN ('Romance') PARTITION BY RANGE(view_count);
        CREATE TABLE IF NOT EXISTS content_repository_thriller PARTITION OF content_repository
        FOR VALUES IN ('Thriller') PARTITION BY RANGE(view_count);
        CREATE TABLE IF NOT EXISTS content_repository_default PARTITION OF content_repository DEFAULT PARTITION BY RANGE(view_count);
        CREATE TABLE IF NOT EXISTS content_repository_action_low_views PARTITION OF content_repository_action
        FOR VALUES FROM (0) TO (500000);
        CREATE TABLE IF NOT EXISTS content_repository_action_high_views PARTITION OF content_repository_action
        FOR VALUES FROM (500000) TO (1000000);
        CREATE TABLE IF NOT EXISTS content_repository_adventure_low_views PARTITION OF content_repository_adventure
        FOR VALUES FROM (0) TO (500000);
        CREATE TABLE IF NOT EXISTS content_repository_adventure_high_views PARTITION OF content_repository_adventure
        FOR VALUES FROM (500000) TO (1000000);
        CREATE TABLE IF NOT EXISTS content_repository_children_low_views PARTITION OF content_repository_children
        FOR VALUES FROM (0) TO (500000);
        CREATE TABLE IF NOT EXISTS content_repository_children_high_views PARTITION OF content_repository_children
        FOR VALUES FROM (500000) TO (1000000);
        CREATE TABLE IF NOT EXISTS content_repository_comedy_low_views PARTITION OF content_repository_comedy
        FOR VALUES FROM (0) TO (500000);
        CREATE TABLE IF NOT EXISTS content_repository_comedy_high_views PARTITION OF content_repository_comedy
        FOR VALUES FROM (500000) TO (1000000);
        CREATE TABLE IF NOT EXISTS content_repository_crime_low_views PARTITION OF content_repository_crime
        FOR VALUES FROM (0) TO (500000);
        CREATE TABLE IF NOT EXISTS content_repository_crime_high_views PARTITION OF content_repository_crime
        FOR VALUES FROM (500000) TO (1000000);
        CREATE TABLE IF NOT EXISTS content_repository_documentary_low_views PARTITION OF content_repository_documentary
        FOR VALUES FROM (0) TO (500000);
        CREATE TABLE IF NOT EXISTS content_repository_documentary_high_views PARTITION OF content_repository_documentary
        FOR VALUES FROM (500000) TO (1000000);
        CREATE TABLE IF NOT EXISTS content_repository_drama_low_views PARTITION OF content_repository_drama
        FOR VALUES FROM (0) TO (500000);
        CREATE TABLE IF NOT EXISTS content_repository_drama_high_views PARTITION OF content_repository_drama
        FOR VALUES FROM (500000) TO (1000000);
        CREATE TABLE IF NOT EXISTS content_repository_fantasy_low_views PARTITION OF content_repository_fantasy
        FOR VALUES FROM (0) TO (500000);
        CREATE TABLE IF NOT EXISTS content_repository_fantasy_high_views PARTITION OF content_repository_fantasy
        FOR VALUES FROM (500000) TO (1000000);
        CREATE TABLE IF NOT EXISTS content_repository_horror_low_views PARTITION OF content_repository_horror
        FOR VALUES FROM (0) TO (500000);
        CREATE TABLE IF NOT EXISTS content_repository_horror_high_views PARTITION OF content_repository_horror
        FOR VALUES FROM (500000) TO (1000000);
        CREATE TABLE IF NOT EXISTS content_repository_mystery_low_views PARTITION OF content_repository_mystery
        FOR VALUES FROM (0) TO (500000);
        CREATE TABLE IF NOT EXISTS content_repository_mystery_high_views PARTITION OF content_repository_mystery
        FOR VALUES FROM (500000) TO (1000000);
        CREATE TABLE IF NOT EXISTS content_repository_romance_low_views PARTITION OF content_repository_romance
        FOR VALUES FROM (0) TO (500000);
        CREATE TABLE IF NOT EXISTS content_repository_romance_high_views PARTITION OF content_repository_romance
        FOR VALUES FROM (500000) TO (1000000);
        CREATE TABLE IF NOT EXISTS content_repository_thriller_low_views PARTITION OF content_repository_thriller
        FOR VALUES FROM (0) TO (500000);
        CREATE TABLE IF NOT EXISTS content_repository_thriller_high_views PARTITION OF content_repository_thriller
        FOR VALUES FROM (500000) TO (1000000);
        CREATE TABLE IF NOT EXISTS content_repository_default_low_views PARTITION OF content_repository_default
        FOR VALUES FROM (0) TO (500000);
        CREATE TABLE IF NOT EXISTS content_repository_default_high_views PARTITION OF content_repository_default
        FOR VALUES FROM (500000) TO (1000000);
    """,
    'user_profiles': """
        CREATE TABLE IF NOT EXISTS user_profiles (
            user_id SERIAL,
            username VARCHAR(255),
            email VARCHAR(255),
            subscription_status BOOLEAN,
            PRIMARY KEY (user_id, subscription_status)
        ) PARTITION BY LIST (subscription_status);
        CREATE TABLE IF NOT EXISTS user_profiles_subscribed PARTITION OF user_profiles
        FOR VALUES IN (true);
        CREATE TABLE IF NOT EXISTS user_profiles_unsubscribed PARTITION OF user_profiles
        FOR VALUES IN (false);
    """,
    'streaming_metadata': """
        CREATE TABLE IF NOT EXISTS streaming_metadata (
            metadata_id SERIAL,
            content_id INTEGER REFERENCES content_repository(content_id),
            bit_rates SMALLINT,
            server_locations VARCHAR(255),
            network_conditions VARCHAR(255),
            PRIMARY KEY (metadata_id, content_id, bit_rates, server_locations)
        ) PARTITION BY RANGE (bit_rates);
        CREATE TABLE IF NOT EXISTS streaming_metadata_low_bit_rates PARTITION OF streaming_metadata
        FOR VALUES FROM (0) TO (50) PARTITION BY LIST(server_locations);
        CREATE TABLE IF NOT EXISTS streaming_metadata_high_bit_rates PARTITION OF streaming_metadata
        FOR VALUES FROM (51) TO (100) PARTITION BY LIST(server_locations);
        CREATE TABLE IF NOT EXISTS streaming_metadata_low_bit_rates_tokyo PARTITION OF streaming_metadata_low_bit_rates
        FOR VALUES IN ('Tokyo');
        CREATE TABLE IF NOT EXISTS streaming_metadata_high_bit_rates_tokyo PARTITION OF streaming_metadata_high_bit_rates
        FOR VALUES IN ('Tokyo');
        CREATE TABLE IF NOT EXISTS streaming_metadata_low_bit_rates_berlin PARTITION OF streaming_metadata_low_bit_rates
        FOR VALUES IN ('Berlin');
        CREATE TABLE IF NOT EXISTS streaming_metadata_high_bit_rates_berlin PARTITION OF streaming_metadata_high_bit_rates
        FOR VALUES IN ('Berlin');
        CREATE TABLE IF NOT EXISTS streaming_metadata_low_bit_rates_nairobi PARTITION OF streaming_metadata_low_bit_rates
        FOR VALUES IN ('Nairobi');
        CREATE TABLE IF NOT EXISTS streaming_metadata_high_bit_rates_nairobi PARTITION OF streaming_metadata_high_bit_rates
        FOR VALUES IN ('Nairobi');
        CREATE TABLE IF NOT EXISTS streaming_metadata_low_bit_rates_rio PARTITION OF streaming_metadata_low_bit_rates
        FOR VALUES IN ('Rio');
        CREATE TABLE IF NOT EXISTS streaming_metadata_high_bit_rates_rio PARTITION OF streaming_metadata_high_bit_rates
        FOR VALUES IN ('Rio');
        CREATE TABLE IF NOT EXISTS streaming_metadata_low_bit_rates_denver PARTITION OF streaming_metadata_low_bit_rates
        FOR VALUES IN ('Denver');
        CREATE TABLE IF NOT EXISTS streaming_metadata_high_bit_rates_denver PARTITION OF streaming_metadata_high_bit_rates
        FOR VALUES IN ('Denver');
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
            user_id INTEGER PRIMARY KEY REFERENCES user_profiles(user_id),
            geolocation_id INTEGER,
            user_geolocation_lat DOUBLE PRECISION,
            user_geolocation_long DOUBLE PRECISION,
            server_location VARCHAR(255)
        );
    """,
    'billing': """
        CREATE TABLE IF NOT EXISTS billing (
            billing_id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES user_profiles(user_id),
            payment_information VARCHAR(255),
            subscription_details VARCHAR(255)
        );
    """,
    'content_recommendations': """
        CREATE TABLE IF NOT EXISTS content_recommendations (
            recommendation_id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES user_profiles(user_id),
            user_preferences VARCHAR(255),
            viewing_history INTEGER REFERENCES content_repository(content_id)
        );
    """,
    'user_preferences': """
        CREATE TABLE IF NOT EXISTS user_preferences (
            user_id INTEGER REFERENCES user_profiles(user_id),
            genre_pref VARCHAR(255)
        );
    """,
    'viewing_history': """
        CREATE TABLE IF NOT EXISTS viewing_history (
            user_id INTEGER REFERENCES user_profiles(user_id),
            content_id INTEGER REFERENCES content_repository(content_id)
        );
    """,
    'logging': """
        CREATE TABLE IF NOT EXISTS logging (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES user_profiles(user_id),
            system_metrics VARCHAR(255),
            gender VARCHAR(255),
            ip_address VARCHAR(255),
            email VARCHAR(255)
        );
    """,
    'server_locations': """
        CREATE TABLE IF NOT EXISTS server_locations (
            server_id SERIAL PRIMARY KEY,
            server_location VARCHAR(255)
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
            listofstatements = statement.split(';')
            print(table)
            for eachstatement in range(len(listofstatements)-1):
                print(listofstatements[eachstatement])
                cursor.execute(listofstatements[eachstatement])
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

        with open(csv_path, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            header = next(csv_reader)  # Assuming the first row contains column names

            for row in csv_reader:
                values = tuple(row)
                print(values)
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

# Function to create the database
def create_database(connection_params):
    conn = None
    # Connect to the default 'postgres' database to create a new database
    conn = connect_postgres("masterdb")
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    # cur.execute(f"CREATE DATABASE {connection_params['dbname']}")
    cur.close()
    conn.close()

def connect_postgres(dbname):
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
    'user': 'postgresadmin',
    'password': 'admin123',
    'host': 'localhost',
    'port': '5001'
}

# Placeholder for connection parameters
connection_params = {
    'database': 'masterdb',
    'user': 'postgresadmin',
    'password': 'admin123',
    'host': 'localhost',
    'port': '5001'
}


if __name__ == "__main__":
    create_database(default_connection_params)
    print("done database")
    create_tables(connection_params)
    with connect_postgres(dbname="masterdb") as conn:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        
        print("done connection")
    # insert_data_from_csv(connection_params, 'content_repository', './datasets/content_repository.csv')
    # print("uploaded content_repository")
    
    insert_data_from_csv(connection_params, 'user_profiles', './datasets/user_profiles.csv')
    print("uploaded user_profiles")
    
    insert_data_from_csv(connection_params, 'streaming_metadata', './datasets/streaming_metadata.csv')
    print("uploaded streaming_metadata")
    
    # insert_data_from_csv(connection_params, 'authentication', './datasets/authentication.csv')
    # print("uploaded authentication")
    
    # insert_data_from_csv(connection_params, 'geolocation', './datasets/geolocation.csv')
    # print("uploaded geolocation")
    
    # insert_data_from_csv(connection_params, 'billing', './datasets/billing.csv')
    # print("uploaded billing")
    
    # insert_data_from_csv(connection_params, 'user_preferences', './datasets/user_preferences.csv')
    # print("uploaded user_preferences")
    
    # insert_data_from_csv(connection_params, 'viewing_history', './datasets/viewing_history.csv')
    # print("uploaded viewing_history")
    
    # insert_data_from_csv(connection_params, 'logging', './datasets/logging.csv')
    # print("uploaded logging")
    
    # insert_data_from_csv(connection_params, 'server_locations', './datasets/server_locations.csv')
    # print("uploaded server_locations")