import psycopg2
import csv



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
                row_num <= 5 AND
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
    query_execute(connection_params)
    print("Location wise sql query with top 5 genres")
