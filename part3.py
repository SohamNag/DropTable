import psycopg2

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

        sql = """
                SELECT pg_advisory_lock(1); 
                EXPLAIN ANALYZE CREATE MATERIALIZED VIEW top_genres_by_geolocation1 AS
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
                SELECT pg_advisory_unlock(1);
                """

        cursor.execute(sql)
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        conn.commit()

        sql = """
                BEGIN;
                LOCK TABLE content_repository IN SHARE MODE;
                CREATE INDEX idx_user_preferences ON user_preferences(user_id, genre_pref);
                CREATE INDEX idx_viewing_history ON viewing_history(user_id, content_id);
                CREATE INDEX idx_content_repository_content_id ON content_repository(content_id);
                CREATE INDEX idx_content_repository_genre ON content_repository(genre);

                EXPLAIN ANALYZE Select * from content_repository where genre in (SELECT DISTINCT genre_pref FROM user_preferences WHERE user_id = 95
                UNION
                SELECT DISTINCT cr.genre FROM content_repository cr JOIN viewing_history vh ON cr.content_id = vh.content_id
                WHERE vh.user_id = 95);
                COMMIT;
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
