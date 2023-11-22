import psycopg2

connection_params = {
    "dbname": "masterdb",
    "user": "postgresadmin",
    "password": "admin123",
    "host": "localhost",
    "port": "5001",
}

# ATOMICITY
def atomicity_example(cursor, content_id, title, genre, duration, content_file_reference, view_count):
    try:
        cursor.execute("BEGIN;")  # Begin a transaction
        cursor.execute("INSERT INTO content_repository (content_id, title, genre, duration, content_file_reference, view_count) VALUES "+
                       "(%s, %s, %s, %s, %s, %s) RETURNING content_id;", (content_id, title, genre, duration, content_file_reference, view_count))
        content_id = cursor.fetchone()[0]
        cursor.execute("COMMIT;")  # Commit the transaction
    except Exception as e:
        cursor.execute("ROLLBACK;")  # Rollback the transaction on error
        print("ROLLBACK executed.")
        print(f"Transaction failed: {e}")
    else:
        # Display updated content_repository table
        cursor.execute("SELECT * FROM content_repository WHERE content_id = %s;", (content_id,))
        content_info = cursor.fetchone()
        print("New Content Info:", content_info)

#CONSISTENCY
def consistency_example(cursor, user_id, username, email, subscription_status, content_id, view_count):
    try:
        cursor.execute("BEGIN;")  # Begin a transaction
        cursor.execute("INSERT INTO user_profiles (user_id, username, email, subscription_status) VALUES "+
                       "(%s, %s, %s, %s) RETURNING user_id;", (user_id, username, email, subscription_status))
        cursor.execute("INSERT INTO viewing_history (user_id, content_id) VALUES (%s, %s) RETURNING user_id;", (user_id, content_id))
        cursor.execute("UPDATE content_repository SET view_count = %s WHERE content_id = %s;", (view_count, content_id))
        cursor.execute("COMMIT;")  # Commit the transaction
    except Exception as e:
        cursor.execute("ROLLBACK;")  # Rollback the transaction on error
        print(f"Transaction failed: {e}")
    else:
        # Display updated content_repository table
        cursor.execute("SELECT * FROM viewing_history WHERE user_id = %s and content_id = %s;", (user_id, content_id))
        vh = cursor.fetchall()
        print("Added viewing history:", vh)
        cursor.execute("SELECT * FROM content_repository WHERE content_id = %s;", (content_id,))
        content_info = cursor.fetchone()
        print("Updated Content Repository Info:", content_info)

#ISOLATION
def isolation_example(cursor, user_id, new_email):
    try:
        cursor.execute("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;")  # Begin a transaction with isolation level
        cursor.execute("UPDATE user_profiles SET email = %s WHERE user_id = %s;", (new_email, user_id))
        cursor.execute("COMMIT;")  # Commit the transaction
    except Exception as e:
        cursor.execute("ROLLBACK;")  # Rollback the transaction on error
        print(f"Transaction failed: {e}")
    else:
        # Display updated users table
        cursor.execute("SELECT * FROM user_profiles WHERE user_id = %s;", (user_id,))
        user_info = cursor.fetchone()
        print("Updated User Info:", user_info)

#DURABILITY
#Induced an error to simulate a scenario
def durability_example(cursor, user_id, username, email, subscription_status, content_id):
    user_id = None
    try:
        cursor.execute("BEGIN;")  # Begin a transaction
        # Insert new user
        cursor.execute("INSERT INTO user_profiles(user_id, username, email, subscription_status) VALUES (%s, %s, %s, %s)"+
                       "RETURNING user_id;", (user_id, username, email, subscription_status))
        user_id = cursor.fetchone()[0]  # Retrieve the newly created user_id
        print(user_id)
        # Insert new viewing_history
        cursor.execute("INSERT INTO viewing_history (user_id, content_id) VALUES (%s, %s);", (user_id, content_id))
        print("done")
        # Update view_count in content_repository table
        cursor.execute("SELECT view_count FROM content_repository where content_id = %s;", (content_id,))
        view_count = int(cursor.fetchone()[0])+1
        cursor.execute("UPDATE content_repository SET view_count = %s WHERE content_id = %s;", (view_count, content_id))
        cursor.execute("COMMIT;")  # Commit the transaction
    except Exception as e:
        cursor.execute("ROLLBACK;")  # Rollback the transaction on error
        print(f"Transaction failed: {e}")
    else:
        print("Transaction committed successfully.")
        # Simulate a system failure after the transaction
        raise SystemError("Simulated system failure")
    finally:
        # Check the durability of the transaction
        try:
            # Check if the user exists after the simulated system failure
            cursor.execute("SELECT * FROM user_profiles WHERE user_id = %s;", (user_id,))
            user_info = cursor.fetchone()
            if user_info:
                print("Durability Confirmed: User info persisted after failure:", user_info)
            else:
                print("Durability Failed: User info not found after failure")
        except Exception as e:
            print(f"Error checking durability: {e}")

if __name__ == "__main__":
    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()

    print("Atomicity : ")
    atomicity_example(cursor,101,"Gilli","Action","	00:01:29","Gilli",100000)
    print("Consistency : ")
    consistency_example(cursor, 1001, "hari", "hari@email.com", "f", 101, 200)
    print("Isolation : ")
    isolation_example(cursor, 1001, "harish@gmail.com")
    print("DURABILITY : ")
    durability_example(cursor, 1002, "ramya", "ramya@gmail.com", "f", 101)

