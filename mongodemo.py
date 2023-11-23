import csv
from pymongo import MongoClient
import time
# Connect to MongoDB
db_name = "demodb"
client = MongoClient('localhost', 6001, directConnection=True)
db = client[db_name]

def read_csv_and_insert(file_path, collection_name):
    # Read CSV file and insert data into MongoDB
    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        data = [row for row in reader]

    # Insert data into MongoDB collection
    collection = db[collection_name]
    result = collection.insert_many(data)
    print(f"Inserted {len(result.inserted_ids)} documents.")

def find_data(collection_name, query={}):
    # Find documents in MongoDB collection
    collection = db[collection_name]
    result = collection.find(query)
    print("Data in database:")
    for document in result:
        print(document)

def update_data(query, update_data, collection_name):
    # Update documents in MongoDB collection
    collection = db[collection_name]
    result = collection.update_many(query, {'$set': update_data})
    print(f"Matched {result.matched_count} documents and modified {result.modified_count} documents.")

def delete_data(query, collection_name):
    # Delete documents in MongoDB collection
    collection = db[collection_name]
    result = collection.delete_many(query)
    print(f"Deleted {result.deleted_count} documents.")

def optimized_query(collection_name):
    start_time = time.time()

    pipeline = [
        {
            "$lookup": {
                "from": "geolocation",
                "localField": "user_id",
                "foreignField": "user_id",
                "as": "geolocation_data"
            }
        },
        {
            "$match": {
                "genre_pref": {"$ne": ""}
            }
        },
        {
            "$project": {
                "genres": {"$split": ["$genre_pref", "|"]},
                "geolocation_data.geolocation_id": 1
            }
        },
        {"$unwind": "$genres"},
        {"$unwind": "$geolocation_data"},
        {
            "$group": {
                "_id": {
                    "genre": "$genres",
                    "geolocation_id": "$geolocation_data.geolocation_id"
                },
                "genre_count": {"$sum": 1}
            }
        },
        {
            "$sort": {"genre_count": -1}
        }
    ]

    results = list(collection_name.aggregate(pipeline))

    end_time = time.time()
    print("<><><><><><><><><><><><><><><>")
    print(f"Optimized query execution time: {end_time - start_time} seconds")
    print("<><><><><><><><><><><><><><><>")
    
    return results

# Optimized function
def non_optimized_query(collection_name):
    start_time = time.time()

    pipeline = [
        {
            "$match": {
                "genre_pref": {"$ne": ""}
            }
        },
        {
            "$project": {
                "user_id": 1,
                "genres": {"$split": ["$genre_pref", "|"]}
            }
        },
        {"$unwind": "$genres"},
        {
            "$lookup": {
                "from": "geolocation",
                "localField": "user_id",
                "foreignField": "user_id",
                "as": "geolocation_data"
            }
        },
        {"$unwind": "$geolocation_data"},
        {
            "$group": {
                "_id": {
                    "genre": "$genres",
                    "geolocation_id": "$geolocation_data.geolocation_id"
                },
                "genre_count": {"$sum": 1}
            }
        },
        {
            "$sort": {"genre_count": -1}
        }
    ]

    results = list(collection_name.aggregate(pipeline))

    end_time = time.time()
    print("<><><><><><><><><><><><><><><>")
    print(f"Unoptimized query execution time: {end_time - start_time} seconds")
    print("<><><><><><><><><><><><><><><>")
    
    return results

csv_files = [
    "authentication.csv",
    "billing.csv",
    "content_repository.csv",
    "geolocation.csv",
    "logging.csv",
    "server_locations.csv",
    "streaming_metadata.csv",
    "user_preferences.csv",
    "user_profiles.csv",
    "viewing_history.csv"
]

if __name__ == "__main__":
    # Read CSV and insert into MongoDB
    for csv_file in csv_files:
        delete_data({}, collection_name=csv_file.split(".")[0])
        
    for csv_file in csv_files:
        csv_file_path = f"./datasets/{csv_file}"
        collectionname = csv_file.split(".")[0]
        read_csv_and_insert(csv_file_path, collection_name=collectionname)

    unoptimised_result = non_optimized_query(db["user_preferences"])
    print("Unoptimized result <truncated>: ", unoptimised_result[:5])
    
    optimised_result = optimized_query(db["user_preferences"])
    print("Optimized result <truncated>: ", optimised_result[:5])
    # Find data
    # s_key = input("Enter the key to search: ")
    # s_value = input("Enter the value to search: ")
    # if not s_key or not s_value:
    #     query = {}
    # else:
    #     query = {s_key: s_value}
    # find_data(collection_name=collectionname, query=query)

    # # Update data (assuming you have documents to update)
    # update_query = {'user_id': '76'}
    # new_data = {'user_id': '176'}
    # update_data(update_query, new_data, collection_name=collectionname)

    # # Delete data (assuming you have documents to delete)
    # delete_query = {'user_id': '177'}
    # delete_data(delete_query, collection_name=collectionname)
