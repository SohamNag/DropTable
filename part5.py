import csv
from pymongo import MongoClient
import time
# Connect to MongoDB
db_name = "demodb"
client = MongoClient('localhost', 6001, directConnection=True)
client2 = MongoClient('localhost', 7001, directConnection=True)
db = client[db_name]
db2 = client2[db_name]

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

def read_csv_and_insert(file_path, collection_name):
    # Read CSV file and insert data into MongoDB
    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        data = [row for row in reader]

    # Insert data into MongoDB collection
    if(collection_name == 'content_repository'):
        for movie in data:
            if(movie['genre'] in unique_genres[:5]):
                collection = db[collection_name]
                collection.insert_one(movie)
            else:
                collection = db2[collection_name]
                collection.insert_one(movie)
        return

    collection = db[collection_name]
    result = collection.insert_many(data)
    print(f"Inserted {len(result.inserted_ids)} documents.")

def find_data(collection_name, query={}):
    # Find documents in MongoDB collection

    if(collection_name == 'content_repository'):
        collection = db[collection_name]
        result = collection.find(query)
        print("First searching in Cluster 1...")
        print("Data in database Cluster 1:")
        for document in result:
            print(document)
        print("Now searching in Cluster 2...")
        collection = db2[collection_name]
        result = collection.find(query)
        print("Data in database Cluster 2:")
        for document in result:
            print(document)
        return

    collection = db[collection_name]
    result = collection.find(query)
    print("Data in database:")
    for document in result:
        print(document)

def update_data(query, update_data, collection_name):
    # Update documents in MongoDB collection

    if(collection_name == 'content_repository'):
        print("First searching in Cluster 1 and updating if found...")
        collection = db[collection_name]
        result = collection.update_many(query, {'$set': update_data})
        print(f"Matched {result.matched_count} documents and modified {result.modified_count} documents.")

        print("Now searching in Cluster 2 and updating if found...")
        collection = db2[collection_name]
        result = collection.update_many(query, {'$set': update_data})
        print(f"Matched {result.matched_count} documents and modified {result.modified_count} documents.")
        return

    collection = db[collection_name]
    result = collection.update_many(query, {'$set': update_data})
    print(f"Matched {result.matched_count} documents and modified {result.modified_count} documents.")

def delete_data(query, collection_name):
    # Delete documents in MongoDB collection

    if(collection_name == 'content_repository'):
        print("First searching in Cluster 1 and deleting if found...")
        collection = db[collection_name]
        result = collection.delete_many(query)
        print(f"Deleted {result.deleted_count} documents.")

        print("Now searching in Cluster 2 and deleting if found...")
        collection = db2[collection_name]
        result = collection.delete_many(query)
        print(f"Deleted {result.deleted_count} documents.")
        return
    
    collection = db[collection_name]
    result = collection.delete_many(query)
    print(f"Deleted {result.deleted_count} documents.")

# Optimized function
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

# Non-Optimized function
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

def initial_data_load():
    for csv_file in csv_files:
        delete_data({}, collection_name=csv_file.split(".")[0])
        
    for csv_file in csv_files:
        csv_file_path = f"./datasets/{csv_file}"
        collectionname = csv_file.split(".")[0]
        read_csv_and_insert(csv_file_path, collection_name=collectionname)

def find_document_in_collection():

    print("Collection:\n0. Authentication\n1. Billing\n2. Content Repository\n3. Geolocation\n4. Logging\n5. Server Locations\n6. Streaming Metadata\n7. User Preferences\n8. User Profiles\n9. Viewing History")
    s_coll = int(input("Enter index of collection to search in from the list above(0-9): "))
    collectionname = csv_files[s_coll].split(".")[0]
    s_key = input("Enter the key to search: ")
    s_value = input("Enter the value to search: ")
    if not s_key or not s_value:
        query = {}
    else:
        query = {s_key: s_value}
    find_data(collection_name=collectionname, query=query)

def update_document_in_collection():

    print("Collection:\n0. Authentication\n1. Billing\n2. Content Repository\n3. Geolocation\n4. Logging\n5. Server Locations\n6. Streaming Metadata\n7. User Preferences\n8. User Profiles\n9. Viewing History")
    s_coll = int(input("Enter index of collection to search in from the list above(0-9): "))
    collectionname = csv_files[s_coll].split(".")[0]
    s_key = input("Enter the key to filter a document with: ")
    s_oldvalue = input("Enter the value of the key to filter a document with: ")
    s_keynew = input("Enter the key to update the value of: ")
    s_newvalue = input("Enter the new value: ")
    if not s_key or not s_oldvalue or not s_keynew or not s_newvalue:
        print("No valid input detected. Need a valid key, old value and new value to do an update operation")
        return
    update_query = {s_key: s_oldvalue}
    new_data = {s_keynew: s_newvalue}
    update_data(update_query, new_data, collection_name=collectionname)

def delete_document_in_collection():

    print("Collection:\n0. Authentication\n1. Billing\n2. Content Repository\n3. Geolocation\n4. Logging\n5. Server Locations\n6. Streaming Metadata\n7. User Preferences\n8. User Profiles\n9. Viewing History")
    s_coll = int(input("Enter index of collection to search in from the list above(0-9): "))
    collectionname = csv_files[s_coll].split(".")[0]
    s_key = input("Enter the key to delete the value of: ")
    s_value = input("Enter the value to delete: ")
    if not s_key or not s_value:
        print("No valid input detected. Need a valid key and value to do a delete operation")
        return
    delete_query = {s_key: s_value}
    delete_data(delete_query, collection_name=collectionname)

if __name__ == "__main__":

    ans = 'y'
    while(ans == 'y'):
        print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
        print("")
        print("Choose an option from the following")
        print('1. Initial data load from CSVs')
        print('2. Find document in a collection')
        print('3. Update document in a collection')
        print('4. Delete document in a collection')
        print('5. Exit')
        choice = int(input("Enter choice (1-5): "))
        if(choice == 1):
            initial_data_load()
        elif(choice == 2):
            find_document_in_collection()
        elif(choice == 3):
            update_document_in_collection()
        elif(choice == 4):
            delete_document_in_collection()
        elif(choice == 5):
            ans = 'n'    

    # unoptimised_result = non_optimized_query(db["user_preferences"])
    # print("Unoptimized result <truncated>: ", unoptimised_result)
    
    # optimised_result = optimized_query(db["user_preferences"])
    # print("Optimized result <truncated>: ", optimised_result)    
