import csv
from pymongo import MongoClient

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

if __name__ == "__main__":
    # Set your file paths and collection names here
    csv_file_path = "path/to/csv"
    collectionname = "authentication"
    
    # Insert data from CSV file
    read_csv_and_insert(csv_file_path, "authentication")

    # Find data
    s_key = input("Enter the key to search: ")
    s_value = input("Enter the value to search: ")
    if not s_key or not s_value:
        query = {}
    else:
        query = {s_key: s_value}
    find_data(collection_name=collectionname, query=query)

    # Update data (assuming you have documents to update)
    update_query = {'user_id': '76'}
    new_data = {'user_id': '176'}
    update_data(update_query, new_data, collectionname)

    # Delete data (assuming you have documents to delete)
    delete_query = {'user_id': '177'}
    delete_data(delete_query, collection_name=collectionname)
