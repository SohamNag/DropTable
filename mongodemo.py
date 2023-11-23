import csv
from pymongo import MongoClient

# MongoDB connection details
mongo_host = "localhost"
mongo_port = "6001"
mongo_user = "admin"
mongo_password = "adminpassword"
mongo_db = "mydb"
mongo_collection = "mycollection"

# Connect to MongoDB
client = MongoClient(f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/{mongo_db}?replicaSet=rs0")
print(client)
db = client[mongo_db]
collection = db[mongo_collection]

def read_csv_and_insert(file_path):
    # Read CSV file and insert data into MongoDB
    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        data = [row for row in reader]

    
    # Insert data into MongoDB collection
    result = collection.insert_many(data)
    print(f"Inserted {len(result.inserted_ids)} documents.")

def find_data(query={}):
    # Find documents in MongoDB collection
    result = collection.find(query)
    for document in result:
        print(document)

def update_data(query, update_data):
    # Update documents in MongoDB collection
    result = collection.update_many(query, {'$set': update_data})
    print(f"Matched {result.matched_count} documents and modified {result.modified_count} documents.")

def delete_data(query):
    # Delete documents in MongoDB collection
    result = collection.delete_many(query)
    print(f"Deleted {result.deleted_count} documents.")

if __name__ == "__main__":
    # Example usage
    csv_file_path = "c:/Users/soham/Desktop/Project_512/DropTable/datasets/authentication.csv"
    
    # Insert data from CSV file
    read_csv_and_insert(csv_file_path)

    # # Find data
    # print("Data in MongoDB:")
    # find_data()

    # # Update data (assuming you have documents to update)
    # update_query = {'field_to_update': 'value_to_match'}
    # update_data = {'field_to_update': 'new_value'}
    # update_data(update_query, update_data)

    # # Find data after update
    # print("Data in MongoDB after update:")
    # find_data()

    # # Delete data (assuming you have documents to delete)
    # delete_query = {'field_to_delete': 'value_to_match'}
    # delete_data(delete_query)

    # # Find data after delete
    # print("Data in MongoDB after delete:")
    # find_data()
