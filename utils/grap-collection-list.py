from pymongo import MongoClient
import os
from dotenv import load_dotenv
import json

# Load environment variables from .env file
load_dotenv()

# MongoDB connection details from .env
MONGO_URI = os.getenv("MONGODB_URI")  # Use the connection string directly


def get_collections():
    """
    Retrieve a list of collections from the MongoDB database.
    """
    client = MongoClient(MONGO_URI)
    db = (
        client.get_default_database()
    )  # Get the default database from the connection string
    # sort the collections by name
    collections = sorted([collection for collection in db.list_collection_names()])
    return collections


if __name__ == "__main__":
    collections = get_collections()
    print(collections)
    # create a json and dump it in
    with open("collections.json", "w") as f:
        json.dump(collections, f, indent=4)
