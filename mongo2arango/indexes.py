from arango import ArangoClient
from dotenv import load_dotenv
import os

load_dotenv()
ARANGODB_HOST = os.environ.get("ARANGODB_HOST")
ARANGODB_USER = os.environ.get("ARANGODB_USER")
ARANGODB_PW = os.environ.get("ARANGODB_PW")
DB_NAME = os.environ.get("ARANGODB_DB")

# ArangoDB connection
client = ArangoClient(hosts=ARANGODB_HOST)
db = client.db(DB_NAME, username=ARANGODB_USER, password=ARANGODB_PW)


def create_index(collection_name, field, index_type="hash"):
    """
    Create an index on a specified field for a collection.
    """
    collection = db.collection(collection_name)
    if index_type == "hash":
        collection.add_hash_index(fields=[field])
    elif index_type == "skiplist":
        collection.add_skiplist_index(fields=[field])
    print(f"Index created on {field} in {collection_name} using {index_type} index.")


# Create indexes
create_index("users", "_key")
create_index("orders", "_key")
create_index("orders", "userId")
create_index("assignations", "userId")
create_index("assignations", "orderId")
create_index("customerFeedbacks", "orderId")
create_index("customerFeedbacks", "driverUserId")
create_index("locations", "_key")
create_index("locations", "countryId")
create_index("countries", "_key")
create_index("routes", "_key")
create_index("travelData", "originId")
create_index("travelData", "destinationId")
create_index("userRoles", "userId")

print("All indexes created successfully!")
