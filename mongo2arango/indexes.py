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
    if isinstance(field, list):  # Check if it's a compound index
        if index_type == "hash":
            collection.add_hash_index(fields=field)
        elif index_type == "skiplist":
            collection.add_skiplist_index(fields=field)
    else:
        if index_type == "hash":
            collection.add_hash_index(fields=[field])
        elif index_type == "skiplist":
            collection.add_skiplist_index(fields=[field])
    print(f"Index created on {field} in {collection_name} using {index_type} index.")


# Create indexes
# create_index("users", "_id")
create_index("users", "_key")
create_index("users", "email")
create_index("users", "countryId")
create_index("users", "createdAt")
# create_index("orders", "_id")
create_index("orders", "_key")
create_index("orders", "userId")
create_index("orders", "createdAt")
create_index("orders", "confirmedAt")
create_index("orders", "cancelledAt")
create_index("orders", "acceptedAt")
create_index("orders", "declinedAt")
create_index("orders", "draftedAt")
create_index("orders", "departureAt")
create_index("orders", "originLocationId")
create_index("orders", "destinationLocationId")
create_index("orders", "userId")
# create_index("assignations", "_id")
create_index("assignations", "_key")
create_index("assignations", "userId")
create_index("assignations", "orderId")
create_index("assignations", "createdAt")
create_index("assignations", "orderDepartureAt")
# create_index("customerFeedbacks", "_id")
create_index("customerFeedbacks", "_key")
create_index("customerFeedbacks", "orderId")
create_index("customerFeedbacks", "driverUserId")
# create_index("locations", "_id")
create_index("locations", "_key")
create_index("locations", "countryId")
create_index("locations", "name")
# create_index("countries", "_id")
create_index("countries", "_key")
create_index("countries", "englishName")
# create_index("routes", "_id")
create_index("routes", "_key")
create_index("routes", "originLocationId")
create_index("routes", "destinationLocationId")
# create_index("travelData", "_id")
create_index("travelData", "_key")
create_index("travelData", "originId")
create_index("travelData", "destinationId")
# create_index("userRoles", "_id")
create_index("userRoles", "_key")
create_index("userRoles", "userId")
# Indexes for relationships
relationship_collections = [
    "created_order",
    "feedback_for_driver",
    "order_location",
    "location_country",
    "order_route",
    "order_travel_data",
    "user_user_role",
    "user_order_location_country",
    "user_assignation",
    "assignation_order",
    "user_order_assignation",
    "user_order_assignation_driver",
]

for rel in relationship_collections:
    create_index(rel, "_from")
    create_index(rel, "_to")

# Compound Indexes
create_index("orders", ["userId", "createdAt"])

# Skiplist Indexes for range queries
create_index("orders", "createdAt", index_type="skiplist")
create_index("orders", "confirmedAt", index_type="skiplist")
create_index("orders", "cancelledAt", index_type="skiplist")
create_index("orders", "acceptedAt", index_type="skiplist")
create_index("orders", "declinedAt", index_type="skiplist")
create_index("orders", "draftedAt", index_type="skiplist")
create_index("orders", "departureAt", index_type="skiplist")
create_index("assignations", "createdAt", index_type="skiplist")
create_index("assignations", "orderDepartureAt", index_type="skiplist")

print("All indexes created successfully!")
print("All indexes created successfully!")
