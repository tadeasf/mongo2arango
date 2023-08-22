from arango import ArangoClient
from arango_orm import Database
from models import (
    CreatedOrder,
    FeedbackForDriver,
    OrderLocation,
    LocationCountry,
    OrderRoute,
    OrderTravelData,
    UserUserRole,
    UserOrderLocationCountry,
    UserAssignation,
    AssignationOrder,
    UserOrderAssignation,
    UserOrderAssignationDriver,
    ReturningCustomer,
    UserCountry,
)
from dotenv import load_dotenv
import os

load_dotenv()
ARANGODB_HOST = os.environ.get("ARANGODB_HOST")
ARANGODB_USER = os.environ.get("ARANGODB_USER")
ARANGODB_PW = os.environ.get("ARANGODB_PW")
DB_NAME = os.environ.get("ARANGODB_DB")

# ArangoDB connection
client = ArangoClient(hosts=ARANGODB_HOST)
db_instance = client.db(DB_NAME, username=ARANGODB_USER, password=ARANGODB_PW)
db = Database(db_instance)

# Edge Collections to Initialize
edge_collections = [
    CreatedOrder,
    FeedbackForDriver,
    OrderLocation,
    LocationCountry,
    OrderRoute,
    OrderTravelData,
    UserUserRole,
    UserOrderLocationCountry,
    UserAssignation,
    AssignationOrder,
    UserOrderAssignation,
    UserOrderAssignationDriver,
    ReturningCustomer,
    UserCountry,
]

for edge_collection in edge_collections:
    if not db.has_collection(edge_collection.__collection__):
        db.create_collection(edge_collection, edge=True)

print("Edge collections initialized successfully!")
