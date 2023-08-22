from arango import ArangoClient
from arango_orm import Database, Relation
from mongo2arango.models import (
    User,
    Order,
    Assignation,
    CustomerFeedback,
    Location,
    Country,
    Route,
    TravelData,
    CreatedOrder,
)
from dotenv import load_dotenv
import os
import json
from collections import defaultdict

load_dotenv()
ARANGODB_HOST = os.environ.get("ARANGODB_HOST")
ARANGODB_USER = os.environ.get("ARANGODB_USER")
ARANGODB_PW = os.environ.get("ARANGODB_PW")
DB_NAME = os.environ.get("ARANGODB_DB")
# ArangoDB connection
client = ArangoClient(hosts=ARANGODB_HOST)
db_instance = client.db(DB_NAME, username=ARANGODB_USER, password=ARANGODB_PW)
db = Database(db_instance)

# Fetch users and orders
users = db.query(User).all()
orders = db.query(Order).all()

# Create a mapping of userId to orders
orders_by_user_id = defaultdict(list)
for order in orders:
    orders_by_user_id[order.userId].append(order._id)

# Prepare the CreatedOrder relationships in bulk
created_orders = []
for user in users:
    user_id = f"users/{user._key}"  # Transform _key to _id for user
    for order_id in orders_by_user_id[user._key]:
        created_orders.append(CreatedOrder(_from=user_id, _to=order_id))

# Add the CreatedOrder relationships in bulk
db.bulk_add(entity_list=created_orders)

print("CreatedOrder relationships populated successfully!")
