from arango import ArangoClient
from arango_orm import Database
from mongo2arango.models import User, Order, Assignation, UserOrderAssignation
from dotenv import load_dotenv
import os
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

# Fetch customers, orders, and assignations
users = db.query(User).all()
orders = db.query(Order).all()
assignations = db.query(Assignation).all()

# Create mappings
orders_by_user = defaultdict(list)
assignations_by_order = {a.orderId: a._key for a in assignations}

for order in orders:
    orders_by_user[order.userId].append(order._key)

# Prepare the UserOrderAssignation relationships in bulk
user_order_assignation_relations = [
    UserOrderAssignation(
        _from=f"users/{user._key}", _to=f"assignations/{assignations_by_order[order]}"
    )
    for user in users
    for order in orders_by_user[user._key]
]

# Add the UserOrderAssignation relationships in bulk
db.bulk_add(entity_list=user_order_assignation_relations)

print("UserOrderAssignation relationships populated successfully!")
