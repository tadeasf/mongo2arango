from arango import ArangoClient
from arango_orm import Database
from mongo2arango.models import User, Order, Assignation, UserOrderAssignationDriver
from dotenv import load_dotenv
import os
from collections import defaultdict

load_dotenv()
ARANGODB_HOST = os.getenv("ARANGODB_HOST")
ARANGODB_USER = os.getenv("ARANGODB_USER")
ARANGODB_PW = os.getenv("ARANGODB_PW")
DB_NAME = os.getenv("ARANGODB_DB")

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
assignations_by_order_key = {a.orderId: a._key for a in assignations}
assignations_by_user_id = {a._key: f"users/{a.userId}" for a in assignations}

for order in orders:
    orders_by_user[order.userId].append(order._key)

# Prepare the UserOrderAssignationDriver relationships in bulk
user_order_assignation_driver_relations = [
    UserOrderAssignationDriver(
        _from=f"users/{user._key}", _to=assignations_by_user_id[assignation_key]
    )
    for user in users
    for order_key in orders_by_user[user._key]
    for assignation_key in [assignations_by_order_key.get(order_key)]
    if assignation_key in assignations_by_user_id
]

# Add the UserOrderAssignationDriver relationships in bulk
db.bulk_add(entity_list=user_order_assignation_driver_relations)

print("UserOrderAssignationDriver relationships populated successfully!")
