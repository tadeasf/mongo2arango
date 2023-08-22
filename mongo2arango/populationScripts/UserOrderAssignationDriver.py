from arango import ArangoClient
from arango_orm import Database
from mongo2arango.models import User, Order, Assignation, UserOrderAssignationDriver
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

# Fetch customers, orders, assignations, and drivers
users = db.query(User).all()
orders = db.query(Order).all()
assignations = db.query(Assignation).all()

# Create mappings
orders_by_user = defaultdict(list)
assignations_by_order = defaultdict(list)
drivers_by_assignation = {a.userId: f"users/{a.userId}" for a in assignations}

for order in orders:
    orders_by_user[order.userId].append(order._key)

for assignation in assignations:
    assignations_by_order[assignation.orderId].append(assignation._key)

# Prepare the UserOrderAssignationDriver relationships in bulk
user_order_assignation_driver_relations = [
    UserOrderAssignationDriver(
        _from=f"users/{user._key}", _to=drivers_by_assignation[assignation]
    )
    for user in users
    for order in orders_by_user[user._key]
    for assignation in assignations_by_order[order]
]

# Add the UserOrderAssignationDriver relationships in bulk
db.bulk_add(entity_list=user_order_assignation_driver_relations)

print("UserOrderAssignationDriver relationships populated successfully!")
