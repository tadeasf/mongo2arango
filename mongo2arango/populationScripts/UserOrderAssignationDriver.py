from arango import ArangoClient
from arango_orm import Database, Relation
from mongo2arango.models import User, Order, Assignation, UserOrderAssignationDriver
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

# Fetch customers, orders, assignations, and drivers
customers = db.query(User).all()
orders = db.query(Order).all()
assignations = db.query(Assignation).all()
drivers = db.query(User).all()  # Assuming drivers are also in the User collection

# Prepare the UserOrderAssignationDriver relationships in bulk
user_order_assignation_driver_relations = [
    UserOrderAssignationDriver(
        _from=f"users/{customer._key}", _to=f"users/{driver._key}"
    )
    for customer in customers
    for order in orders
    if order.userId == customer._key
    for assignation in assignations
    if assignation.orderId == order._key
    for driver in drivers
    if assignation.userId == driver._key
]

# Add the UserOrderAssignationDriver relationships in bulk
db.bulk_add(entity_list=user_order_assignation_driver_relations)

print("UserOrderAssignationDriver relationships populated successfully!")
