from arango import ArangoClient
from arango_orm import Database, Relation
from mongo2arango.models import User, Order, Assignation, UserOrderAssignation
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

# Fetch customers, orders, and assignations
customers = db.query(User).all()
orders = db.query(Order).all()
assignations = db.query(Assignation).all()

# Prepare the UserOrderAssignation relationships in bulk
user_order_assignation_relations = [
    UserOrderAssignation(
        _from=f"users/{customer._key}", _to=f"assignations/{assignation._key}"
    )
    for customer in customers
    for order in orders
    if order.userId == customer._key
    for assignation in assignations
    if assignation.orderId == order._key
]

# Add the UserOrderAssignation relationships in bulk
db.bulk_add(entity_list=user_order_assignation_relations)

print("UserOrderAssignation relationships populated successfully!")
