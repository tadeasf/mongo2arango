from arango import ArangoClient
from arango_orm import Database, Relation
from mongo2arango.models import Assignation, Order, AssignationOrder
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

# Fetch assignations and orders
assignations = db.query(Assignation).all()
orders = db.query(Order).all()

# Create a mapping between order keys and their corresponding _id fields
order_mapping = {order._key: order._id for order in orders}

# Prepare the AssignationOrder relationships in bulk
assignation_order_relations = [
    AssignationOrder(_from=assignation._id, _to=order_mapping[assignation.orderId])
    for assignation in assignations
    if assignation.orderId in order_mapping
]

# Add the AssignationOrder relationships in bulk
db.bulk_add(entity_list=assignation_order_relations)

print("AssignationOrder relationships populated successfully!")
