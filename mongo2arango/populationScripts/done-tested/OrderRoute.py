from arango import ArangoClient
from arango_orm import Database, Relation
from mongo2arango.models import Order, Route, OrderRoute
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
# Fetch orders and routes
orders = db.query(Order).all()
routes = db.query(Route).all()

# Prepare the OrderRoute relationships in bulk
order_route_relations = []
for order in orders:
    route_id = f"routes/{order.routeId}"
    order_route_relations.append(OrderRoute(_from=order._id, _to=route_id))

# Add the OrderRoute relationships in bulk
db.bulk_add(entity_list=order_route_relations)

print("OrderRoute relationships populated successfully!")
