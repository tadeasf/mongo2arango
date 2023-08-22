from arango import ArangoClient
from arango_orm import Database, Relation
from mongo2arango.models import Order, Location, OrderLocation
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
# Fetch orders and locations
orders = db.query(Order).all()
locations = db.query(Location).all()

# Prepare the OrderLocation relationships in bulk
order_location_relations = []
for order in orders:
    origin_location_id = f"locations/{order.originLocationId}"
    destination_location_id = f"locations/{order.destinationLocationId}"
    order_location_relations.append(
        OrderLocation(_from=order._id, _to=origin_location_id)
    )
    order_location_relations.append(
        OrderLocation(_from=order._id, _to=destination_location_id)
    )

# Add the OrderLocation relationships in bulk
db.bulk_add(entity_list=order_location_relations)

print("OrderLocation relationships populated successfully!")
