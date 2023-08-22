from arango import ArangoClient
from arango_orm import Database
from mongo2arango.models import Order, TravelData, OrderTravelData
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

# Fetch orders and travel data
orders = db.query(Order).all()
travel_data_entries = db.query(TravelData).all()

# Create a set of travel data keys using origin and destination IDs
travel_data_keys = {
    (td.originId, td.destinationId): td._id for td in travel_data_entries
}

# Prepare the OrderTravelData relationships in bulk
order_travel_data_relations = []
for order in orders:
    order_key = (order.originLocationId, order.destinationLocationId)
    if order_key in travel_data_keys:
        travel_data_id = f"travel_data/{travel_data_keys[order_key]}"
        order_travel_data_relations.append(
            OrderTravelData(_from=order._id, _to=travel_data_id)
        )

# Add the OrderTravelData relationships in bulk
db.bulk_add(entity_list=order_travel_data_relations)

print("OrderTravelData relationships populated successfully!")
