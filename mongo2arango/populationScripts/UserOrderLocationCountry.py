from arango import ArangoClient
from arango_orm import Database
from mongo2arango.models import User, Order, Location, Country, UserOrderLocationCountry
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

# Fetch users, orders, locations, and countries
users = db.query(User).all()
orders = db.query(Order).all()
locations = db.query(Location).all()
country_keys = {country._key for country in db.query(Country).all()}

# Mapping location keys to country IDs
location_keys = {
    location._key: f"countries/{location.countryId}"
    for location in locations
    if location.countryId in country_keys
}

# Group orders by user
orders_by_user = defaultdict(list)
for order in orders:
    orders_by_user[order.userId].append(order)

# Prepare the UserOrderLocationCountry relationships in bulk
user_order_location_country_relations = []
for user in users:
    user_orders = orders_by_user.get(user._key, [])
    for order in user_orders:
        for loc_id in [order.originLocationId, order.destinationLocationId]:
            if loc_id in location_keys:
                relation = UserOrderLocationCountry(
                    _from=user._id, _to=location_keys[loc_id]
                )
                user_order_location_country_relations.append(relation)

# Add the UserOrderLocationCountry relationships in bulk
db.bulk_add(entity_list=user_order_location_country_relations)

print("UserOrderLocationCountry relationships populated successfully!")
