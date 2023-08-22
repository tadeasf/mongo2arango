from arango import ArangoClient
from arango_orm import Database, Relation
from mongo2arango.models import User, Order, Location, Country, UserOrderLocationCountry
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

# Fetch users, orders, locations, and countries
users = db.query(User).all()
orders = db.query(Order).all()
locations = db.query(Location).all()
countries = db.query(Country).all()

# Mapping location keys to country IDs
location_keys = {location._key: location.countryId for location in locations}

# Prepare the UserOrderLocationCountry relationships in bulk
user_order_location_country_relations = [
    UserOrderLocationCountry(_from=user._id, _to=location_keys[order.originLocationId])
    for user in users
    for order in orders
    if order.userId == user._key
    if (
        order.originLocationId in location_keys
        and location_keys[order.originLocationId] in location_keys.values()
    )
]

user_order_location_country_relations += [
    UserOrderLocationCountry(
        _from=user._id, _to=location_keys[order.destinationLocationId]
    )
    for user in users
    for order in orders
    if order.userId == user._key
    if (
        order.destinationLocationId in location_keys
        and location_keys[order.destinationLocationId] in location_keys.values()
    )
]

# Add the UserOrderLocationCountry relationships in bulk
db.bulk_add(entity_list=user_order_location_country_relations)

print("UserOrderLocationCountry relationships populated successfully!")
