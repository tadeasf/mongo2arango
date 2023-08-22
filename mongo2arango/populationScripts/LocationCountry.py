from arango import ArangoClient
from arango_orm import Database, Relation
from mongo2arango.models import Location, Country, LocationCountry
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
# Fetch locations and countries
locations = db.query(Location).all()
countries = db.query(Country).all()

# Prepare the LocationCountry relationships in bulk
location_country_relations = []
for location in locations:
    country_id = f"countries/{location.countryId}"
    location_country_relations.append(
        LocationCountry(_from=location._id, _to=country_id)
    )

# Add the LocationCountry relationships in bulk
db.bulk_add(entity_list=location_country_relations)

print("LocationCountry relationships populated successfully!")
