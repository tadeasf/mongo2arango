from arango import ArangoClient
from arango_orm import Database
from mongo2arango.models import User, UserCountry, Country
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
# Fetch users and their country IDs
users = db.query(User).all()

# Iterate through users and create UserCountry relations
for user in users:
    country_id = user.countryId
    if country_id:
        # Fetch the corresponding country
        country = db.query(Country).by_key(country_id)
        if country:
            # Create the UserCountry relation
            user_country_relation = UserCountry(_from=user._id, _to=country._id)
            db.add(user_country_relation)

print("UserCountry relations populated successfully!")
