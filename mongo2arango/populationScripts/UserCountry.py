from arango import ArangoClient
from arango_orm import Database
from mongo2arango.models import User, Country, UserCountry
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

# Fetch users and countries
users = db.query(User).all()
countries = db.query(Country).all()

# Create a mapping between country keys and their corresponding _id fields
country_mapping = {country._key: country._id for country in countries}

# Prepare the UserCountry relationships in bulk
user_country_relations = [
    UserCountry(_from=user._id, _to=country_mapping[user.countryId])
    for user in users
    if user.countryId in country_mapping
]

# Add the UserCountry relationships in bulk
db.bulk_add(entity_list=user_country_relations)

print("UserCountry relations populated successfully!")
