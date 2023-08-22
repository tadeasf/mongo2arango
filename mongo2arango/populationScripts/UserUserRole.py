from arango import ArangoClient
from arango_orm import Database
from mongo2arango.models import User, UserRole, UserUserRole
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

# Fetch users and user roles
users = db.query(User).all()
user_roles = db.query(UserRole).all()

# Create a mapping between user keys and their corresponding _id fields
user_mapping = {user._key: user._id for user in users}

# Prepare the UserUserRole relationships in bulk
user_user_role_relations = [
    UserUserRole(_from=user_role._id, _to=user_mapping[user_role.userId])
    for user_role in user_roles
    if user_role.userId in user_mapping
]

# Add the UserUserRole relationships in bulk
db.bulk_add(entity_list=user_user_role_relations)

print("UserUserRole relationships populated successfully!")
