from arango import ArangoClient
from arango_orm import Database, Relation
from mongo2arango.models import User, UserRole, UserUserRole
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
# Fetch users and user roles
users = db.query(User).all()
user_roles = db.query(UserRole).all()

# Prepare the UserUserRole relationships in bulk
user_user_role_relations = [
    UserUserRole(_from=user._id, _to=f"user_roles/{user.roleId}") for user in users
]

# Add the UserUserRole relationships in bulk
db.bulk_add(entity_list=user_user_role_relations)

print("UserUserRole relationships populated successfully!")
