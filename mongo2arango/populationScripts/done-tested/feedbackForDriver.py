from arango import ArangoClient
from arango_orm import Database, Relation
from mongo2arango.models import (
    User,
    CustomerFeedback,
    FeedbackForDriver,
)
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

# Fetch customer feedback and users (drivers)
feedbacks = db.query(CustomerFeedback).all()
users = db.query(User).all()

# Create a mapping of driverUserId to feedbacks
feedbacks_by_driver_user_id = defaultdict(list)
for feedback in feedbacks:
    feedbacks_by_driver_user_id[feedback.driverUserId].append(feedback._id)

# Prepare the FeedbackForDriver relationships in bulk
feedback_for_driver_relations = []
for user in users:
    user_id = f"users/{user._key}"  # Transform _key to _id for user
    for feedback_id in feedbacks_by_driver_user_id[user._key]:
        feedback_for_driver_relations.append(
            FeedbackForDriver(_from=user_id, _to=feedback_id)
        )

# Add the FeedbackForDriver relationships in bulk
db.bulk_add(entity_list=feedback_for_driver_relations)

print("FeedbackForDriver relationships populated successfully!")
