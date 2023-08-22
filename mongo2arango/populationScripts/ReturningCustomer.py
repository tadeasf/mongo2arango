from arango import ArangoClient
from arango_orm import Database
from mongo2arango.models import User, Order, ReturningCustomer
from collections import defaultdict
from datetime import datetime
import os
import time
from dotenv import load_dotenv

load_dotenv()
ARANGODB_HOST = os.environ.get("ARANGODB_HOST")
ARANGODB_USER = os.environ.get("ARANGODB_USER")
ARANGODB_PW = os.environ.get("ARANGODB_PW")
DB_NAME = os.environ.get("ARANGODB_DB")

# ArangoDB connection
client = ArangoClient(hosts=ARANGODB_HOST)
db_instance = client.db(DB_NAME, username=ARANGODB_USER, password=ARANGODB_PW)
db = Database(db_instance)


# Function to determine the season
def determine_season(date):
    return "season" if date.month in [4, 5, 6, 7, 8, 9] else "off-season"


BATCH_SIZE = 5000  # Size of each batch


def process_users(users_batch):
    # Fetch orders for users in this batch
    user_keys = [user._key for user in users_batch]

    # Create a dictionary to store orders by userId
    orders_by_user = defaultdict(list)

    # Iterate through the user_keys and query orders for each user
    for user_key in user_keys:
        orders = db.query(Order).filter("userId == @user_key", user_key=user_key).all()
        orders_by_user[user_key] = orders

    relationships = []
    for user in users_batch:
        user_orders = sorted(orders_by_user[user._key], key=lambda x: x.createdAt)
        last_season = None
        for i in range(len(user_orders) - 1):
            current_order = user_orders[i]
            next_order = user_orders[i + 1]
            current_season = determine_season(current_order.createdAt)
            next_season = determine_season(next_order.createdAt)

            if current_season != next_season and (
                last_season is None or current_season != last_season
            ):
                relationships.append(
                    ReturningCustomer(_from=user._key, _to=next_order._key)
                )
                last_season = current_season

    # Add the relationships in bulk
    db.bulk_add(entity_list=relationships)

    return len(relationships)


# Fetch users and orders
users = db.query(User).all()
orders = db.query(Order).all()

# Create batches of users
user_batches = [users[i : i + BATCH_SIZE] for i in range(0, len(users), BATCH_SIZE)]

# Process the user batches sequentially
start_time = time.time()
total_relationships = 0
for user_batch in user_batches:
    total_relationships += process_users(user_batch)
end_time = time.time()
print(f"Total processing time: {end_time - start_time} seconds.")

# Print the total number of relationships created
print("Relationships populated successfully!")
print(f"Total relationships created: {total_relationships}")
