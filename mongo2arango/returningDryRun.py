from arango import ArangoClient
from arango_orm import Database  # Import Database from arango_orm
from mongo2arango.models import User, Order, ReturningCustomer
from collections import defaultdict
from datetime import datetime
import json
from dotenv import load_dotenv
import os
import time

# Load the environment variables
load_dotenv()
ARANGODB_HOST = os.environ.get("ARANGODB_HOST")
ARANGODB_USER = os.environ.get("ARANGODB_USER")
ARANGODB_PW = os.environ.get("ARANGODB_PW")
DB_NAME = os.environ.get("ARANGODB_DB")

# ArangoDB connection
client = ArangoClient(hosts=ARANGODB_HOST)
db_instance = client.db(DB_NAME, username=ARANGODB_USER, password=ARANGODB_PW)
db = Database(db_instance)  # Create Database object

# Fetch a limited number of users and orders
start = time.time()
users = db.query(User).limit(50000).all()  # Query using the Database object
orders = db.query(Order).limit(150000).all()
end = time.time()
print(f"Querying users and orders took {end - start} seconds.")

# Group orders by user
start = time.time()
orders_by_user = defaultdict(list)
for order in orders:
    orders_by_user[order.userId].append(order)
end = time.time()
print(f"Grouping orders by user took {end - start} seconds.")


# Function to determine the season
def determine_season(date):
    return "season" if date.month in [4, 5, 6, 7, 8, 9] else "off-season"


# Initialize stats
connection_stats = {"ReturningCustomer": {"validated": 0, "failed": 0}}

# Process the ReturningCustomer relationships
start = time.time()
for user in users:
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
            connection_stats["ReturningCustomer"]["validated"] += 1
            last_season = current_season
        else:
            connection_stats["ReturningCustomer"]["failed"] += 1
end = time.time()
print(f"Processing the ReturningCustomer relationships took {end - start} seconds.")

print("Dry run completed.")
print("ReturningCustomer relationships:")
print(f"  Validated: {connection_stats['ReturningCustomer']['validated']}")
print(f"  Failed: {connection_stats['ReturningCustomer']['failed']}")
