from arango import ArangoClient
from models import (
    User,
    Order,
    Assignation,
    CustomerFeedback,
    Location,
    Country,
    Route,
    TravelData,
    UserRole,
)
from dotenv import load_dotenv
import os
import json

load_dotenv()
ARANGODB_HOST = os.environ.get("ARANGODB_HOST")
ARANGODB_USER = os.environ.get("ARANGODB_USER")
ARANGODB_PW = os.environ.get("ARANGODB_PW")
DB_NAME = os.environ.get("ARANGODB_DB")

# ArangoDB connection
client = ArangoClient(hosts=ARANGODB_HOST)
db = client.db(DB_NAME, username=ARANGODB_USER, password=ARANGODB_PW)

# Initialize counters and example lists
connection_stats = {
    "UserOrder": {
        "validated": 0,
        "failed": 0,
    },
    "UserAssignationOrder": {
        "validated": 0,
        "failed": 0,
    },
    "FeedbackOrderUser": {
        "validated": 0,
        "failed": 0,
    },
    "OrderLocationCountry": {
        "validated": 0,
        "failed": 0,
    },
    "OrderRoute": {
        "validated": 0,
        "failed": 0,
    },
    "OrderTravelData": {
        "validated": 0,
        "failed": 0,
    },
    "UserRoleUser": {
        "validated": 0,
        "failed": 0,
    },
}

# Simulate creating relationships without actually doing it

# UserOrder relationship
users = db.collection(User.__collection__).all()
orders = db.collection(Order.__collection__).all()
for user in users:
    for order in orders:
        if user["_key"] == order["userId"]:
            connection_stats["UserOrder"]["validated"] += 1
        else:
            connection_stats["UserOrder"]["failed"] += 1

# UserAssignationOrder relationship
assignations = db.collection(Assignation.__collection__).all()
for user in users:
    for assignation in assignations:
        if user["_key"] == assignation["userId"]:
            for order in orders:
                if assignation["orderId"] == order["_key"]:
                    connection_stats["UserAssignationOrder"]["validated"] += 1
                else:
                    connection_stats["UserAssignationOrder"]["failed"] += 1

# FeedbackOrderUser relationship
feedbacks = db.collection(CustomerFeedback.__collection__).all()
for feedback in feedbacks:
    for order in orders:
        if feedback["orderId"] == order["_key"]:
            for user in users:
                if feedback["userId"] == user["_key"]:
                    connection_stats["FeedbackOrderUser"]["validated"] += 1
                else:
                    connection_stats["FeedbackOrderUser"]["failed"] += 1

# OrderLocationCountry relationship
locations = db.collection(Location.__collection__).all()
countries = db.collection(Country.__collection__).all()
for order in orders:
    for location in locations:
        if (
            order["destinationLocationId"] == location["_key"]
            or order["originLocationId"] == location["_key"]
        ):
            for country in countries:
                if location["countryId"] == country["_key"]:
                    connection_stats["OrderLocationCountry"]["validated"] += 1
                else:
                    connection_stats["OrderLocationCountry"]["failed"] += 1

# OrderRoute relationship
routes = db.collection(Route.__collection__).all()
for order in orders:
    for route in routes:
        if order["routeId"] == route["_key"]:
            connection_stats["OrderRoute"]["validated"] += 1
        else:
            connection_stats["OrderRoute"]["failed"] += 1

# OrderTravelData relationship
travel_data_entries = db.collection(TravelData.__collection__).all()
for order in orders:
    for travel_data in travel_data_entries:
        if (
            order["originLocationId"] == travel_data["originId"]
            and order["destinationLocationId"] == travel_data["destinationId"]
        ):
            connection_stats["OrderTravelData"]["validated"] += 1
        else:
            connection_stats["OrderTravelData"]["failed"] += 1

# UserRoleUser relationship
user_roles = db.collection(UserRole.__collection__).all()
for user_role in user_roles:
    for user in users:
        if user_role["userId"] == user["_key"]:
            connection_stats["UserRoleUser"]["validated"] += 1
        else:
            connection_stats["UserRoleUser"]["failed"] += 1

# Calculate percentages
total_validated = sum([stats["validated"] for stats in connection_stats.values()])
total_failed = sum([stats["failed"] for stats in connection_stats.values()])

for connection, stats in connection_stats.items():
    stats["%_of_total_validated"] = (stats["validated"] / total_validated) * 100
    stats["%_of_total_failed"] = (stats["failed"] / total_failed) * 100

with open("/logs/connection_stats.json", "w") as f:
    f.write(json.dumps(connection_stats, indent=4))
