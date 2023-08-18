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


def process_users_orders(users, orders):
    for user in users:
        for order in orders:
            if user["_key"] == order["userId"]:
                connection_stats["UserOrder"]["validated"] += 1
            else:
                connection_stats["UserOrder"]["failed"] += 1


def process_users_assignations_orders(users, assignations, orders):
    for user in users:
        for assignation in assignations:
            if user["_key"] == assignation["userId"] and assignation["orderId"] in [
                order["_key"] for order in orders
            ]:
                connection_stats["UserAssignationOrder"]["validated"] += 1
            else:
                connection_stats["UserAssignationOrder"]["failed"] += 1


def process_feedback_order_user(feedbacks, orders, users):
    for feedback in feedbacks:
        for order in orders:
            if feedback["orderId"] == order["_key"]:
                for user in users:
                    if feedback["userId"] == user["_key"]:
                        connection_stats["FeedbackOrderUser"]["validated"] += 1
                    else:
                        connection_stats["FeedbackOrderUser"]["failed"] += 1


def process_order_location_country(orders, locations, countries):
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


def process_order_route(orders, routes):
    for order in orders:
        for route in routes:
            if order["routeId"] == route["_key"]:
                connection_stats["OrderRoute"]["validated"] += 1
            else:
                connection_stats["OrderRoute"]["failed"] += 1


def process_order_travel_data(orders, travel_data_entries):
    for order in orders:
        for travel_data in travel_data_entries:
            if (
                order["originLocationId"] == travel_data["originId"]
                and order["destinationLocationId"] == travel_data["destinationId"]
            ):
                connection_stats["OrderTravelData"]["validated"] += 1
            else:
                connection_stats["OrderTravelData"]["failed"] += 1


def process_user_role_user(user_roles, users):
    for user_role in user_roles:
        for user in users:
            if user_role["userId"] == user["_key"]:
                connection_stats["UserRoleUser"]["validated"] += 1
            else:
                connection_stats["UserRoleUser"]["failed"] += 1


def process_data_in_chunks(collection, chunk_size=1000):
    cursor = collection.all()
    all_data = list(cursor)
    for i in range(0, len(all_data), chunk_size):
        yield all_data[i : i + chunk_size]


# Process data in chunks
for users_chunk in process_data_in_chunks(db.collection(User.__collection__)):
    for orders_chunk in process_data_in_chunks(db.collection(Order.__collection__)):
        process_users_orders(users_chunk, orders_chunk)

        for assignations_chunk in process_data_in_chunks(
            db.collection(Assignation.__collection__)
        ):
            process_users_assignations_orders(
                users_chunk, assignations_chunk, orders_chunk
            )

        for feedbacks_chunk in process_data_in_chunks(
            db.collection(CustomerFeedback.__collection__)
        ):
            process_feedback_order_user(feedbacks_chunk, orders_chunk, users_chunk)

        for locations_chunk in process_data_in_chunks(
            db.collection(Location.__collection__)
        ):
            for countries_chunk in process_data_in_chunks(
                db.collection(Country.__collection__)
            ):
                process_order_location_country(
                    orders_chunk, locations_chunk, countries_chunk
                )

        for routes_chunk in process_data_in_chunks(db.collection(Route.__collection__)):
            process_order_route(orders_chunk, routes_chunk)

        for travel_data_chunk in process_data_in_chunks(
            db.collection(TravelData.__collection__)
        ):
            process_order_travel_data(orders_chunk, travel_data_chunk)

        for user_roles_chunk in process_data_in_chunks(
            db.collection(UserRole.__collection__)
        ):
            process_user_role_user(user_roles_chunk, users_chunk)

# Calculate percentages
total_validated = sum([stats["validated"] for stats in connection_stats.values()])
total_failed = sum([stats["failed"] for stats in connection_stats.values()])

for connection, stats in connection_stats.items():
    stats["%_of_total_validated"] = (stats["validated"] / total_validated) * 100
    stats["%_of_total_failed"] = (stats["failed"] / total_failed) * 100

with open("/logs/connection_stats.json", "w") as f:
    f.write(json.dumps(connection_stats, indent=4))
