from arango import ArangoClient
from arango_orm import Database
from models import (
    User,
    Order,
    UserRole,
    Assignation,
    CustomerFeedback,
    Location,
    Country,
    Route,
    TravelData,
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
test_db = client.db(DB_NAME, username=ARANGODB_USER, password=ARANGODB_PW)
db = Database(test_db)

# Initialize counters and example lists
connection_stats = {
    "UserOrder": {"validated": 0, "failed": 0},
    "UserAssignationOrder": {"validated": 0, "failed": 0},
    "FeedbackOrderUser": {"validated": 0, "failed": 0},
    "OrderLocationCountry": {"validated": 0, "failed": 0},
    "OrderRoute": {"validated": 0, "failed": 0},
    "OrderTravelData": {"validated": 0, "failed": 0},
    "UserRoleUser": {"validated": 0, "failed": 0},
    "CustomerFeedback": {"validated": 0, "failed": 0},
}


# Simulate creating relationships without actually doing it


def process_users_orders(users, orders):
    user_keys = set(user["_key"] for user in users)

    for order in orders:
        if order["userId"] in user_keys:
            connection_stats["UserOrder"]["validated"] += 1
        else:
            connection_stats["UserOrder"]["failed"] += 1


def process_users_assignations_orders(users, assignations, orders):
    user_keys = set(user["_key"] for user in users)
    order_keys = set(order["_key"] for order in orders)

    for assignation in assignations:
        if assignation["userId"] in user_keys and assignation["orderId"] in order_keys:
            connection_stats["UserAssignationOrder"]["validated"] += 1
        else:
            connection_stats["UserAssignationOrder"]["failed"] += 1


def process_feedback_order_user(feedbacks, orders, users):
    order_keys = set(order["_key"] for order in orders)
    user_keys = set(user["_key"] for user in users)

    for feedback in feedbacks:
        if feedback["orderId"] in order_keys and feedback["driverUserId"] in user_keys:
            connection_stats["FeedbackOrderUser"]["validated"] += 1
        else:
            connection_stats["FeedbackOrderUser"]["failed"] += 1


def process_customer_feedback(feedbacks, orders):
    order_keys = set(order["_key"] for order in orders)

    for feedback in feedbacks:
        if feedback["orderId"] in order_keys:
            connection_stats["CustomerFeedback"]["validated"] += 1
        else:
            connection_stats["CustomerFeedback"]["failed"] += 1


def process_order_location_country(orders, locations, countries):
    location_keys = {location["_key"]: location["countryId"] for location in locations}
    country_keys = set(country["_key"] for country in countries)

    for order in orders:
        if (
            order["destinationLocationId"] in location_keys
            or order["originLocationId"] in location_keys
        ):
            if (
                location_keys[order["destinationLocationId"]] in country_keys
                or location_keys[order["originLocationId"]] in country_keys
            ):
                connection_stats["OrderLocationCountry"]["validated"] += 1
            else:
                connection_stats["OrderLocationCountry"]["failed"] += 1


def process_order_route(orders, routes):
    route_keys = set(route["_key"] for route in routes)

    for order in orders:
        if order["routeId"] in route_keys:
            connection_stats["OrderRoute"]["validated"] += 1
        else:
            connection_stats["OrderRoute"]["failed"] += 1


def process_order_travel_data(orders, travel_data_entries):
    travel_data_keys = {
        (td["originId"], td["destinationId"]) for td in travel_data_entries
    }

    for order in orders:
        if (
            order["originLocationId"],
            order["destinationLocationId"],
        ) in travel_data_keys:
            connection_stats["OrderTravelData"]["validated"] += 1
        else:
            connection_stats["OrderTravelData"]["failed"] += 1


def process_user_role_user(user_roles, users):
    user_keys = set(user["_key"] for user in users)

    for user_role in user_roles:
        if user_role["userId"] in user_keys:
            connection_stats["UserRoleUser"]["validated"] += 1
        else:
            connection_stats["UserRoleUser"]["failed"] += 1


def process_data_in_chunks(
    collection, chunk_size=1000, sample_size=None, is_user=False
):
    if is_user and sample_size:  # Only sample for users
        aql = f"FOR doc IN {collection.name} SORT RAND() LIMIT {sample_size} RETURN doc"
        cursor = db.aql.execute(aql)
    else:
        cursor = collection.all()
    all_data = list(cursor)
    for i in range(0, len(all_data), chunk_size):
        yield all_data[i : i + chunk_size]


# Randomly sample a subset of users
sample_size = 1000
aql_query = f"FOR user IN users SORT RAND() LIMIT {sample_size} RETURN user"
cursor = db.aql.execute(aql_query)
sampled_users = list(cursor)

# For each user, find the related entities in other collections
for user in sampled_users:
    related_orders = (
        db.query(Order).filter("userId==@userId", userId=user["_key"]).all()
    )
    for order in related_orders:
        related_assignations = (
            db.query(Assignation).filter("orderId==@orderId", orderId=order._key).all()
        )
        related_feedbacks = (
            db.query(CustomerFeedback)
            .filter("orderId==@orderId", orderId=order._key)
            .all()
        )
        related_locations = (
            db.query(Location)
            .filter(
                "_key IN [@originId, @destinationId]",
                originId=order.originLocationId,
                destinationId=order.destinationLocationId,
            )
            .all()
        )
        related_routes = (
            db.query(Route).filter("_key==@routeId", routeId=order.routeId).all()
        )
        related_travel_data = (
            db.query(TravelData)
            .filter(
                "travelData.originId==@originId AND travelData.destinationId==@destinationId",
                originId=order.originLocationId,
                destinationId=order.destinationLocationId,
            )
            .all()
        )

        # Validate the relationships for these entities
        process_users_orders([user], [order])
        process_users_assignations_orders([user], related_assignations, [order])
        process_feedback_order_user(related_feedbacks, [order], [user])
        process_order_location_country(
            [order], related_locations, []
        )  # Assuming countries are loaded elsewhere
        process_order_route([order], related_routes)
        process_order_travel_data([order], related_travel_data)

# Calculate percentages
total_validated = sum([stats["validated"] for stats in connection_stats.values()])
total_failed = sum([stats["failed"] for stats in connection_stats.values()])

for connection, stats in connection_stats.items():
    stats["%_of_total_validated"] = (stats["validated"] / total_validated) * 100
    stats["%_of_total_failed"] = (stats["failed"] / total_failed) * 100

with open("./logs/connection_stats.json", "w") as f:
    f.write(json.dumps(connection_stats, indent=4))
