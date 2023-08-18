from arango import ArangoClient
from arango_orm import Database
from models import (
    User,
    Order,
    Assignation,
    CustomerFeedback,
    Location,
    Country,
    Route,
    TravelData,
    CreatedOrder,
)
from dotenv import load_dotenv
import os
import json
import logging
from arango.exceptions import ArangoError

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("./logs/wetRun.log"), logging.StreamHandler()],
)
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
    "UserOrderLocationCountry": {"validated": 0, "failed": 0},
    "UserAssignationOrder": {"validated": 0, "failed": 0},
    "UserOrderAssignation": {"validated": 0, "failed": 0},
    "UserOrderAssignationDriver": {"validated": 0, "failed": 0},
}

# Simulate creating relationships without actually doing it


def process_users_orders(users, orders):
    for user in users:
        for order in orders:
            if order.userId == user._key:
                connection_stats["UserOrder"]["validated"] += 1
                user_id = f"users/{user._key}"  # Transform _key to _id for user
                create_relationship(CreatedOrder, user_id, order.userId)
            else:
                connection_stats["UserOrder"]["failed"] += 1


def process_customer_feedback_order_user(customerFeedbacks, orders, users):
    order_keys = set(order._key for order in orders)
    user_keys = set(user._key for user in users)

    for feedback in customerFeedbacks:
        if feedback.orderId in order_keys and feedback.driverUserId in user_keys:
            connection_stats["FeedbackOrderUser"]["validated"] += 1
        else:
            connection_stats["FeedbackOrderUser"]["failed"] += 1


def process_order_location_country(orders, locations, countries):
    location_keys = {location._key: location.countryId for location in locations}
    country_keys = set(country._key for country in countries)

    for order in orders:
        if (
            order.originLocationId in location_keys
            and location_keys[order.originLocationId] in country_keys
        ):
            connection_stats["OrderLocationCountry"]["validated"] += 1
            create_relationship(
                "OrderLocationCountry",
                order._key,
                location_keys[order.originLocationId],
            )
        elif (
            order.destinationLocationId in location_keys
            and location_keys[order.destinationLocationId] in country_keys
        ):
            connection_stats["OrderLocationCountry"]["validated"] += 1
            create_relationship(
                "OrderLocationCountry",
                order._key,
                location_keys[order.destinationLocationId],
            )
        else:
            connection_stats["OrderLocationCountry"]["failed"] += 1


def process_order_route(orders, routes):
    route_keys = set(route._key for route in routes)  # Use dot notation

    for order in orders:
        if order.routeId in route_keys:  # Use dot notation
            connection_stats["OrderRoute"]["validated"] += 1
        else:
            connection_stats["OrderRoute"]["failed"] += 1


def process_order_travel_data(orders, travel_data_entries):
    travel_data_keys = {
        (td.originId, td.destinationId)
        for td in travel_data_entries  # Use dot notation
    }

    for order in orders:
        if (
            order.originLocationId,
            order.destinationLocationId,
        ) in travel_data_keys:
            connection_stats["OrderTravelData"]["validated"] += 1
        else:
            connection_stats["OrderTravelData"]["failed"] += 1


def process_user_role_user(user_roles, users):
    user_keys = set(user._key for user in users)

    for user_role in user_roles:
        if user_role.userId in user_keys:  # Use dot notation
            connection_stats["UserRoleUser"]["validated"] += 1
        else:
            connection_stats["UserRoleUser"]["failed"] += 1


def process_user_order_location_country(users, orders, locations, countries):
    user_keys = set(user._key for user in users)
    location_keys = {location._key: location.countryId for location in locations}
    country_keys = set(country._key for country in countries)

    for user in users:
        for order in orders:
            if order.userId == user._key:
                if (
                    order.originLocationId in location_keys
                    and location_keys[order.originLocationId] in country_keys
                ):
                    connection_stats["UserOrderLocationCountry"]["validated"] += 1
                    create_relationship(
                        "UserOrderLocationCountry",
                        user._key,
                        location_keys[order.originLocationId],
                    )
                elif (
                    order.destinationLocationId in location_keys
                    and location_keys[order.destinationLocationId] in country_keys
                ):
                    connection_stats["UserOrderLocationCountry"]["validated"] += 1
                    create_relationship(
                        "UserOrderLocationCountry",
                        user._key,
                        location_keys[order.destinationLocationId],
                    )
                else:
                    connection_stats["UserOrderLocationCountry"]["failed"] += 1


def process_driver_assignation_order(drivers, assignations, orders):
    for driver in drivers:
        for assignation in assignations:
            if assignation.userId == driver._key:
                for order in orders:
                    if assignation.orderId == order._key:
                        connection_stats["UserAssignationOrder"]["validated"] += 1
                        create_relationship(
                            "UserAssignationOrder",
                            f"users/{driver._key}",
                            f"orders/{order._key}",
                        )
                    else:
                        connection_stats["UserAssignationOrder"]["failed"] += 1


def process_customer_order_assignation(customers, orders, assignations):
    for customer in customers:
        for order in orders:
            if order.userId == customer._key:
                for assignation in assignations:
                    if assignation.orderId == order._key:
                        connection_stats["UserOrderAssignation"]["validated"] += 1
                        create_relationship(
                            "UserOrderAssignation",
                            f"users/{customer._key}",
                            f"assignations/{assignation._key}",
                        )
                    else:
                        connection_stats["UserOrderAssignation"]["failed"] += 1


def process_customer_order_assignation_driver(customers, orders, assignations, drivers):
    for customer in customers:
        for order in orders:
            if order.userId == customer._key:
                for assignation in assignations:
                    if assignation.orderId == order._key:
                        for driver in drivers:
                            if assignation.userId == driver._key:
                                connection_stats["UserOrderAssignationDriver"][
                                    "validated"
                                ] += 1
                                create_relationship(
                                    "UserOrderAssignationDriver",
                                    f"users/{customer._key}",
                                    f"users/{driver._key}",
                                )
                            else:
                                connection_stats["UserOrderAssignationDriver"][
                                    "failed"
                                ] += 1


def create_relationship(collection_name, from_id, to_id):
    """
    Create a relationship in the specified collection from the given _from to _to.
    """
    try:
        collection = db.collection(collection_name)
        collection.insert({"_from": from_id, "_to": to_id})
        logging.info(
            f"Created relationship in {collection_name} from {from_id} to {to_id}"
        )
    except ArangoError as e:
        logging.error(
            f"Error creating relationship in {collection_name} from {from_id} to {to_id}. Error: {e}"
        )


# def create_relationship(collection, from_id, to_id):
#     # This function simulates the creation of a relationship
#     # In a real scenario, you'd use the ORM to create the relationship
#     print(f"Creating relationship in {collection} from {from_id} to {to_id}")


def fetch_data_by_field(collection_class, field_name, field_values):
    print(f"Fetching data for {field_name}: {field_values}")  # Debugging statement
    data = (
        db.query(collection_class)
        .filter(f"{field_name} IN @values", values=field_values)
        .all()
    )
    print(f"Fetched {len(data)} records from {collection_class.__collection__}")
    return data


def main():
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("./logs/wetRun.log"), logging.StreamHandler()],
    )

    load_dotenv()
    ARANGODB_HOST = os.environ.get("ARANGODB_HOST")
    ARANGODB_USER = os.environ.get("ARANGODB_USER")
    ARANGODB_PW = os.environ.get("ARANGODB_PW")
    DB_NAME = os.environ.get("ARANGODB_DB")

    # ArangoDB connection
    client = ArangoClient(hosts=ARANGODB_HOST)
    test_db = client.db(DB_NAME, username=ARANGODB_USER, password=ARANGODB_PW)
    db = Database(test_db)

    all_users = db.query(User).all()

    # sample_size = 10
    # # Randomly sample a subset of users
    # all_users = (
    #     db.query(User).limit(sample_size).all()
    # )  # Assuming arango_orm supports random sampling
    for user in all_users:
        related_orders = fetch_data_by_field(Order, "userId", [user._key])
        order_keys = [order._key for order in related_orders]
        related_assignations = fetch_data_by_field(Assignation, "orderId", order_keys)
        related_feedbacks = fetch_data_by_field(CustomerFeedback, "orderId", order_keys)

        related_locations_keys = list(
            set(
                [order.originLocationId for order in related_orders]
                + [order.destinationLocationId for order in related_orders]
            )
        )
        related_locations = fetch_data_by_field(
            Location, "_key", related_locations_keys
        )

        related_routes_keys = [order.routeId for order in related_orders]
        related_routes = fetch_data_by_field(Route, "_key", related_routes_keys)

        related_countries_keys = [location.countryId for location in related_locations]
        related_countries = fetch_data_by_field(Country, "_key", related_countries_keys)

        related_travel_data_keys = [
            {
                "originId": order.originLocationId,
                "destinationId": order.destinationLocationId,
            }
            for order in related_orders
        ]
        related_travel_data = []
        for key_pair in related_travel_data_keys:
            data = fetch_data_by_field(TravelData, "originId", [key_pair["originId"]])
            for entry in data:
                if entry.destinationId == key_pair["destinationId"]:
                    related_travel_data.append(entry)

        # Validate the relationships for these entities
        process_users_orders([user], related_orders)
        process_order_location_country(
            related_orders, related_locations, related_countries
        )
        process_order_route(related_orders, related_routes)
        process_order_travel_data(related_orders, related_travel_data)
        process_user_order_location_country(
            [user], related_orders, related_locations, related_countries
        )
        process_driver_assignation_order([user], related_assignations, related_orders)
        process_customer_order_assignation([user], related_orders, related_assignations)
        process_customer_order_assignation_driver(
            [user], related_orders, related_assignations, all_users
        )

    # Calculate percentages
    total_validated = sum([stats["validated"] for stats in connection_stats.values()])
    total_failed = sum([stats["failed"] for stats in connection_stats.values()])

    for connection, stats in connection_stats.items():
        stats["%_of_total_validated"] = (
            (stats["validated"] / total_validated) * 100 if total_validated else 0
        )
        stats["%_of_total_failed"] = (
            (stats["failed"] / total_failed) * 100 if total_failed else 0
        )

    # Write to file
    with open("./logs/connection_stats.json", "w") as f:
        f.write(json.dumps(connection_stats, indent=4))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"Error: {e}")
