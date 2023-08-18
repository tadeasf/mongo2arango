from arango import ArangoClient
from arango_orm import Database
from models import User, Order, Assignation
from dotenv import load_dotenv
import os

load_dotenv()
ARANGODB_HOST = os.environ.get("ARANGODB_HOST")
ARANGODB_USER = os.environ.get("ARANGODB_USER")
ARANGODB_PW = os.environ.get("ARANGODB_PW")
DB_NAME = os.environ.get("ARANGODB_DB")

# ArangoDB connection
client = ArangoClient(hosts=ARANGODB_HOST)
test_db = client.db(DB_NAME, username=ARANGODB_USER, password=ARANGODB_PW)
db = Database(test_db)


def fetch_data_by_field(collection_class, field_name, field_values):
    data = (
        db.query(collection_class)
        .filter(f"{field_name} IN @values", values=field_values)
        .all()
    )
    return data


def validate_customer_driver_relationship(user_key):
    # Fetch orders related to the user
    related_orders = fetch_data_by_field(Order, "userId", [user_key])
    order_keys = [order._key for order in related_orders]

    # Fetch assignations related to these orders
    related_assignations = fetch_data_by_field(Assignation, "orderId", order_keys)

    # For each order and its related assignation, check if there's a driver associated
    for order in related_orders:
        for assignation in related_assignations:
            if assignation.orderId == order._key:
                # Fetch the driver related to this assignation
                related_driver = fetch_data_by_field(User, "_key", [assignation.userId])
                if related_driver:
                    print(
                        f"Valid relationship found for user {user_key} and driver {related_driver[0]._key} and order {order._key} and assignation {assignation._key}"
                    )
                    return True
    print(f"No valid relationship found for user {user_key}")
    return False


# Test the function with a sample user key
sample_user_key = input("Enter the user key to test: ")
validate_customer_driver_relationship(sample_user_key)
