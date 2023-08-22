from arango import ArangoClient
from arango_orm.database import Database
from dotenv import load_dotenv
import os
from models import CustomerGraph

load_dotenv()
ARANGODB_HOST = os.environ.get("ARANGODB_HOST")
ARANGODB_USER = os.environ.get("ARANGODB_USER")
ARANGODB_PW = os.environ.get("ARANGODB_PW")
DB_NAME = os.environ.get("ARANGODB_DB")

client = ArangoClient(hosts=ARANGODB_HOST)
test_db = client.db(DB_NAME, username=ARANGODB_USER, password=ARANGODB_PW)
db = Database(test_db)

customer_graph = CustomerGraph(connection=db)

# Check if the graph already exists
if not db.has_graph(customer_graph.__graph__):
    db.create_graph(customer_graph)
    print(f"Graph {customer_graph.__graph__} created successfully!")
else:
    print(f"Graph {customer_graph.__graph__} already exists.")
