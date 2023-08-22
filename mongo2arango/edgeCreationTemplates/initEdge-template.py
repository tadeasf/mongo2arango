from models import MyEdgeRelationship  # Import the edge relationship model
from arango_orm import Database
from arango import ArangoClient

# Connect to ArangoDB
client = ArangoClient()
db_instance = client.db("my_database")
db = Database(db_instance)

# Create the edge collection if it does not exist
if not db.has_collection(MyEdgeRelationship.__collection__):
    db.create_collection(MyEdgeRelationship)

# for dropping
# if db.has_collection(MyEdgeRelationship.__collection__):
#     db.drop_collection(MyEdgeRelationship.__collection__)

print(f"{MyEdgeRelationship.__collection__} edge collection initialized successfully!")
