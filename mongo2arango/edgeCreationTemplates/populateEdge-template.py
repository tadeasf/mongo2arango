from models import Model1, Model2, MyEdgeRelationship  # Import relevant models
from arango_orm import Database
from arango import ArangoClient

# Connect to ArangoDB and fetch data
client = ArangoClient()
db_instance = client.db("my_database")
db = Database(db_instance)

# Fetch relevant data from collections
# Example:
# items1 = db.query(Model1).all()
# items2 = db.query(Model2).all()

# Prepare the relationships in bulk
# Example:
# relationships = [MyEdgeRelationship(_from=item1._id, _to=item2._id) for item1 in items1 for item2 in items2]

# Add the relationships in bulk
# db.bulk_add(entity_list=relationships)

print("Relationships populated successfully!")
