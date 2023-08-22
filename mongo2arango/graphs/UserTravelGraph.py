from arango_orm import Graph, Database
from arango_orm.collections import EdgeDefinition
from mongo2arango.models import (
    User,
    Order,
    Location,
    Country,
    OrderLocation,
    LocationCountry,
)
from arango import ArangoClient

# Connect to ArangoDB
client = ArangoClient()
db_instance = client.db("my_database")
db = Database(db_instance)


class UserTravelGraph(Graph):
    __graph__ = "user_travel_graph"
    edge_definitions = [
        EdgeDefinition(
            "user_order",
            from_vertex_collections=[User.__collection__],
            to_vertex_collections=[Order.__collection__],
        ),
        EdgeDefinition(
            "order_location",
            from_vertex_collections=[Order.__collection__],
            to_vertex_collections=[Location.__collection__],
        ),
        EdgeDefinition(
            "location_country",
            from_vertex_collections=[Location.__collection__],
            to_vertex_collections=[Country.__collection__],
        ),
    ]


# Creating the graph
db.create_graph(UserTravelGraph)
