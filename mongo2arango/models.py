from arango_orm import Collection, Relation, Graph, GraphConnection
from arango_orm.fields import String, List, Dict, DateTime, Boolean, Float, Int

# Collections


class User(Collection):
    __collection__ = "users"
    _key = String(unique=True)
    _id = String(unique=True)
    email = String(allow_none=True)
    createdAt = DateTime()
    driver = Dict(allow_none=True)
    countryId = String(allow_none=True)
    travelAgent = Dict(allow_none=True)
    driversCompany = Dict(allow_none=True)
    firstName = String(allow_none=True)
    lastName = String(allow_none=True)
    phoneNumber = String(allow_none=True)
    verifiedAt = DateTime(allow_none=True)
    birthdayAt = DateTime(allow_none=True)
    # ... other fields ...


class Order(Collection):
    __collection__ = "orders"
    _id = String(unique=True)
    _key = String(unique=True)
    userId = String(allow_none=True)
    destinationLocationId = String(allow_none=True)
    originLocationId = String(allow_none=True)
    routeId = String(allow_none=True)
    passengers = List(Dict(), allow_none=True)
    automaticEmails = List(Dict(), allow_none=True)
    vehicleTypesPricesFees = List(Dict(), allow_none=True)
    confirmedAt = DateTime(allow_none=True)
    createdAt = DateTime()
    acceptedAt = DateTime(allow_none=True)
    departureAt = DateTime(allow_none=True)
    requestHeader = Dict(allow_none=True)
    originLocation = Dict(allow_none=True)
    contentLocations = List(Dict(), allow_none=True)
    draftedAt = DateTime(allow_none=True)
    # ... other fields ...


class UserRole(Collection):
    __collection__ = "userRoles"
    _id = String(unique=True)
    _key = String(unique=True)
    userId = String(allow_none=True)
    roles = List(String(), allow_none=True)
    createdAt = DateTime(allow_none=True)


class Assignation(Collection):
    __collection__ = "assignations"
    _id = String(unique=True)
    _key = String(unique=True)
    userId = String(allow_none=True)
    orderId = String(allow_none=True)
    createdAt = DateTime(allow_none=True)
    orderDepartureAt = DateTime(allow_none=True)
    # ... other fields ...


class CustomerFeedback(Collection):
    __collection__ = "customerFeedbacks"
    _id = String(unique=True)
    _key = String(unique=True)
    orderId = String(allow_none=True)
    driverUserId = String(allow_none=True)  # Updated field
    textScore = Float(allow_none=True)
    text = String(allow_none=True)


class Location(Collection):
    __collection__ = "locations"
    _key = String(unique=True)
    _id = String(unique=True)
    name = String(allow_none=True)
    countryId = String(allow_none=True)


class Country(Collection):
    __collection__ = "countries"
    _key = String(unique=True)
    _id = String(unique=True)
    englishName = String(allow_none=True)


class Route(Collection):
    __collection__ = "routes"
    _key = String(unique=True)
    _id = String(unique=True)
    destinationLocationId = String(allow_none=True)
    originLocationId = String(allow_none=True)
    # ... other fields ...


class TravelData(Collection):
    __collection__ = "travelData"
    _id = String(unique=True)
    _key = String(unique=True)
    originId = String(allow_none=True)
    destinationId = String(allow_none=True)
    distance = Float(allow_none=True)
    duration = Float(allow_none=True)


# Relationships


class CreatedOrder(Relation):
    __collection__ = "created_order"
    _from = String()  # users._id
    _to = String()  # orders.userId


class FeedbackForDriver(Relation):
    __collection__ = "feedback_for_driver"
    _from = String()  # customerFeedbacks._key
    _to = String()  # users._id (for drivers)


class OrderLocation(Relation):
    __collection__ = "order_location"
    _from = String()  # orders._key
    _to = String()  # locations._key


class LocationCountry(Relation):
    __collection__ = "location_country"
    _from = String()  # locations._key
    _to = String()  # countries._key


class OrderRoute(Relation):
    __collection__ = "order_route"
    _from = String()  # orders._key
    _to = String()  # routes._key


class OrderTravelData(Relation):
    __collection__ = "order_travel_data"
    _from = String()  # orders._key
    _to = String()  # travelData._key


class UserUserRole(Relation):
    __collection__ = "user_user_role"
    _from = String()  # users._id
    _to = String()  # userRoles.userId


class UserOrderLocationCountry(Relation):
    __collection__ = "user_order_location_country"
    _from = String()  # users._id
    _to = String()  # countries._key


class UserAssignation(Relation):
    __collection__ = "user_assignation"
    _from = String()  # users._key (Driver)
    _to = String()  # assignations._key


class AssignationOrder(Relation):
    __collection__ = "assignation_order"
    _from = String()  # assignations._key
    _to = String()  # orders._key


class UserOrderAssignation(Relation):
    __collection__ = "user_order_assignation"
    _from = String()  # users._key (Customer)
    _to = String()  # assignations._key


class UserOrderAssignationDriver(Relation):
    __collection__ = "user_order_assignation_driver"
    _from = String()  # users._key (Customer)
    _to = String()  # users._key (Driver)


# Define the ReturningCustomer relationship
class ReturningCustomer(Relation):
    __collection__ = "returning_customers"
    _from = String(required=True)  # User ID
    _to = String(required=True)  # Subsequent Order ID


class UserCountry(Relation):
    __collection__ = "user_countries"
    _from = String()
    _to = String()


class CustomerGraph(Graph):
    __graph__ = "customer_graph"
    graph_connections = [
        GraphConnection(User, UserCountry, Country),
        GraphConnection(User, UserOrderLocationCountry, Country),
        GraphConnection(CustomerFeedback, FeedbackForDriver, User),
    ]
