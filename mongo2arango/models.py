from arango_orm import Collection, Relation
from arango_orm.fields import (
    String,
    List,
    Dict,
    DateTime,
    Boolean,
    Int,
    Float,
    UUID,
    Mapping,
)

# Collections


class User(Collection):
    __collection__ = "users"
    _key = String()
    # ... other fields ...


class Order(Collection):
    __collection__ = "orders"
    _key = String()
    userId = UUID()
    destinationLocationId = UUID()
    originLocationId = UUID()
    routeId = UUID()
    passengers = List(Dict())
    automaticEmails = List(Dict())
    vehicleTypesPricesFees = List(Dict())
    confirmedAt = Mapping(keys="$date", values=DateTime(), allow_none=True)
    createdAt = Mapping(keys="$date", values=DateTime())
    acceptedAt = Mapping(keys="$date", values=DateTime(), allow_none=True)
    departureAt = Mapping(keys="$date", values=DateTime())
    requestHeader = Dict()
    originLocation = Dict()
    contentLocations = List(Dict())
    # ... other fields ...


class UserRole(Collection):
    __collection__ = "userRoles"
    _key = String()
    userId = UUID()
    roles = List(String())


class Assignation(Collection):
    __collection__ = "assignations"
    _key = String()
    userId = String()
    orderId = String()
    # ... other fields ...


class CustomerFeedback(Collection):
    __collection__ = "customerFeedbacks"
    _key = String()
    orderId = String()
    userId = String()
    textScore = String()
    text = String()


class Location(Collection):
    __collection__ = "locations"
    _key = String()
    name = String()
    countryId = String()


class Country(Collection):
    __collection__ = "countries"
    _key = String()
    englishName = String()


class Route(Collection):
    __collection__ = "routes"
    _key = String()
    # ... other fields ...


class TravelData(Collection):
    __collection__ = "travelData"
    _key = String()
    originId = String()
    destinationId = String()
    distance = Float()
    duration = Float()


# Relationships


class CreatedOrder(Relation):
    __collection__ = "created_order"
    _from = String()  # users._id
    _to = String()  # orders.userId


class UserAssignation(Relation):
    __collection__ = "user_assignation"
    _from = String()  # users._id
    _to = String()  # assignations.userId


class AssignationOrder(Relation):
    __collection__ = "assignation_order"
    _from = String()  # assignations._key
    _to = String()  # orders._key


class FeedbackForDriver(Relation):
    __collection__ = "feedback_for_driver"
    _from = String()  # customerFeedbacks._key
    _to = String()  # users._id


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
