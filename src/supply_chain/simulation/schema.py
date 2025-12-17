from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from enum import Enum

class EventType(str, Enum):
    ARRIVAL_NODE = "ARRIVAL_NODE"
    START_SERVICE = "START_SERVICE"
    END_SERVICE = "END_SERVICE"
    DEPART_NODE = "DEPART_NODE"
    TRUCK_SPAWN = "TRUCK_SPAWN"
    START_REST = "START_REST"
    END_REST = "END_REST"
    ORDER_CREATED = "ORDER_CREATED"
    ORDER_ASSIGNED = "ORDER_ASSIGNED"

class TruckStatus(str, Enum):
    IDLE = "IDLE"
    EN_ROUTE_TO_PICKUP = "EN_ROUTE_TO_PICKUP"
    EN_ROUTE_TO_DELIVERY = "EN_ROUTE_TO_DELIVERY"
    RESTING = "RESTING"

@dataclass
class Order:
    id: str
    origin_node_id: str
    destination_node_id: str
    creation_time: float
    status: str = "PENDING" # PENDING, ASSIGNED, COMPLETED

class NodeType(str, Enum):
    WAREHOUSE = "warehouse"
    HUB = "hub"
    INSPECTION = "inspection"
    CUSTOMER = "customer"
    PORT = "port"

@dataclass
class Node:
    id: str
    type: NodeType  # warehouse, hub, inspection, customer, port
    lat: float
    lon: float
    capacity: int = 1
    is_inspection: bool = False
    queue: List[str] = field(default_factory=list)  # List of truck_ids
    busy_count: int = 0

@dataclass
class Edge:
    source: str
    target: str
    base_travel_time: float  # minutes
    distance_km: float
    mode_allowed: str = "truck"

@dataclass
class Truck:
    id: str
    current_node_id: str # Where the truck is currently (or last known node)
    route: List[str] = field(default_factory=list) # Dynamic route
    current_node_index: int = 0
    cargo_type: str = "general"
    driving_time_since_rest: float = 0.0
    is_resting: bool = False
    status: TruckStatus = TruckStatus.IDLE
    # Remember status before rest so we can restore it afterwards
    previous_status: Optional[TruckStatus] = None
    assigned_order_id: Optional[str] = None
    current_leg_start_time: float = 0.0
    current_leg_duration: float = 0.0

@dataclass(order=True)
class Event:
    time: float
    truck_id: str = field(compare=False)
    node_id: str = field(compare=False)
    event_type: EventType = field(compare=False)
    event_id: int = field(default=0, compare=False)
    details: Dict[str, Any] = field(default_factory=dict, compare=False)
