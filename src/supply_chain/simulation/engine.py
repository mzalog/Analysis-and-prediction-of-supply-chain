import heapq
import random
from typing import List, Dict, Any, Optional
from .schema import Event, EventType, Node, Truck, Order, TruckStatus
from .graph import GraphBuilder
from .delays import DelayModel

class SimulationEngine:
    def __init__(self, graph_builder: GraphBuilder):
        self.graph_builder = graph_builder
        self.event_queue: List[Event] = []
        self.current_time = 0.0
        self.event_counter = 0
        self.processed_events: List[Event] = []
        
        # Track trucks and orders
        self.trucks: Dict[str, Truck] = {}
        self.orders: Dict[str, Order] = {}
        self.pending_orders: List[str] = [] # List of Order IDs

    def schedule_event(self, event: Event):
        heapq.heappush(self.event_queue, (event.time, self.event_counter, event))
        self.event_counter += 1

    def get_horizon_events(self, n: int = 5) -> List[Event]:

        smallest = heapq.nsmallest(n, self.event_queue)
        return [item[2] for item in smallest]

    def run(self, duration: float, step_callback=None):
        while self.event_queue and self.current_time < duration:
            self.step()
            if step_callback:
                step_callback(self.current_time)

    def step(self):
        if not self.event_queue:
            return
            
        time, _, event = heapq.heappop(self.event_queue)
        self.current_time = time
        self.processed_events.append(event)
        
        self.process_event(event)

    def process_event(self, event: Event):
        if event.event_type == EventType.TRUCK_SPAWN:
            self.handle_truck_spawn(event)
        elif event.event_type == EventType.ORDER_CREATED:
            self.handle_order_created(event)
        elif event.event_type == EventType.ARRIVAL_NODE:
            self.handle_arrival(event)
        elif event.event_type == EventType.START_SERVICE:
            self.handle_start_service(event)
        elif event.event_type == EventType.END_SERVICE:
            self.handle_end_service(event)
        elif event.event_type == EventType.DEPART_NODE:
            self.handle_depart(event)
        elif event.event_type == EventType.START_REST:
            self.handle_start_rest(event)
        elif event.event_type == EventType.END_REST:
            self.handle_end_rest(event)

    def handle_truck_spawn(self, event: Event):
        truck_id = event.truck_id
        start_node_id = event.node_id
        
        # Spawn truck IDLE at start node
        truck = Truck(id=truck_id, current_node_id=start_node_id, status=TruckStatus.IDLE)
        self.trucks[truck_id] = truck
        
        self.dispatcher_logic()

    def handle_order_created(self, event: Event):
        order_id = event.details["order_id"]
        origin = event.details["origin"]
        destination = event.details["destination"]
        
        order = Order(id=order_id, origin_node_id=origin, destination_node_id=destination, creation_time=self.current_time)
        self.orders[order_id] = order
        self.pending_orders.append(order_id)
        
        # Trigger dispatcher
        self.dispatcher_logic()

    def dispatcher_logic(self):

        
        if not self.pending_orders:
            return

        # Find all IDLE trucks
        idle_trucks = [t for t in self.trucks.values() if t.status == TruckStatus.IDLE]
        if not idle_trucks:
            return

        # For now, just take the first one
        order_id = self.pending_orders[0]
        order = self.orders[order_id]
        
        # Find nearest idle truck
        best_truck = None
        min_dist = float('inf')
        
        for truck in idle_trucks:

            best_truck = truck
            break
        
        if best_truck:
            self.assign_order_to_truck(order, best_truck)
            self.pending_orders.pop(0)

    def assign_order_to_truck(self, order: Order, truck: Truck):
        # Plan route: Truck -> Origin -> Destination
        # 1. Path to Pickup
        path_to_pickup = []
        if truck.current_node_id != order.origin_node_id:
            path_to_pickup = self.graph_builder.get_shortest_path(truck.current_node_id, order.origin_node_id)
            path_to_pickup = self.graph_builder.get_shortest_path(truck.current_node_id, order.origin_node_id)
        
        # 2. Path to Delivery
        path_to_delivery = self.graph_builder.get_shortest_path(order.origin_node_id, order.destination_node_id)
        
        # Combine routes

        
        full_route = []
        if path_to_pickup:
            full_route.extend(path_to_pickup)
            if path_to_delivery:
                if full_route[-1] == path_to_delivery[0]:
                    full_route.extend(path_to_delivery[1:])
                else:
                    full_route.extend(path_to_delivery)
        else:

            full_route = path_to_delivery
        
        if not full_route or len(full_route) < 2:
            order.status = "CANCELLED"
            return


        truck.status = TruckStatus.EN_ROUTE_TO_PICKUP
        truck.assigned_order_id = order.id
        order.status = "ASSIGNED"


        self.schedule_event(Event(
            time=self.current_time,
            truck_id=truck.id,
            node_id=truck.current_node_id,
            event_type=EventType.ORDER_ASSIGNED,
            details={
                "order_id": order.id,
                "origin": order.origin_node_id,
                "destination": order.destination_node_id,
                "reason": "Nearest Idle Truck"
            }
        ))

        truck.route = full_route
        truck.current_node_index = 0
        

        
        if len(truck.route) > 1:
            self.schedule_event(Event(
                time=self.current_time,
                truck_id=truck.id,
                node_id=truck.current_node_id,
                event_type=EventType.DEPART_NODE
            ))
        elif len(truck.route) == 1:
            pass

    def handle_arrival(self, event: Event):
        node = self.graph_builder.get_node(event.node_id)
        truck_id = event.truck_id
        truck = self.trucks[truck_id]
        
        truck.current_node_id = node.id # Update location
        truck.current_leg_duration = 0.0 # Reset interpolation
        truck.current_leg_start_time = 0.0
        
        # Check if we reached Pickup or Delivery
        order = None
        if truck.assigned_order_id:
            order = self.orders[truck.assigned_order_id]
        
        # Check capacity
        if node.busy_count < node.capacity:
            self.schedule_event(Event(
                time=self.current_time,
                truck_id=truck_id,
                node_id=node.id,
                event_type=EventType.START_SERVICE
            ))
        else:
            node.queue.append(truck_id)

    def handle_start_service(self, event: Event):
        node = self.graph_builder.get_node(event.node_id)
        node.busy_count += 1
        
        service_time = DelayModel.get_service_time(node)
        
        self.schedule_event(Event(
            time=self.current_time + service_time,
            truck_id=event.truck_id,
            node_id=node.id,
            event_type=EventType.END_SERVICE,
            details={"service_duration": service_time}
        ))

    def handle_end_service(self, event: Event):
        node = self.graph_builder.get_node(event.node_id)
        node.busy_count -= 1
        truck = self.trucks[event.truck_id]
        

        if truck.assigned_order_id:
            order = self.orders[truck.assigned_order_id]
            if truck.status == TruckStatus.EN_ROUTE_TO_PICKUP and node.id == order.origin_node_id:
                truck.status = TruckStatus.EN_ROUTE_TO_DELIVERY
            elif truck.status == TruckStatus.EN_ROUTE_TO_DELIVERY and node.id == order.destination_node_id:

                truck.status = TruckStatus.IDLE
                truck.assigned_order_id = None
                order.status = "COMPLETED"
                truck.route = []
                

                if node.queue:
                    next_truck_id = node.queue.pop(0)
                    self.schedule_event(Event(
                        time=self.current_time,
                        truck_id=next_truck_id,
                        node_id=node.id,
                        event_type=EventType.START_SERVICE
                    ))
                return

        if truck.route and truck.current_node_index < len(truck.route) - 1:
            self.schedule_event(Event(
                time=self.current_time,
                truck_id=event.truck_id,
                node_id=node.id,
                event_type=EventType.DEPART_NODE
            ))
        

        if node.queue:
            next_truck_id = node.queue.pop(0)
            self.schedule_event(Event(
                time=self.current_time,
                truck_id=next_truck_id,
                node_id=node.id,
                event_type=EventType.START_SERVICE
            ))

    def handle_depart(self, event: Event):
        truck = self.trucks[event.truck_id]
        current_node_id = event.node_id
        
        if not truck.route:
            return

        if truck.current_node_index < len(truck.route) - 1:
            next_node_id = truck.route[truck.current_node_index + 1]
            
            edge = self.graph_builder.get_edge(current_node_id, next_node_id)
            if edge:
                travel_time = DelayModel.get_travel_time(edge)
                

                if truck.driving_time_since_rest > 0 and (truck.driving_time_since_rest + travel_time > 480):
                    self.schedule_event(Event(
                        time=self.current_time,
                        truck_id=truck.id,
                        node_id=current_node_id,
                        event_type=EventType.START_REST
                    ))
                    return

                truck.current_node_index += 1
                truck.driving_time_since_rest += travel_time
                
                truck.current_leg_start_time = self.current_time
                truck.current_leg_duration = travel_time
                
                self.schedule_event(Event(
                    time=self.current_time + travel_time,
                    truck_id=truck.id,
                    node_id=next_node_id,
                    event_type=EventType.ARRIVAL_NODE,
                    details={"travel_duration": travel_time}
                ))

    def handle_start_rest(self, event: Event):
        truck = self.trucks[event.truck_id]

        truck.previous_status = truck.status
        truck.is_resting = True
        truck.status = TruckStatus.RESTING

        rest_duration = 60.0
        self.schedule_event(Event(
            time=self.current_time + rest_duration,
            truck_id=truck.id,
            node_id=event.node_id,
            event_type=EventType.END_REST,
            details={"rest_duration": rest_duration}
        ))

    def handle_end_rest(self, event: Event):
        truck = self.trucks[event.truck_id]
        truck.is_resting = False
        truck.driving_time_since_rest = 0.0


        if truck.assigned_order_id and truck.previous_status is not None:
            if truck.previous_status in (TruckStatus.EN_ROUTE_TO_PICKUP, TruckStatus.EN_ROUTE_TO_DELIVERY):
                truck.status = truck.previous_status
            else:
                truck.status = TruckStatus.EN_ROUTE_TO_DELIVERY
        else:
            truck.status = TruckStatus.IDLE

        truck.previous_status = None


        self.schedule_event(Event(
            time=self.current_time,
            truck_id=truck.id,
            node_id=event.node_id,
            event_type=EventType.DEPART_NODE
        ))
