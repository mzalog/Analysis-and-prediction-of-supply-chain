import random
from .schema import Node, Edge

class DelayModel:
    @staticmethod
    def get_travel_time(edge: Edge) -> float:

        base = edge.base_travel_time
        noise = random.uniform(0.0, 1.0)
        
        if random.random() < 0.05:
            noise += random.uniform(0.5, 2.0)
            
        return max(1.0, base * (1 + noise))

    @staticmethod
    def get_service_time(node: Node) -> float:

        base = 10.0
        if node.type == "hub":
            base = 15.0
        elif node.type == "inspection":
            base = 20.0
        
        actual = random.gammavariate(4.0, 35.0)
        return max(60.0, min(actual, 300.0))
