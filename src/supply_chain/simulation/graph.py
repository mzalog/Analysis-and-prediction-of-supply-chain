import networkx as nx
import random
import math
from pathlib import Path
from typing import Dict, List, Tuple

from .schema import Node, Edge, NodeType
from .tsplib_parser import parse_tsplib, normalize_coordinates, euclidean_distance

def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in km
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    
    a = math.sin(dphi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

class GraphBuilder:
    def __init__(self):
        self.nodes: Dict[str, Node] = {}
        self.edges: Dict[Tuple[str, str], Edge] = {}
        self.graph = nx.DiGraph()

    def add_node(self, node: Node):
        self.nodes[node.id] = node
        self.graph.add_node(node.id, data=node)

    def add_edge(self, edge: Edge):
        self.edges[(edge.source, edge.target)] = edge
        self.graph.add_edge(edge.source, edge.target, data=edge, weight=edge.base_travel_time)

    def get_node(self, node_id: str) -> Node:
        return self.nodes[node_id]

    def get_edge(self, source: str, target: str) -> Edge:
        return self.edges.get((source, target))

    def create_random_graph(self, num_nodes=10, k_neighbors=3):
        # Create Nodes
        node_types = list(NodeType)
        nodes_list = []
        
        for i in range(num_nodes):
            node_id = f"N{i+1}"
            node_type = random.choice(node_types)
            # Random lat/lon roughly around central Europe
            lat = 50.0 + random.uniform(-5, 5) # Reduced spread for more realistic density
            lon = 19.0 + random.uniform(-10, 10)
            capacity = random.randint(1, 3)
            is_inspection = (random.random() < 0.3)
            
            node = Node(
                id=node_id,
                type=node_type,
                lat=lat,
                lon=lon,
                capacity=capacity,
                is_inspection=is_inspection
            )
            self.add_node(node)
            nodes_list.append(node)

        # Create Edges using k-Nearest Neighbors
        # This ensures sparsity and local connectivity
        
        for i, node_u in enumerate(nodes_list):
            # Calculate distances to all other nodes
            distances = []
            for j, node_v in enumerate(nodes_list):
                if i == j: continue
                dist = haversine_distance(node_u.lat, node_u.lon, node_v.lat, node_v.lon)
                distances.append((dist, node_v))
            
            # Sort by distance and pick k nearest
            distances.sort(key=lambda x: x[0])
            nearest = distances[:k_neighbors]
            
            # Add edges to nearest neighbors
            for dist, node_v in nearest:
                # Add edge u -> v
                base_time = dist / 60.0 * 60.0 # assume 60km/h -> minutes
                edge_uv = Edge(
                    source=node_u.id,
                    target=node_v.id,
                    distance_km=dist,
                    base_travel_time=base_time
                )
                self.add_edge(edge_uv)
                
                # Ensure bidirectionality (v -> u)
                if (node_v.id, node_u.id) not in self.edges:
                    edge_vu = Edge(
                        source=node_v.id,
                        target=node_u.id,
                        distance_km=dist,
                        base_travel_time=base_time
                    )
                    self.add_edge(edge_vu)

        # Ensure Graph Connectivity
        # k-NN might leave disconnected components. We connect them here.
        undirected = self.graph.to_undirected()
        if not nx.is_connected(undirected):
            components = list(nx.connected_components(undirected))
            # Connect component i to component i+1
            for i in range(len(components) - 1):
                comp_a = list(components[i])
                comp_b = list(components[i+1])
                
                # Find closest pair of nodes between comp_a and comp_b
                min_dist = float('inf')
                best_pair = None
                
                # Find best pair

                for u_id in comp_a:
                    u = self.nodes[u_id]
                    for v_id in comp_b:
                        v = self.nodes[v_id]
                        dist = haversine_distance(u.lat, u.lon, v.lat, v.lon)
                        if dist < min_dist:
                            min_dist = dist
                            best_pair = (u, v)
                
                # Add edge between best pair
                if best_pair:
                    u, v = best_pair
                    base_time = min_dist / 60.0 * 60.0
                    
                    # Add bidirectional edge
                    self.add_edge(Edge(u.id, v.id, min_dist, base_time))
                    self.add_edge(Edge(v.id, u.id, min_dist, base_time))

    def get_shortest_path(self, start_node: str, end_node: str) -> List[str]:
        try:
            return nx.shortest_path(self.graph, start_node, end_node, weight="weight")
        except nx.NetworkXNoPath:
            return []

    def create_from_tsplib(self, file_path: Path, k_neighbors: int = 4):
        """
        Build a sparse graph from a TSPLIB file (kroA100.txt).
        
        Uses k-nearest neighbors to create a realistic, non-complete graph.
        Node types are assigned based on position patterns.
        
        Args:
            file_path: Path to the TSPLIB .txt/.tsp file
            k_neighbors: Number of nearest neighbors to connect (default 4)
        """
        problem_name, tsp_nodes = parse_tsplib(file_path)
        
        if not tsp_nodes:
            raise ValueError(f"No nodes found in {file_path}")
        
        # Normalize coordinates to realistic lat/lon (Central Europe)
        coords = normalize_coordinates(tsp_nodes)
        
        # Assign node types based on some heuristics:
        # - First ~10% are warehouses
        # - Next ~10% are hubs  
        # - Next ~5% are ports
        # - Next ~5% are inspection points
        # - Rest are customers
        n = len(tsp_nodes)
        type_assignments = []
        for i in range(n):
            ratio = i / n
            if ratio < 0.10:
                type_assignments.append(NodeType.WAREHOUSE)
            elif ratio < 0.20:
                type_assignments.append(NodeType.HUB)
            elif ratio < 0.25:
                type_assignments.append(NodeType.PORT)
            elif ratio < 0.30:
                type_assignments.append(NodeType.INSPECTION)
            else:
                type_assignments.append(NodeType.CUSTOMER)
        
        random.shuffle(type_assignments)
        
        nodes_list: List[Node] = []
        for i, (tsp_node, (lat, lon), node_type) in enumerate(zip(tsp_nodes, coords, type_assignments)):
            node_id = f"N{tsp_node.id}"
            
            if node_type == NodeType.WAREHOUSE:
                capacity = random.randint(3, 5)
            elif node_type == NodeType.HUB:
                capacity = random.randint(2, 4)
            elif node_type == NodeType.PORT:
                capacity = random.randint(2, 3)
            else:
                capacity = random.randint(1, 2)
            
            is_inspection = (node_type == NodeType.INSPECTION)
            
            node = Node(
                id=node_id,
                type=node_type,
                lat=lat,
                lon=lon,
                capacity=capacity,
                is_inspection=is_inspection
            )
            self.add_node(node)
            nodes_list.append(node)
        
        # Create edges using k-Nearest Neighbors based on original TSP distances
        # This preserves the spatial structure from the TSPLIB instance
        for i, node_u in enumerate(nodes_list):
            tsp_u = tsp_nodes[i]
            
            distances = []
            for j, node_v in enumerate(nodes_list):
                if i == j:
                    continue
                tsp_v = tsp_nodes[j]
                # Use original Euclidean distance for neighbor selection
                dist_tsp = euclidean_distance(tsp_u, tsp_v)
                distances.append((dist_tsp, j, node_v))
            
            distances.sort(key=lambda x: x[0])
            nearest = distances[:k_neighbors]
            
            for dist_tsp, j, node_v in nearest:
                # Calculate realistic distance in km using haversine on normalized coords
                dist_km = haversine_distance(node_u.lat, node_u.lon, node_v.lat, node_v.lon)
                
                # Travel time: assume average 50 km/h for logistics
                base_time = (dist_km / 50.0) * 60.0  # minutes
                
                # Add edge u -> v
                if (node_u.id, node_v.id) not in self.edges:
                    self.add_edge(Edge(
                        source=node_u.id,
                        target=node_v.id,
                        distance_km=dist_km,
                        base_travel_time=base_time
                    ))
                
                # Add reverse edge for bidirectionality
                if (node_v.id, node_u.id) not in self.edges:
                    self.add_edge(Edge(
                        source=node_v.id,
                        target=node_u.id,
                        distance_km=dist_km,
                        base_travel_time=base_time
                    ))
        
        # Ensure graph connectivity
        self._ensure_connectivity()
    
    def _ensure_connectivity(self):
        """Connect disconnected components in the graph."""
        undirected = self.graph.to_undirected()
        if nx.is_connected(undirected):
            return
        
        components = list(nx.connected_components(undirected))
        
        for i in range(len(components) - 1):
            comp_a = list(components[i])
            comp_b = list(components[i + 1])
            
            min_dist = float('inf')
            best_pair = None
            
            for u_id in comp_a:
                u = self.nodes[u_id]
                for v_id in comp_b:
                    v = self.nodes[v_id]
                    dist = haversine_distance(u.lat, u.lon, v.lat, v.lon)
                    if dist < min_dist:
                        min_dist = dist
                        best_pair = (u, v)
            
            if best_pair:
                u, v = best_pair
                base_time = (min_dist / 50.0) * 60.0
                
                self.add_edge(Edge(u.id, v.id, min_dist, base_time))
                self.add_edge(Edge(v.id, u.id, min_dist, base_time))
