import torch
import math
import random
import hashlib
from torch_geometric.data import Data
from supply_chain.simulation.schema import EventType, NodeType

def compute_edge_attrs(graph, node_mapping, current_time_min):
    """
    Computes dynamic edge attributes [Dist, Traffic, Weather] for GNN.
    Matches logic in dataset.py.
    """
    edge_attrs = []
    
    # Helper noise 
    time_h = current_time_min / 60.0
    def noise(lat, lon, t):
        val = math.sin(lon/5.0 + t/24.0) + math.cos(lat/5.0 + t/48.0)
        return (val + 2.0) / 4.0

    # Must match set of edges in edge_index.
    # We iterate sorted(graph.edges()) to ensure deterministic order matching edge_index construction.
    
    for u, v in sorted(graph.edges()):
        if u in node_mapping and v in node_mapping:
            src_node = graph.nodes[u]['data']
            dst_node = graph.nodes[v]['data']
            
            dist = ((src_node.lat - dst_node.lat)**2 + (src_node.lon - dst_node.lon)**2) ** 0.5
            dist_norm = dist / 10.0
            
            w_src = noise(src_node.lat, src_node.lon, time_h)
            w_dst = noise(dst_node.lat, dst_node.lon, time_h)
            weather = (w_src + w_dst) / 2.0
            
            t_src = noise(src_node.lat+10, src_node.lon+10, time_h)
            t_dst = noise(dst_node.lat+10, dst_node.lon+10, time_h)
            traffic = (t_src + t_dst) / 2.0
            
            # Forward Edge
            edge_attrs.append([dist_norm, traffic, weather])
            # Backward Edge
            edge_attrs.append([dist_norm, traffic, weather])
            
    return torch.tensor(edge_attrs, dtype=torch.float)

def get_current_graph_snapshot(engine, graph_builder, node_overrides=None, test_mode=False):
    """
    Extracts current simulation state into a PyG Data object for GNN inference.
    Uses REAL simulation state (queues, history) and DETERMINISTIC environment factors.
    Args:
        engine: The simulation engine instance.
        graph_builder: The graph builder instance.
        node_overrides (dict): Manual overrides for node states (sabotage).
        test_mode (bool): If True, suppresses random traffic/weather on non-sabotaged nodes for clean signal verification.
    """
    if node_overrides is None:
        node_overrides = {}
        
    graph = graph_builder.graph
    # CRITICAL: Sort nodes by ID to ensure alignment across partial snapshots!
    sorted_nodes = sorted(graph.nodes())
    node_mapping = {n: i for i, n in enumerate(sorted_nodes)}
    num_nodes = len(sorted_nodes)
    
    # 1. Edge Index
    src, dst = [], []
    # Ensure consistent iteration order with compute_edge_attrs
    for u, v in sorted(graph.edges()): 
        if u in node_mapping and v in node_mapping:
            src.append(node_mapping[u])
            dst.append(node_mapping[v])
            src.append(node_mapping[v]) # Undirected for message passing
            dst.append(node_mapping[u])
    edge_index = torch.tensor([src, dst], dtype=torch.long)
    
    # 1.5 Edge Attrs
    edge_attr = compute_edge_attrs(graph, node_mapping, engine.current_time)
    if test_mode:
        # Stabilize GNN inputs in demo/test mode
        edge_attr = torch.zeros_like(edge_attr)
    
    # 2. Node Features [Num_Nodes, 6]
    # Features: [Type, Load, Traffic, Weather, Delay, Backlog]
    x = torch.zeros((num_nodes, 6), dtype=torch.float)
    
    # Helper for Delay Stat (Average service time of last N completed events)
    # This is expensive if history is huge, but fine for demo scale.
    node_delays = {}
    # Optimization: engine.processed_events might be large, stick to last 500
    for ev in reversed(engine.processed_events[-500:]): 
        if ev.event_type == EventType.END_SERVICE and ev.node_id in node_mapping:
            if ev.node_id not in node_delays:
                node_delays[ev.node_id] = []
            node_delays[ev.node_id].append(ev.details.get("service_duration", 0.0))
    
    for node_id, idx in node_mapping.items():
        node_data = graph.nodes[node_id]['data']
        
        # Feature 0: Type
        type_val = 0
        if node_data.type == NodeType.WAREHOUSE: type_val = 1
        elif node_data.type == NodeType.HUB: type_val = 2
        elif node_data.type == NodeType.PORT: type_val = 3
        elif node_data.type == NodeType.CUSTOMER: type_val = 4
        x[idx, 0] = type_val
        
        # Feature 1: Load (Orders currently ASSIGNED/Moving from this node)
        load = sum(1 for o in engine.orders.values() if o.origin_node_id == node_id and o.status == "ASSIGNED")
        x[idx, 1] = float(load)
        
        # Feature 5: Backlog (Pending Orders waiting at this node)
        backlog = sum(1 for oid in engine.pending_orders if engine.orders[oid].origin_node_id == node_id)
        x[idx, 5] = float(backlog)
        
        # Feature 4: Current Delays / Congestion (Calculate BASELINE first)
        # Metric: Average Service Time + Queue Penalty
        recent_times = node_delays.get(node_id, [])
        avg_svc = sum(recent_times) / len(recent_times) if recent_times else 0.0
        
        # Queue penalty: If queue is long, "delay expectation" rises
        queue_len = len(node_data.queue)
        # Heuristic: Delay = Avg Service + (Queue * 5 mins)
        estimated_delay = avg_svc + (queue_len * 5.0)

        # Feature 2 & 3: Context (Deterministic or Override)
        if node_id in node_overrides:
             # User sabotaged this node! 
             # Force High Traffic (Feature 2) - amplified for demo
             x[idx, 2] = node_overrides[node_id].get('traffic', 0.5) * 15.0 
             # Force High Weather Severity (Feature 3)
             x[idx, 3] = node_overrides[node_id].get('weather', 0.5) * 2.0
             
             # CRITICAL: Strong signal injection for demo visibility
             # Inject high values directly (not additive, to ensure signal)
             x[idx, 1] = 20.0   # Load = busy
             x[idx, 4] = 120.0  # Delay = 2 hours
             x[idx, 5] = 150.0  # Backlog = 150 orders stuck
                 
        else:
            # Deterministic Seeding based on Node + Time (Hour)
            # This ensures stable predictions within the same simulation hour
            node_hash = int(hashlib.md5(str(node_id).encode()).hexdigest(), 16)
            seed_val = node_hash + int(engine.current_time / 60.0)
            
            # Use private RNG instance to avoid polluting global random state
            rng = random.Random(seed_val)
            
            if test_mode:
                # 🧪 Test Mode: FULLY IDEAL Conditions
                x[idx, 1] = 0.0  # Load = 0
                x[idx, 2] = 0.0  # Traffic = 0
                x[idx, 3] = 0.0  # Weather = 0
                x[idx, 4] = 0.0  # Delay = 0
                x[idx, 5] = 0.0  # Backlog = 0
                # Only Type (Feature 0) remains as real value
            else:
                # Simulate "Live" Traffic/Weather
                traffic_sim = rng.uniform(0, 10)
                weather_sim = rng.uniform(0, 1)
                
                x[idx, 2] = float(traffic_sim)
                x[idx, 3] = float(weather_sim)
                
                x[idx, 4] = float(estimated_delay)
        
    return Data(x=x, edge_index=edge_index, edge_attr=edge_attr)
