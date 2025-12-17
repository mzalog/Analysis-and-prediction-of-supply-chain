import sys
import os
from pathlib import Path

# Add src to path
# tests/verify_graph_sparsity.py -> tests -> big-data-supply-chain -> src
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from supply_chain.simulation.graph import GraphBuilder, haversine_distance

def verify_graph():
    print("Verifying Graph Generation...")
    gb = GraphBuilder()
    num_nodes = 20
    k = 3
    gb.create_random_graph(num_nodes=num_nodes, k_neighbors=k)
    
    # 1. Check Node Count
    assert len(gb.nodes) == num_nodes, f"Expected {num_nodes} nodes, got {len(gb.nodes)}"
    print(f"[PASS] Node count: {len(gb.nodes)}")
    
    # 2. Check Edge Count (Sparsity)
    # Each node connects to k neighbors. We also added reverse edges.
    # Max edges should be roughly k * N * 2 (if all reverse edges were missing)
    # Min edges k * N
    num_edges = len(gb.edges)
    print(f"Total edges: {num_edges}")
    
    # Check average degree
    avg_degree = num_edges / num_nodes
    print(f"Average degree: {avg_degree}")
    
    if avg_degree > num_nodes / 2:
        print("[WARN] Graph might be too dense!")
    else:
        print("[PASS] Graph is sparse.")
        
    # 3. Check Distances
    print("Checking distance calculations...")
    for (u, v), edge in gb.edges.items():
        node_u = gb.get_node(u)
        node_v = gb.get_node(v)
        
        calc_dist = haversine_distance(node_u.lat, node_u.lon, node_v.lat, node_v.lon)
        assert abs(calc_dist - edge.distance_km) < 0.1, f"Distance mismatch: {calc_dist} vs {edge.distance_km}"
        
    print("[PASS] Distances match Haversine formula.")
    
    # 4. Check Connectivity
    # Pick random pairs and check path
    import random
    import networkx as nx
    
    print("Checking connectivity...")
    # Since we use k-NN with k=3 on 20 nodes, it SHOULD be connected usually, but not guaranteed 100% without MST.
    # However, we added bidirectional edges, which helps.
    
    # Let's check if the underlying undirected graph is connected
    undirected = gb.graph.to_undirected()
    is_connected = nx.is_connected(undirected)
    print(f"Graph connected (undirected): {is_connected}")
    
    if not is_connected:
        components = list(nx.connected_components(undirected))
        print(f"Graph has {len(components)} connected components.")
        print("Note: k-NN does not guarantee single component, but for simulation it might be acceptable if we route within components.")
    else:
        print("[PASS] Graph is fully connected.")

if __name__ == "__main__":
    verify_graph()
