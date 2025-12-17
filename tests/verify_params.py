import sys
import os
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parents[1] / "src"))

from supply_chain.simulation.delays import DelayModel
from supply_chain.simulation.graph import GraphBuilder
from supply_chain.simulation.schema import Node

def verify_service_times():
    print("\n--- Verifying Service Times (Unloading) ---")
    node = Node(id="test", type="warehouse", lat=0, lon=0, capacity=1)
    times = [DelayModel.get_service_time(node) for _ in range(1000)]
    
    avg = sum(times) / len(times)
    min_t = min(times)
    max_t = max(times)
    
    print(f"Average: {avg:.2f} mins ({avg/60:.2f} hours)")
    print(f"Min: {min_t:.2f} mins")
    print(f"Max: {max_t:.2f} mins")
    
    if 60 <= avg <= 300:
        print("PASS: Average service time is within expected range (1-5 hours).")
    else:
        print("FAIL: Average service time is out of range.")

def verify_route_lengths():
    print("\n--- Verifying Route Lengths ---")
    gb = GraphBuilder()
    gb.create_random_graph(num_nodes=20, num_edges=50)
    
    distances = [edge.distance_km for edge in gb.edges.values()]
    avg_dist = sum(distances) / len(distances)
    min_dist = min(distances)
    max_dist = max(distances)
    
    print(f"Average Distance: {avg_dist:.2f} km")
    print(f"Min Distance: {min_dist:.2f} km")
    print(f"Max Distance: {max_dist:.2f} km")
    
    if 10 <= avg_dist <= 200:
        print("PASS: Average distance is within expected range (10-200 km).")
    else:
        print("FAIL: Average distance is out of range.")

if __name__ == "__main__":
    verify_service_times()
    verify_route_lengths()
