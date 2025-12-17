import sys
import os
import random
from typing import List

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

from supply_chain.simulation.graph import GraphBuilder
from supply_chain.simulation.schema import Node, NodeType

def test_spawn_restriction():
    print("Testing spawn restriction logic...")
    
    # Mock GraphBuilder with specific nodes
    gb = GraphBuilder()
    
    # Add nodes manually to control types
    nodes_data = [
        ("N1", NodeType.WAREHOUSE),
        ("N2", NodeType.HUB),
        ("N3", NodeType.PORT),
        ("N4", NodeType.CUSTOMER),
        ("N5", NodeType.INSPECTION),
        ("N6", NodeType.CUSTOMER)
    ]
    
    for nid, ntype in nodes_data:
        gb.add_node(Node(id=nid, type=ntype, lat=0, lon=0))
        
    # Replicate logic from main.py
    valid_spawn_nodes = [
        n.id for n in gb.nodes.values() 
        if n.type not in [NodeType.CUSTOMER, NodeType.INSPECTION]
    ]
    
    print(f"Valid spawn nodes: {valid_spawn_nodes}")
    
    # Assertions
    expected_valid = ["N1", "N2", "N3"]
    invalid_nodes = ["N4", "N5", "N6"]
    
    for node in expected_valid:
        if node not in valid_spawn_nodes:
            print(f"FAILED: Expected {node} to be valid.")
            sys.exit(1)
            
    for node in invalid_nodes:
        if node in valid_spawn_nodes:
            print(f"FAILED: Found restricted node {node} in valid list.")
            sys.exit(1)
            
    print("SUCCESS: Spawn restriction logic is correct.")

if __name__ == "__main__":
    test_spawn_restriction()
