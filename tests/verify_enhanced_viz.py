import sys
import os
import matplotlib.pyplot as plt

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

from supply_chain.simulation.graph import GraphBuilder
from supply_chain.simulation.engine import SimulationEngine
from supply_chain.simulation.visualization import SimulationVisualizer
from supply_chain.simulation.schema import Order, Event, EventType

def test_viz():
    print("Setting up simulation...")
    gb = GraphBuilder()
    gb.create_random_graph(num_nodes=10)
    
    engine = SimulationEngine(gb)
    
    # Create some dummy orders
    nodes = list(gb.nodes.keys())
    engine.schedule_event(Event(0, "T1", nodes[0], EventType.TRUCK_SPAWN))
    engine.schedule_event(Event(0, "T2", nodes[1], EventType.TRUCK_SPAWN))
    
    engine.schedule_event(Event(1, "SYS", nodes[0], EventType.ORDER_CREATED, 
                                details={"order_id": "O1", "origin": nodes[0], "destination": nodes[2]}))
    
    print("Running simulation step...")
    engine.run(duration=10)
    
    print("Testing animation generation...")
    try:
        # We just want to call it and see if it crashes
        # Since plt.show() blocks, we might need to mock it or just rely on the fact that 
        # we are in a headless env and it might fail to show but succeed to run logic.
        # Actually, we can just check if we can create the objects.
        
        # Monkey patch plt.show to avoid blocking
        plt.show = lambda: print("plt.show() called")
        
        SimulationVisualizer.animate_simulation(engine, gb)
        print("Animation function executed successfully.")
    except Exception as e:
        print(f"Animation failed: {e}")
        raise e

if __name__ == "__main__":
    test_viz()
