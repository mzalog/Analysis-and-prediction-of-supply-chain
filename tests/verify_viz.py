import sys
from pathlib import Path
import matplotlib.pyplot as plt

# Add src to path
sys.path.append(str(Path(__file__).parents[1] / "src"))

from supply_chain.simulation.graph import GraphBuilder
from supply_chain.simulation.visualization import SimulationVisualizer

def verify_viz():
    print("--- Verifying Visualization Code ---")
    gb = GraphBuilder()
    gb.create_random_graph(num_nodes=5, num_edges=5)
    
    viz = SimulationVisualizer([], gb.graph)
    
    output_path = "test_graph.png"
    try:
        viz.plot_graph(output_path)
        print("PASS: plot_graph ran successfully.")
        if Path(output_path).exists():
            print(f"PASS: Output file {output_path} created.")
            # Clean up
            Path(output_path).unlink()
        else:
            print("FAIL: Output file not created.")
            
    except Exception as e:
        print(f"FAIL: plot_graph raised exception: {e}")

if __name__ == "__main__":
    verify_viz()
