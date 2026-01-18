
import sys
from pathlib import Path
import numpy as np

# Add src to sys.path
project_root = Path(__file__).resolve().parent
src_path = project_root / "src"
if str(src_path) not in sys.path:
    sys.path.append(str(src_path))

from supply_chain.gnn.model import SupplyChainGNN
import torch

def test_model():
    print("🔬 Testing GNN Inference Sensitivity...")
    
    # Load Model
    project_root = Path(".").resolve()
    model_path = project_root / "models" / "supply_chain_gnn.pth"
    
    if not model_path.exists():
        print(f"❌ Model not found at {model_path}")
        return

    model = SupplyChainGNN(in_channels=5, hidden_channels=64, out_channels=1)
    try:
        model.load_state_dict(torch.load(model_path, map_location=torch.device('cpu')))
        model.eval()
        print("✅ Model loaded successfully.")
    except Exception as e:
        print(f"❌ Failed to load model: {e}")
        return

    # Create Synthetic Inputs
    # Features: [Type, Load, Traffic, Weather, Delay]
    
    # Case 1: Baseline (Low Traffic, Low Delay)
    x_low = torch.tensor([[2.0, 5.0, 1.0, 0.1, 0.0]], dtype=torch.float32)
    edge_index = torch.tensor([[0],[0]], dtype=torch.long) # Self-loop
    
    # Case 2: High Traffic Only
    x_traffic = torch.tensor([[2.0, 5.0, 9.0, 0.1, 0.0]], dtype=torch.float32)

    # Case 3: High Delay Only (Sabotage Scenario)
    x_delay = torch.tensor([[2.0, 5.0, 1.0, 0.1, 60.0]], dtype=torch.float32)
    
    # Case 4: Extreme Chaos
    x_chaos = torch.tensor([[2.0, 5.0, 9.0, 0.9, 100.0]], dtype=torch.float32)

    with torch.no_grad():
        p_low = model(x_low, edge_index).item()
        p_traffic = model(x_traffic, edge_index).item()
        p_delay = model(x_delay, edge_index).item()
        p_chaos = model(x_chaos, edge_index).item()

    print("\n📊 Predictions (Risk Probability):")
    print(f"1. Baseline (Delay=0):   {p_low:.4f}")
    print(f"2. Traffic Only (Tr=9):  {p_traffic:.4f}")
    print(f"3. Delay Only (Del=60):  {p_delay:.4f}")
    print(f"4. Extreme (Del=100):    {p_chaos:.4f}")

    if p_delay < 0.5:
        print("\n⚠️  WARNING: Model is NOT predicting high risk for 60m delay!")
        print("    Possible causes: Training data imbalance, feature scaling mismatch, or leakage fix over-correction.")
    else:
        print("\n✅ Model reacts correctly to delay.")

if __name__ == "__main__":
    test_model()
