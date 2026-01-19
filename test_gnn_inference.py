
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
import json

def test_model():
    print("🔬 Testing GNN Inference Sensitivity...")
    
    # Load Model
    project_root = Path(".").resolve()
    model_path = project_root / "models" / "supply_chain_gnn.pth"
    
    if not model_path.exists():
        print(f"❌ Model not found at {model_path}")
        return

    model = SupplyChainGNN(in_channels=18, hidden_channels=64, out_channels=1)
    try:
        model.load_state_dict(torch.load(model_path, map_location=torch.device('cpu')))
        model.eval()
        print("✅ Model loaded successfully.")
    except Exception as e:
        print(f"❌ Failed to load model: {e}")
        return

    # Create Synthetic Inputs
    # Features: [Type, Load, Traffic, Weather, Delay, Backlog]
    # Input shape must be [N, 18] (3 stacked windows of 6 features)
    
    def create_input(type_val, load, traff, weath, delay, backlog):
        # Base feature vector [1, 6]
        base = torch.tensor([[type_val, load, traff, weath, delay, backlog]], dtype=torch.float32)
        # Stack 3 times for window [1, 18]
        return torch.cat([base, base, base], dim=1)
    
    # Case 1: Baseline (Low Traffic, Low Delay, No Backlog)
    x_low = create_input(2.0, 5.0, 1.0, 0.1, 0.0, 0.0)
    edge_index = torch.tensor([[0], [0]], dtype=torch.long)  # Self-loop
    edge_attr = torch.zeros((1, 3), dtype=torch.float32)

    # Optional normalization (recommended for consistent results)
    scaler_path = project_root / "models" / "gnn_scaler.json"
    scaler = None
    if scaler_path.exists():
        with open(scaler_path, "r") as f:
            scaler = json.load(f)
    def normalize(x, e):
        if not scaler:
            return x, e
        xm = torch.tensor(scaler["x_mean"], dtype=torch.float32)
        xs = torch.tensor(scaler["x_std"], dtype=torch.float32)
        em = torch.tensor(scaler["edge_mean"], dtype=torch.float32)
        es = torch.tensor(scaler["edge_std"], dtype=torch.float32)
        return (x - xm) / xs, (e - em) / es
    
    # Case 2: High Traffic Only
    x_traffic = create_input(2.0, 5.0, 9.0, 0.1, 0.0, 0.0)

    # Case 3: High Delay Only (Sabotage Scenario)
    x_delay = create_input(2.0, 5.0, 1.0, 0.1, 60.0, 0.0)
    
    # Case 4: Extreme Chaos
    x_chaos = create_input(2.0, 5.0, 9.0, 0.9, 100.0, 50.0)
    
    # Case 5: High Backlog Warning (Early Warning Test)
    x_backlog = create_input(2.0, 5.0, 5.0, 0.1, 5.0, 50.0) # Moderate delay, but huge backlog

    with torch.no_grad():
        x_low_n, e_low_n = normalize(x_low, edge_attr)
        x_traffic_n, e_traffic_n = normalize(x_traffic, edge_attr)
        x_delay_n, e_delay_n = normalize(x_delay, edge_attr)
        x_chaos_n, e_chaos_n = normalize(x_chaos, edge_attr)
        x_backlog_n, e_backlog_n = normalize(x_backlog, edge_attr)

        p_low = torch.sigmoid(model(x_low_n, edge_index, e_low_n)).item()
        p_traffic = torch.sigmoid(model(x_traffic_n, edge_index, e_traffic_n)).item()
        p_delay = torch.sigmoid(model(x_delay_n, edge_index, e_delay_n)).item()
        p_chaos = torch.sigmoid(model(x_chaos_n, edge_index, e_chaos_n)).item()
        p_backlog = torch.sigmoid(model(x_backlog_n, edge_index, e_backlog_n)).item()

    print("\n📊 Predictions (Risk Probability):")
    print(f"1. Baseline (Delay=0):   {p_low:.4f}")
    print(f"2. Traffic Only (Tr=9):  {p_traffic:.4f}")
    print(f"3. Delay Only (Del=60):  {p_delay:.4f}")
    print(f"4. Extreme (Del=100):    {p_chaos:.4f}")
    print(f"5. Backlog Only (Bk=50): {p_backlog:.4f}")

    if p_delay < 0.5:
        print("\n⚠️  WARNING: Model is NOT predicting high risk for 60m delay!")
        print("    Possible causes: Training data imbalance, feature scaling mismatch, or leakage fix over-correction.")
    else:
        print("\n✅ Model reacts correctly to delay.")

if __name__ == "__main__":
    test_model()
