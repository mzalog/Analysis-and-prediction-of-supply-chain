import sys
import os
from pathlib import Path

# Add project root/src to sys.path to allow imports from supply_chain package
current_dir = Path(__file__).resolve().parent
src_path = current_dir.parent.parent.parent / "src"
if str(src_path) not in sys.path:
    sys.path.append(str(src_path))

import torch
import torch.nn as nn
from supply_chain.gnn.dataset import SupplyChainGraphDataset
from supply_chain.gnn.model import SupplyChainGNN
from supply_chain.simulation.graph import GraphBuilder
from supply_chain.config import REPORTS_DIR, DATA_RAW_DIR
import pandas as pd

def train_gnn():
    print("🚀 Starting GNN Training Pipeline...")
    
    # 0. Setup Device (Support User's GPU)
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"   Using Device: {device}")
    if device.type == 'cpu':
        print("   ⚠️ CUDA not detected. Running on CPU (slower).")

    # 1. Load Data
    data_path = Path(REPORTS_DIR) / "experiments" / "long_term_5y" / "simulated_data_5y.csv"
    if not data_path.exists():
        # Fallback to older experiment
        data_path = Path(REPORTS_DIR) / "experiments" / "experiment_massive" / "simulated_data.csv"
        
    print(f"   Loading Simulation Data from: {data_path}")
    df = pd.read_csv(data_path)
    
    # Quick Map Load
    gb = GraphBuilder()
    map_path = Path("kroA100.txt")
    if map_path.exists():
        gb.create_from_tsplib(map_path, k_neighbors=4)
    else:
        gb.create_random_graph(30, 4)
        
    # 2. Prepare Graph Dataset
    print("   Converting Data to Graph Snapshots...")
    dataset = SupplyChainGraphDataset(gb, df, time_window_min=60*24) # Daily snapshots
    dataset.process()
    loader = dataset.get_loader(batch_size=8)
    
    # 3. Initialize Model
    model = SupplyChainGNN(in_channels=5, hidden_channels=64, out_channels=1).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
    criterion = nn.MSELoss()
    
    # 4. Training Loop
    model.train()
    epochs = 20
    
    print("\n   🔄 Training Epochs:")
    for epoch in range(epochs):
        total_loss = 0
        steps = 0
        
        for batch in loader:
            batch = batch.to(device)
            optimizer.zero_grad()
            
            # Forward
            out = model(batch.x, batch.edge_index, batch.batch)
            
            # Loss (Compare predicted Risk to Calculated Risk in Y)
            loss = criterion(out, batch.y)
            
            loss.backward()
            optimizer.step()
            
            total_loss += loss.item()
            steps += 1
            
        avg_loss = total_loss / steps
        if epoch % 5 == 0:
            print(f"     Epoch {epoch+1}/{epochs} | Loss: {avg_loss:.4f}")

    # 5. Save Model
    # Determine project root (assuming src/supply_chain/gnn/train.py)
    project_root = Path(__file__).resolve().parent.parent.parent.parent
    output_dir = project_root / "models"
    output_dir.mkdir(parents=True, exist_ok=True)
    save_path = output_dir / "supply_chain_gnn.pth"
    torch.save(model.state_dict(), save_path)
    print(f"\n✅ GNN Model Saved to: {save_path}")

if __name__ == "__main__":
    train_gnn()
