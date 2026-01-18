
import sys
import os
from pathlib import Path
import numpy as np

# Add project root/src to sys.path
current_dir = Path(__file__).resolve().parent
src_path = current_dir.parent.parent.parent / "src"
if str(src_path) not in sys.path:
    sys.path.append(str(src_path))

import torch
import torch.nn as nn
from torch_geometric.loader import DataLoader
from supply_chain.gnn.dataset import SupplyChainGraphDataset
from supply_chain.gnn.model import SupplyChainGNN
from supply_chain.simulation.graph import GraphBuilder
from supply_chain.config import REPORTS_DIR
import pandas as pd

def train_gnn():
    print("🚀 Starting GNN Training Pipeline (Temporal Split)...")
    
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"   Using Device: {device}")

    # 1. Load Data
    # Use the main dataset provided by the user (2021-2025)
    project_root = Path(__file__).resolve().parent.parent.parent.parent
    data_path = project_root / "data" / "raw" / "simulated_supply_chain_data_2021_2025.csv"
        
    print(f"   Loading Simulation Data from: {data_path}")
    if not data_path.exists():
        print("❌ No data found. Please run a long-term simulation first.")
        return

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
    
    if len(dataset.snapshots) < 10:
        print("❌ Not enough snapshots for temporal split. Need at least 10.")
        return

    # 3. Temporal Split (Strict Time Order)
    total_snapshots = len(dataset.snapshots)
    train_idx = int(0.7 * total_snapshots)
    val_idx = int(0.85 * total_snapshots)
    
    train_data = dataset.snapshots[:train_idx]
    val_data = dataset.snapshots[train_idx:val_idx]
    test_data = dataset.snapshots[val_idx:]
    
    print(f"   Split: Train={len(train_data)}, Val={len(val_data)}, Test={len(test_data)}")
    
    train_loader = DataLoader(train_data, batch_size=8, shuffle=True) # Shuffle ok within train set
    val_loader = DataLoader(val_data, batch_size=8, shuffle=False)
    test_loader = DataLoader(test_data, batch_size=8, shuffle=False)
    
    # 4. Initialize Model (15 channels = 3 steps * 5 features)
    model = SupplyChainGNN(in_channels=15, hidden_channels=64, out_channels=1).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=0.005)
    criterion = nn.BCEWithLogitsLoss() # More stable than Sigmoid + BCELoss
    
    # 5. Training Loop
    best_val_loss = float('inf')
    epochs = 30
    
    print("\n   🔄 Training Epochs:")
    for epoch in range(epochs):
        model.train()
        total_loss = 0
        steps = 0
        
        for batch in train_loader:
            batch = batch.to(device)
            optimizer.zero_grad()
            # Pass edge attributes!
            out = model(batch.x, batch.edge_index, batch.edge_attr, batch.batch)
            loss = criterion(out, batch.y)
            loss.backward()
            optimizer.step()
            total_loss += loss.item()
            steps += 1
            
        train_loss = total_loss / steps if steps > 0 else 0
        
        # Validation
        model.eval()
        val_loss = 0
        val_steps = 0
        with torch.no_grad():
            for batch in val_loader:
                batch = batch.to(device)
                out = model(batch.x, batch.edge_index, batch.batch)
                loss = criterion(out, batch.y)
                val_loss += loss.item()
                val_steps += 1
        
        val_loss = val_loss / val_steps if val_steps > 0 else 0
        
        if epoch % 5 == 0:
            print(f"     Epoch {epoch+1}/{epochs} | Train Loss: {train_loss:.4f} | Val Loss: {val_loss:.4f}")
            
        if val_loss < best_val_loss:
            best_val_loss = val_loss
            # Save best model
            project_root = Path(__file__).resolve().parent.parent.parent.parent
            output_dir = project_root / "models"
            output_dir.mkdir(parents=True, exist_ok=True)
            torch.save(model.state_dict(), output_dir / "supply_chain_gnn.pth")
            
    # 6. Final Evaluation & Baseline
    print("\n   📊 Final Test Evaluation:")
    model.load_state_dict(torch.load(output_dir / "supply_chain_gnn.pth"))
    model.eval()
    
    test_loss = 0
    test_steps = 0
    
    # Baseline Calculation (Mean Risk Prediction)
    # Simple baseline: Predict mean risk from Train set for everything
    train_y = torch.cat([data.y for data in train_data])
    mean_risk = train_y.mean().item()
    baseline_loss = 0
    
    with torch.no_grad():
        for batch in test_loader:
            batch = batch.to(device)
            out = model(batch.x, batch.edge_index, batch.batch)
            loss = criterion(out, batch.y)
            test_loss += loss.item()
            
            # Baseline MSE
            # Create a tensor of mean_risk with same shape as batch.y
            baseline_preds = torch.full_like(batch.y, mean_risk)
            b_loss = criterion(baseline_preds, batch.y)
            baseline_loss += b_loss.item()
            
            test_steps += 1
            
    avg_test_loss = test_loss / test_steps
    avg_baseline_loss = baseline_loss / test_steps
    
    print(f"     ✅ GNN Test MSE:      {avg_test_loss:.5f}")
    print(f"     ⚖️ Baseline MSE:      {avg_baseline_loss:.5f}")
    
    if avg_test_loss < avg_baseline_loss:
        print("     🎉 GNN outperforms Baseline!")
    else:
        print("     ⚠️ GNN behaves like random noise or worse than mean.")

if __name__ == "__main__":
    train_gnn()
