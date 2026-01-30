
import sys
import torch
import torch.nn.functional as F
import numpy as np
from pathlib import Path
import json

# Setup Path
try:
    from paths import setup_path
    project_root = setup_path()
except ImportError:
    current_dir = Path(__file__).resolve().parent
    sys.path.append(str(current_dir))
    from paths import setup_path
    project_root = setup_path()

from supply_chain.simulation.graph import GraphBuilder
from supply_chain.data.dataset_gnn_v2 import SupplyChainGraphDatasetV2
from supply_chain.models.gnn import SupplyChainGNN
from supply_chain.config import DATA_RAW_DIR

def train_v2():
    print("Starting GNN V2 Training (Delay Regression)...")
    
    # 1. Prepare Data
    gb = GraphBuilder()
    tsp_path = project_root / "data" / "kroA100.txt"
    if not tsp_path.exists(): tsp_path = project_root / "kroA100.txt"
    gb.create_from_tsplib(tsp_path, k_neighbors=4)
    
    # Update path to where run_scenarios.py actually outputs data
    data_dir = project_root / "reports" / "experiments" / "scenarios"
    dataset = SupplyChainGraphDatasetV2(gb, data_dir=data_dir, time_window_min=60)
    dataset.process()
    
    if not dataset.snapshots:
        print("No snapshots found! Run run_scenarios.py first.")
        return

    # 2. Statistics & Scaling
    all_x = torch.cat([d.x for d in dataset.snapshots], dim=0)
    all_edge = torch.cat([d.edge_attr for d in dataset.snapshots], dim=0)
    all_y = torch.cat([d.y for d in dataset.snapshots], dim=0)
    
    x_mean = all_x.mean(dim=0)
    x_std = all_x.std(dim=0) + 1e-6
    edge_mean = all_edge.mean(dim=0)
    edge_std = all_edge.std(dim=0) + 1e-6
    
    y_mean = all_y.mean(dim=0)
    y_std = all_y.std(dim=0)
    
    print(f"Stats: X_mean={x_mean[:3]}...")
    print(f"Stats: Y_mean={y_mean.item():.4f} (Log Space), Y_max={all_y.max().item():.4f}")
    
    # Save Scaler
    scaler = {
        "x_mean": x_mean.tolist(), "x_std": x_std.tolist(),
        "edge_mean": edge_mean.tolist(), "edge_std": edge_std.tolist(),
        "y_log_mean": y_mean.item(), "y_log_std": y_std.item()
    }
    
    models_dir = project_root / "models"
    models_dir.mkdir(parents=True, exist_ok=True)
    with open(models_dir / "gnn_v2_scaler.json", "w") as f:
        json.dump(scaler, f)

    # 3. Model Setup
    # 3 steps * 6 features = 18 input channels
    model = SupplyChainGNN(in_channels=18, hidden_channels=64, out_channels=1)
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001, weight_decay=5e-4)
    criterion = torch.nn.SmoothL1Loss(beta=0.2) # Robust regression loss
    
    train_loader = dataset.get_loader(batch_size=32)
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model = model.to(device)
    
    # Move stats to device
    xm, xs = x_mean.to(device), x_std.to(device)
    em, es = edge_mean.to(device), edge_std.to(device)

    # 4. Training Loop
    model.train()
    best_loss = float('inf')
    
    for epoch in range(30):
        total_loss = 0
        total_mae_minutes = 0
        steps = 0
        
        for batch in train_loader:
            batch = batch.to(device)
            optimizer.zero_grad()
            
            # Normalize
            batch.x = (batch.x - xm) / xs
            batch.edge_attr = (batch.edge_attr - em) / es
            
            out = model(batch.x, batch.edge_index, batch.edge_attr)
            
            loss = criterion(out, batch.y)
            loss.backward()
            optimizer.step()
            
            total_loss += loss.item()
            
            # Calculate Interpretable MAE (Minutes)
            # Pred -> explm1 -> minutes
            # Target -> explm1 -> minutes
            pred_min = torch.expm1(out.clamp(min=0.0))
            target_min = torch.expm1(batch.y)
            mae_min = (pred_min - target_min).abs().mean().item()
            
            total_mae_minutes += mae_min
            steps += 1
            
        avg_loss = total_loss / steps
        avg_mae = total_mae_minutes / steps
        print(f"Epoch {epoch+1:02d} | Loss: {avg_loss:.4f} | MAE: {avg_mae:.1f} min")
        
        if avg_loss < best_loss:
            best_loss = avg_loss
            torch.save(model.state_dict(), models_dir / "supply_chain_gnn_v2.pth")
            
    print(f"Training Complete (V2). Model saved to {models_dir / 'supply_chain_gnn_v2.pth'}")

if __name__ == "__main__":
    train_v2()
