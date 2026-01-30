from torch.utils.data import WeightedRandomSampler
import numpy as np
import sys
import os
from pathlib import Path
import json

# Setup Path using helper
try:
    from paths import setup_path
    project_root = setup_path()
except ImportError:
    # Fallback if running directly without paths.py in context? 
    # But files are in same dir.
    current_dir = Path(__file__).resolve().parent
    sys.path.append(str(current_dir))
    from paths import setup_path
    project_root = setup_path()

import torch
import torch.nn as nn
from torch_geometric.loader import DataLoader
from sklearn.metrics import average_precision_score

# Adjusted Imports
from supply_chain.data.dataset_gnn import SupplyChainGraphDataset
from supply_chain.models.gnn import SupplyChainGNN
from supply_chain.simulation.graph import GraphBuilder
from supply_chain.config import REPORTS_DIR

def train_gnn():
    print("Starting GNN Training Pipeline (Refined Strategy)...")
    
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"   Using Device: {device}")
    
    # 1. Load Data
    data_dir = project_root / "data" / "raw"
    
    print(f"   Loading Episodes from: {data_dir}")
    if not data_dir.exists():
        print("Data directory missing.")
        return

    gb = GraphBuilder()
    map_path = project_root / "kroA100.txt"
    if map_path.exists():
        gb.create_from_tsplib(map_path, k_neighbors=6)
    else:
        gb.create_random_graph(30, 4)
        
    dataset = SupplyChainGraphDataset(gb, data_dir=data_dir, time_window_min=60*24)
    # The 'process' method now produces continuous risk values via sigmoid
    dataset.process()
    
    num_episodes = len(dataset.episodes)
    if num_episodes < 3:
        print("Not enough episodes.")
        return

    # --- 1. Audit Target Distribution (Per-Episode) ---
    print("\n   Auditing Episodes (Topology & Distribution Check)...")
    
    for i, ep in enumerate(dataset.episodes):
        y_ep = torch.cat([d.y for d in ep]).view(-1).numpy()
        mean_r = y_ep.mean()
        high_r = (y_ep > 0.8).mean() * 100
        print(f"     Ep {i}: Samples={len(y_ep)}, Mean Risk={mean_r:.3f}, High Risk(>0.8)={high_r:.1f}%")

    # --- 2. Stratified Episode Split ---
    ep_stats = []
    for i, ep in enumerate(dataset.episodes):
        all_y = torch.cat([d.y for d in ep])
        ep_risk = (all_y > 0.8).float().mean().item()
        ep_stats.append((i, ep_risk))
        
    ep_stats.sort(key=lambda x: x[1], reverse=True)
    sorted_indices = [x[0] for x in ep_stats]
    
    train_eps, val_eps, test_eps = [], [], []
    for k, idx in enumerate(sorted_indices):
        if k % 5 == 3: val_eps.append(dataset.episodes[idx])
        elif k % 5 == 4: test_eps.append(dataset.episodes[idx])
        else: train_eps.append(dataset.episodes[idx])
        
    train_data = [s for ep in train_eps for s in ep]
    val_data = [s for ep in val_eps for s in ep]
    test_data = [s for ep in test_eps for s in ep]
    
    print(f"\n   Stratified Split (by Risk Rate):")
    print(f"     Train Eps: {len(train_eps)} | Samples: {len(train_data)}")
    print(f"     Val Eps:   {len(val_eps)}   | Samples: {len(val_data)}")
    print(f"     Test Eps:  {len(test_eps)}  | Samples: {len(test_data)}")

    # --- 3. Normalization (Calibrated on Train Only) ---
    print("   Computing Normalization Stats...")
    if not train_data: return

    all_x = torch.cat([data.x for data in train_data], dim=0)
    all_edge = torch.cat([data.edge_attr for data in train_data], dim=0)
    
    x_mean = all_x.mean(dim=0)
    x_std = all_x.std(dim=0)
    e_mean = all_edge.mean(dim=0)
    e_std = all_edge.std(dim=0)
    
    x_std = torch.where(x_std < 1e-5, torch.ones_like(x_std)*1e-5, x_std)
    e_std = torch.where(e_std < 1e-5, torch.ones_like(e_std)*1e-5, e_std)
    
    cat_indices = [0, 6, 12]
    for idx in cat_indices:
        x_mean[idx] = 0.0
        x_std[idx] = 1.0
    
    scaler_path = project_root / "models" / "gnn_scaler.json"
    stats = {
        "x_mean": x_mean.tolist(), "x_std": x_std.tolist(),
        "edge_mean": e_mean.tolist(), "edge_std": e_std.tolist()
    }
    with open(scaler_path, 'w') as f: json.dump(stats, f)
    
    def normalize_list(data_list, xm, xs, em, es):
        for data in data_list:
            data.x = (data.x - xm) / xs
            data.edge_attr = (data.edge_attr - em) / es
            
    normalize_list(train_data, x_mean, x_std, e_mean, e_std)
    normalize_list(val_data, x_mean, x_std, e_mean, e_std)
    normalize_list(test_data, x_mean, x_std, e_mean, e_std)

    # --- 4. Balancing Strategy: Weighted Sampler Only ---
    train_y = torch.cat([d.y for d in train_data]).view(-1)
    
    print(f"Balancing: Using WeightedRandomSampler (Deciles).")
    
    counts = torch.histc(train_y, bins=5, min=0, max=1)
    print(f"Train Distribution (5 bins): {counts.int().tolist()}")
    
    bin_weights = 1.0 / (counts + 1.0) 
    sample_weights = []
    
    for data in train_data:
        r = data.y.max().item()
        b_idx = min(int(r * 5), 4)
        sample_weights.append(bin_weights[b_idx].item())
        
    sampler = WeightedRandomSampler(sample_weights, num_samples=len(train_data), replacement=True)
    
    train_loader = DataLoader(train_data, batch_size=32, sampler=sampler)
    val_loader = DataLoader(val_data, batch_size=32, shuffle=False)
    test_loader = DataLoader(test_data, batch_size=32, shuffle=False)
    
    # 5. Initialize Model
    model = SupplyChainGNN(in_channels=18, hidden_channels=64, out_channels=1).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=0.005)
    
    criterion = nn.BCEWithLogitsLoss() 
    
    # 6. Training Loop
    best_val_auc = 0.0 
    epochs = 40 
    output_dir = project_root / "models"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    def compute_metrics(loader, model, threshold=0.5):
        targets, preds = [], []
        with torch.no_grad():
            for batch in loader:
                batch = batch.to(device)
                out = model(batch.x, batch.edge_index, batch.edge_attr, batch.batch)
                probs = torch.sigmoid(out)
                targets.extend(batch.y.cpu().numpy())
                preds.extend(probs.cpu().numpy())
        
        targets = np.array(targets).flatten()
        preds = np.array(preds).flatten()
        try:
            ap = average_precision_score((targets > threshold).astype(int), preds)
        except:
            ap = 0.0
        return ap

    print("\n   🔄 Training Epochs:")
    for epoch in range(epochs):
        model.train()
        total_loss = 0
        steps = 0
        
        for batch in train_loader:
            batch = batch.to(device)
            optimizer.zero_grad()
            out = model(batch.x, batch.edge_index, batch.edge_attr, batch.batch)
            loss = criterion(out, batch.y)
            loss.backward()
            optimizer.step()
            total_loss += loss.item()
            steps += 1
            
        train_loss = total_loss / steps if steps > 0 else 0
        
        # Validation Metrics
        model.eval()
        val_auc_05 = compute_metrics(val_loader, model, 0.5)
        val_auc_08 = compute_metrics(val_loader, model, 0.8)
        
        if epoch % 5 == 0:
            print(f"     Epoch {epoch+1}/{epochs} | Loss: {train_loss:.4f} | Val AP@0.5: {val_auc_05:.4f} | AP@0.8: {val_auc_08:.4f}")
            
        if val_auc_08 > best_val_auc:
            best_val_auc = val_auc_08
            torch.save(model.state_dict(), output_dir / "supply_chain_gnn.pth")
            
    # 7. Final Evaluation
    print("\n   📊 Final Test Evaluation:")
    model.load_state_dict(torch.load(output_dir / "supply_chain_gnn.pth", weights_only=True))
    model.eval()
    
    test_ap_05 = compute_metrics(test_loader, model, 0.5)
    test_ap_08 = compute_metrics(test_loader, model, 0.8)
    test_ap_09 = compute_metrics(test_loader, model, 0.9)
    
    print(f"     ✅ Test PR-AUC (>0.5): {test_ap_05:.5f}")
    print(f"     ✅ Test PR-AUC (>0.8): {test_ap_08:.5f}")
    print(f"     ✅ Test PR-AUC (>0.9): {test_ap_09:.5f}")

if __name__ == "__main__":
    train_gnn()
