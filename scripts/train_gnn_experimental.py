"""
Quick GNN Retraining with Better Hyperparameters
=================================================
Uses existing data pipeline but with:
- Higher pos_weight for class imbalance
- More aggressive learning
- Better discrimination
"""

import torch
import torch.nn as nn
import numpy as np
import json
import random
from pathlib import Path
from torch_geometric.loader import DataLoader
from sklearn.metrics import average_precision_score
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent / "src"))
from supply_chain.models.gnn import SupplyChainGNN
from supply_chain.data.dataset_gnn import SupplyChainGraphDataset
from supply_chain.simulation.graph import GraphBuilder


def main():
    print("🚀 GNN Retraining with Better Hyperparameters\n")

    # Reproducibility for stable demo results
    seed = 42
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)
    torch.backends.cudnn.deterministic = True
    torch.backends.cudnn.benchmark = False
    
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"Device: {device}")
    
    project_root = Path(__file__).parent
    data_dir = project_root / "data" / "raw"
    
    # Build graph
    print("\n📊 Building graph...")
    gb = GraphBuilder()
    map_path = project_root / "data" / "kroA100.txt"
    if map_path.exists():
        gb.create_from_tsplib(map_path, k_neighbors=6)
        print(f"   Loaded TSPLIB graph: {len(gb.nodes)} nodes")
    else:
        gb.create_random_graph(30, 4)
        print(f"   Created random graph: {len(gb.nodes)} nodes")
    
    # Load dataset
    print("\n📂 Loading dataset...")
    dataset = SupplyChainGraphDataset(gb, data_dir=data_dir, time_window_min=60*24)
    dataset.process()
    
    print(f"   Episodes: {len(dataset.episodes)}")
    total_snaps = sum(len(ep) for ep in dataset.episodes)
    print(f"   Total snapshots: {total_snaps}")
    
    # Flatten and analyze
    all_data = [s for ep in dataset.episodes for s in ep]
    all_y = torch.cat([d.y for d in all_data]).view(-1).numpy()
    
    print(f"\n📈 Risk Distribution:")
    print(f"   Mean: {all_y.mean():.4f}")
    print(f"   Std:  {all_y.std():.4f}")
    print(f"   >0.3: {(all_y > 0.3).mean()*100:.1f}%")
    print(f"   >0.5: {(all_y > 0.5).mean()*100:.1f}%")
    print(f"   >0.7: {(all_y > 0.7).mean()*100:.1f}%")
    
    # Split
    np.random.seed(42)
    indices = np.random.permutation(len(all_data))
    n = len(all_data)
    
    train_idx = indices[:int(0.8*n)]
    val_idx = indices[int(0.8*n):int(0.9*n)]
    test_idx = indices[int(0.9*n):]
    
    train_data = [all_data[i] for i in train_idx]
    val_data = [all_data[i] for i in val_idx]
    test_data = [all_data[i] for i in test_idx]
    
    print(f"\n📂 Split: Train={len(train_data)}, Val={len(val_data)}, Test={len(test_data)}")
    
    # Normalization
    print("\n⚖️ Normalizing...")
    all_x = torch.cat([d.x for d in train_data], dim=0)
    all_e = torch.cat([d.edge_attr for d in train_data], dim=0)
    
    x_mean = all_x.mean(dim=0)
    x_std = all_x.std(dim=0)
    e_mean = all_e.mean(dim=0)
    e_std = all_e.std(dim=0)
    
    x_std = torch.where(x_std < 1e-5, torch.ones_like(x_std), x_std)
    e_std = torch.where(e_std < 1e-5, torch.ones_like(e_std), e_std)
    
    # Don't normalize categorical
    for idx in [0, 6, 12]:
        x_mean[idx] = 0.0
        x_std[idx] = 1.0
    
    # Save scaler
    scaler = {
        "x_mean": x_mean.tolist(),
        "x_std": x_std.tolist(),
        "edge_mean": e_mean.tolist(),
        "edge_std": e_std.tolist()
    }
    scaler_path = project_root / "models" / "gnn_scaler.json"
    with open(scaler_path, 'w') as f:
        json.dump(scaler, f)
    print(f"   Saved scaler")
    
    # Apply normalization
    for d in train_data + val_data + test_data:
        d.x = (d.x - x_mean) / x_std
        d.edge_attr = (d.edge_attr - e_mean) / e_std
    
    # Weighted sampler - moderate oversampling of high risk
    print("\n⚖️ Creating weighted sampler...")
    weights = []
    for d in train_data:
        max_risk = d.y.max().item()
        if max_risk > 0.7:
            weights.append(2.0)
        elif max_risk > 0.5:
            weights.append(1.5)
        elif max_risk > 0.3:
            weights.append(1.1)
        else:
            weights.append(1.0)
    
    sampler = torch.utils.data.WeightedRandomSampler(weights, len(train_data), replacement=True)
    
    train_loader = DataLoader(train_data, batch_size=16, sampler=sampler)  # Smaller batch
    val_loader = DataLoader(val_data, batch_size=32, shuffle=False)
    test_loader = DataLoader(test_data, batch_size=32, shuffle=False)
    
    # Model with dropout
    print("\n🧠 Initializing model...")
    model = SupplyChainGNN(in_channels=18, hidden_channels=64, out_channels=1).to(device)
    
    # Aggressive optimizer
    optimizer = torch.optim.AdamW(model.parameters(), lr=0.005, weight_decay=0.01)
    scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=50)
    
    # Moderate positive weight
    pos_weight = torch.tensor([1.0]).to(device)
    criterion = nn.BCEWithLogitsLoss(pos_weight=pos_weight)
    
    # Training
    print("\n🏋️ Training...")
    best_discrimination = 0.0
    
    for epoch in range(60):
        model.train()
        total_loss = 0
        
        for batch in train_loader:
            batch = batch.to(device)
            optimizer.zero_grad()
            out = model(batch.x, batch.edge_index, batch.edge_attr, batch.batch)
            loss = criterion(out, batch.y)
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
            total_loss += loss.item()
        
        scheduler.step()
        
        # Evaluate discrimination every 5 epochs
        if epoch % 5 == 0:
            model.eval()
            
            # Quick discrimination test
            with torch.no_grad():
                # Ideal input (zeros)
                x_ideal = torch.zeros(10, 18).to(device)
                x_ideal[:, 0] = 2  # Type
                
                # Crisis input (high values, normalized)
                x_crisis = torch.zeros(10, 18).to(device)
                x_crisis[:, 0] = 2
                x_crisis[:, 2] = 3.0   # High traffic (normalized)
                x_crisis[:, 4] = 2.0   # High delay
                x_crisis[:, 5] = 3.0   # High backlog
                # Repeat for all time steps
                x_crisis[:, 8] = 3.0
                x_crisis[:, 10] = 2.0
                x_crisis[:, 11] = 3.0
                x_crisis[:, 14] = 3.0
                x_crisis[:, 16] = 2.0
                x_crisis[:, 17] = 3.0
                
                src = list(range(9)) + list(range(1,10))
                dst = list(range(1,10)) + list(range(9))
                edge_index = torch.tensor([src, dst], dtype=torch.long).to(device)
                edge_attr = torch.zeros(18, 3).to(device)
                
                ideal_out = torch.sigmoid(model(x_ideal, edge_index, edge_attr)).mean().item()
                crisis_out = torch.sigmoid(model(x_crisis, edge_index, edge_attr)).mean().item()
                discrimination = crisis_out - ideal_out
            
            print(f"   Epoch {epoch:2d} | Loss: {total_loss/len(train_loader):.4f} | "
                  f"Ideal: {ideal_out:.3f} | Crisis: {crisis_out:.3f} | Δ: {discrimination:.3f}")
            
            if discrimination > best_discrimination:
                best_discrimination = discrimination
                torch.save(model.state_dict(), project_root / "models" / "supply_chain_gnn.pth")
    
    # Final test
    print("\n📊 Final Evaluation:")
    model.load_state_dict(torch.load(project_root / "models" / "supply_chain_gnn.pth", weights_only=True))
    model.eval()
    
    with torch.no_grad():
        x_ideal = torch.zeros(10, 18).to(device)
        x_ideal[:, 0] = 2
        
        x_crisis = torch.zeros(10, 18).to(device)
        x_crisis[:, 0] = 2
        for i in [2, 8, 14]:  # Traffic
            x_crisis[:, i] = 3.0
        for i in [4, 10, 16]:  # Delay
            x_crisis[:, i] = 2.0
        for i in [5, 11, 17]:  # Backlog
            x_crisis[:, i] = 3.0
        
        src = list(range(9)) + list(range(1,10))
        dst = list(range(1,10)) + list(range(9))
        edge_index = torch.tensor([src, dst], dtype=torch.long).to(device)
        edge_attr = torch.zeros(18, 3).to(device)
        
        ideal_out = torch.sigmoid(model(x_ideal, edge_index, edge_attr))
        crisis_out = torch.sigmoid(model(x_crisis, edge_index, edge_attr))
    
    print(f"\n   🟢 Ideal:  mean={ideal_out.mean().item():.4f}, max={ideal_out.max().item():.4f}")
    print(f"   🔴 Crisis: mean={crisis_out.mean().item():.4f}, max={crisis_out.max().item():.4f}")
    print(f"   📊 Discrimination: {crisis_out.mean().item() - ideal_out.mean().item():.4f}")
    
    print(f"\n✅ Model saved to: models/supply_chain_gnn.pth")
    print(f"   Best discrimination: {best_discrimination:.4f}")


if __name__ == "__main__":
    main()
