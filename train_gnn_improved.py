"""
Improved GNN Training Script with Synthetic Crisis Data
========================================================
Creates extreme scenarios for better risk discrimination.
"""

import torch
import torch.nn as nn
import numpy as np
import pandas as pd
import json
from pathlib import Path
from torch_geometric.data import Data
from torch_geometric.loader import DataLoader
from sklearn.metrics import average_precision_score
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from supply_chain.gnn.model import SupplyChainGNN
from supply_chain.gnn.dataset import SupplyChainGraphDataset
from supply_chain.simulation.graph import GraphBuilder


def generate_crisis_episodes(base_df: pd.DataFrame, num_episodes: int = 5) -> list:
    """Generate synthetic crisis episodes with extreme values."""
    crisis_dfs = []
    
    for ep_id in range(num_episodes):
        df = base_df.copy()
        
        # Select random nodes to be "in crisis"
        unique_nodes = df['node_id'].unique()
        crisis_nodes = np.random.choice(unique_nodes, size=max(3, len(unique_nodes)//5), replace=False)
        
        # Apply crisis to selected nodes
        mask = df['node_id'].isin(crisis_nodes)
        
        # Extreme traffic (8-10)
        df.loc[mask, 'traffic_congestion_level'] = np.random.uniform(8, 10, mask.sum())
        
        # Bad weather (0.7-1.0)
        df.loc[mask, 'weather_condition_severity'] = np.random.uniform(0.7, 1.0, mask.sum())
        
        # High delays (200-400 min)
        df.loc[mask, 'delivery_time_deviation'] = np.random.uniform(200, 400, mask.sum())
        
        # High backlog (50-200)
        if 'pending_orders_count' in df.columns:
            df.loc[mask, 'pending_orders_count'] = np.random.randint(50, 200, mask.sum())
        
        df['episode_id'] = f'crisis_{ep_id}'
        crisis_dfs.append(df)
        
        print(f"  Crisis episode {ep_id}: {len(crisis_nodes)} nodes in crisis")
    
    return crisis_dfs


def create_balanced_dataset(data_dir: Path, graph_builder) -> tuple:
    """Create dataset with both normal and crisis data."""
    
    print("📊 Loading original episodes...")
    dataset = SupplyChainGraphDataset(graph_builder, data_dir=data_dir, time_window_min=60*24)
    dataset.process()
    
    print(f"   Found {len(dataset.episodes)} original episodes")
    
    # Load one episode for crisis generation
    base_df = pd.read_csv(data_dir / "episode_0.csv")
    
    print("\n🔥 Generating crisis episodes...")
    crisis_dfs = generate_crisis_episodes(base_df, num_episodes=5)
    
    # Process crisis episodes
    print("\n📈 Processing crisis data...")
    crisis_snapshots = []
    for crisis_df in crisis_dfs:
        crisis_dataset = SupplyChainGraphDataset(
            graph_builder, 
            dataframe=crisis_df, 
            time_window_min=60*24
        )
        crisis_dataset.process()
        if crisis_dataset.episodes:
            crisis_snapshots.extend(crisis_dataset.episodes[0])
    
    print(f"   Generated {len(crisis_snapshots)} crisis snapshots")
    
    # Combine all data
    all_snapshots = []
    for ep in dataset.episodes:
        all_snapshots.extend(ep)
    all_snapshots.extend(crisis_snapshots)
    
    print(f"\n📦 Total dataset: {len(all_snapshots)} snapshots")
    
    return all_snapshots, dataset


def train_improved_gnn():
    print("🚀 Starting IMPROVED GNN Training\n")
    print("=" * 50)
    
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"Device: {device}")
    
    project_root = Path(__file__).parent
    data_dir = project_root / "data" / "raw"
    
    # Build graph
    gb = GraphBuilder()
    map_path = project_root / "kroA100.txt"
    if map_path.exists():
        gb.create_from_tsplib(map_path, k_neighbors=6)
    else:
        gb.create_random_graph(30, 4)
    
    # Create balanced dataset
    all_data, original_dataset = create_balanced_dataset(data_dir, gb)
    
    # Analyze risk distribution
    print("\n📊 Risk Distribution Analysis:")
    all_y = torch.cat([d.y for d in all_data]).view(-1).numpy()
    print(f"   Mean: {all_y.mean():.3f}")
    print(f"   Std:  {all_y.std():.3f}")
    print(f"   Min:  {all_y.min():.3f}")
    print(f"   Max:  {all_y.max():.3f}")
    print(f"   High Risk (>0.5): {(all_y > 0.5).mean()*100:.1f}%")
    print(f"   Very High (>0.8): {(all_y > 0.8).mean()*100:.1f}%")
    
    # Split data (80/10/10)
    np.random.shuffle(all_data)
    n = len(all_data)
    train_data = all_data[:int(0.8*n)]
    val_data = all_data[int(0.8*n):int(0.9*n)]
    test_data = all_data[int(0.9*n):]
    
    print(f"\n📂 Split: Train={len(train_data)}, Val={len(val_data)}, Test={len(test_data)}")
    
    # Compute normalization stats (ONLY on train)
    print("\n⚖️ Computing normalization stats...")
    all_x = torch.cat([d.x for d in train_data], dim=0)
    all_e = torch.cat([d.edge_attr for d in train_data], dim=0)
    
    x_mean = all_x.mean(dim=0)
    x_std = all_x.std(dim=0)
    e_mean = all_e.mean(dim=0)
    e_std = all_e.std(dim=0)
    
    # Prevent division by zero
    x_std = torch.where(x_std < 1e-5, torch.ones_like(x_std), x_std)
    e_std = torch.where(e_std < 1e-5, torch.ones_like(e_std), e_std)
    
    # Don't normalize categorical (Type) - indices 0, 6, 12
    for idx in [0, 6, 12]:
        x_mean[idx] = 0.0
        x_std[idx] = 1.0
    
    # Save scaler
    scaler_path = project_root / "models" / "gnn_scaler.json"
    scaler = {
        "x_mean": x_mean.tolist(),
        "x_std": x_std.tolist(),
        "edge_mean": e_mean.tolist(),
        "edge_std": e_std.tolist()
    }
    with open(scaler_path, 'w') as f:
        json.dump(scaler, f)
    print(f"   Saved scaler to {scaler_path}")
    
    # Normalize all data
    def normalize(data_list):
        for d in data_list:
            d.x = (d.x - x_mean) / x_std
            d.edge_attr = (d.edge_attr - e_mean) / e_std
    
    normalize(train_data)
    normalize(val_data)
    normalize(test_data)
    
    # Create weighted sampler for balanced batches
    train_y = torch.cat([d.y for d in train_data]).view(-1)
    weights = torch.zeros(len(train_data))
    
    for i, d in enumerate(train_data):
        max_risk = d.y.max().item()
        # Higher weight for high-risk samples
        if max_risk > 0.8:
            weights[i] = 5.0
        elif max_risk > 0.5:
            weights[i] = 2.0
        else:
            weights[i] = 1.0
    
    sampler = torch.utils.data.WeightedRandomSampler(weights, len(train_data), replacement=True)
    
    train_loader = DataLoader(train_data, batch_size=32, sampler=sampler)
    val_loader = DataLoader(val_data, batch_size=32, shuffle=False)
    test_loader = DataLoader(test_data, batch_size=32, shuffle=False)
    
    # Model
    print("\n🧠 Initializing Model...")
    model = SupplyChainGNN(in_channels=18, hidden_channels=64, out_channels=1).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001, weight_decay=1e-4)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer, patience=5, factor=0.5)
    
    # Loss with positive weight to emphasize high-risk
    pos_weight = torch.tensor([3.0]).to(device)
    criterion = nn.BCEWithLogitsLoss(pos_weight=pos_weight)
    
    # Training
    print("\n🏋️ Training...")
    best_val_ap = 0.0
    patience_counter = 0
    max_patience = 15
    
    for epoch in range(100):
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
        
        # Validation
        model.eval()
        val_preds, val_targets = [], []
        with torch.no_grad():
            for batch in val_loader:
                batch = batch.to(device)
                out = model(batch.x, batch.edge_index, batch.edge_attr, batch.batch)
                val_preds.extend(torch.sigmoid(out).cpu().numpy())
                val_targets.extend(batch.y.cpu().numpy())
        
        val_preds = np.array(val_preds).flatten()
        val_targets = np.array(val_targets).flatten()
        
        # AP at different thresholds
        val_ap_05 = average_precision_score((val_targets > 0.5).astype(int), val_preds)
        val_ap_08 = average_precision_score((val_targets > 0.8).astype(int), val_preds)
        
        scheduler.step(1 - val_ap_08)
        
        if epoch % 5 == 0:
            print(f"   Epoch {epoch:3d} | Loss: {total_loss/len(train_loader):.4f} | "
                  f"Val AP@0.5: {val_ap_05:.4f} | AP@0.8: {val_ap_08:.4f}")
        
        # Early stopping on AP@0.8
        if val_ap_08 > best_val_ap:
            best_val_ap = val_ap_08
            patience_counter = 0
            torch.save(model.state_dict(), project_root / "models" / "supply_chain_gnn.pth")
        else:
            patience_counter += 1
            if patience_counter >= max_patience:
                print(f"   Early stopping at epoch {epoch}")
                break
    
    # Final evaluation
    print("\n📊 Final Evaluation:")
    model.load_state_dict(torch.load(project_root / "models" / "supply_chain_gnn.pth"))
    model.eval()
    
    test_preds, test_targets = [], []
    with torch.no_grad():
        for batch in test_loader:
            batch = batch.to(device)
            out = model(batch.x, batch.edge_index, batch.edge_attr, batch.batch)
            test_preds.extend(torch.sigmoid(out).cpu().numpy())
            test_targets.extend(batch.y.cpu().numpy())
    
    test_preds = np.array(test_preds).flatten()
    test_targets = np.array(test_targets).flatten()
    
    print(f"   Test AP@0.5: {average_precision_score((test_targets > 0.5).astype(int), test_preds):.4f}")
    print(f"   Test AP@0.8: {average_precision_score((test_targets > 0.8).astype(int), test_preds):.4f}")
    
    # Test discrimination
    print("\n🧪 Discrimination Test:")
    # Create ideal vs crisis input
    N = 10
    x_ideal = torch.zeros(N, 18)
    x_ideal[:, 0] = 2  # Type
    
    x_crisis = torch.zeros(N, 18)
    x_crisis[:, 0] = 2
    x_crisis[:, 2] = (10 - x_mean[2]) / x_std[2]  # High traffic (normalized)
    x_crisis[:, 4] = (200 - x_mean[4]) / x_std[4]  # High delay
    x_crisis[:, 5] = (100 - x_mean[5]) / x_std[5]  # High backlog
    
    # Simple edge index
    src = list(range(9)) + list(range(1,10))
    dst = list(range(1,10)) + list(range(9))
    edge_index = torch.tensor([src, dst], dtype=torch.long)
    edge_attr = torch.zeros(18, 3)
    edge_attr_norm = (edge_attr - e_mean) / e_std
    
    with torch.no_grad():
        ideal_out = torch.sigmoid(model(x_ideal, edge_index, edge_attr_norm))
        crisis_out = torch.sigmoid(model(x_crisis, edge_index, edge_attr_norm))
    
    print(f"   Ideal conditions:  mean={ideal_out.mean().item():.4f}")
    print(f"   Crisis conditions: mean={crisis_out.mean().item():.4f}")
    print(f"   Discrimination:    {crisis_out.mean().item() - ideal_out.mean().item():.4f}")
    
    print("\n✅ Training complete!")
    print(f"   Model saved to: {project_root / 'models' / 'supply_chain_gnn.pth'}")


if __name__ == "__main__":
    train_improved_gnn()
