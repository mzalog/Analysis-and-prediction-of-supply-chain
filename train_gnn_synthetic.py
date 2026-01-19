"""
GNN Training with Synthetic Contrast Data
==========================================
Creates training data with clear distinction between:
- IDEAL: All features near zero (low risk)
- CRISIS: High traffic, backlog, delay (high risk)

This ensures the model learns to discriminate properly.
"""

import torch
import torch.nn as nn
import numpy as np
import json
from pathlib import Path
from torch_geometric.data import Data
from torch_geometric.loader import DataLoader
import sys

sys.path.insert(0, str(Path(__file__).parent / "src"))

from supply_chain.gnn.model import SupplyChainGNN
from supply_chain.simulation.graph import GraphBuilder
from supply_chain.simulation.schema import NodeType


def create_synthetic_dataset(graph_builder, num_samples=1000):
    """
    Creates synthetic graph snapshots with clear risk labels.
    
    Features per node (6):
    [0] Type (0-4)
    [1] Load (0-50)
    [2] Traffic (0-10)
    [3] Weather (0-1)
    [4] Delay (0-300)
    [5] Backlog (0-200)
    """
    graph = graph_builder.graph
    sorted_nodes = sorted(graph.nodes())
    node_mapping = {n: i for i, n in enumerate(sorted_nodes)}
    num_nodes = len(sorted_nodes)
    
    # Build edge index
    src, dst = [], []
    for u, v in sorted(graph.edges()):
        if u in node_mapping and v in node_mapping:
            src.append(node_mapping[u])
            dst.append(node_mapping[v])
            src.append(node_mapping[v])
            dst.append(node_mapping[u])
    edge_index = torch.tensor([src, dst], dtype=torch.long)
    num_edges = len(src)
    
    # Get node types
    node_types = []
    for node_id in sorted_nodes:
        node_data = graph.nodes[node_id]['data']
        if node_data.type == NodeType.WAREHOUSE: t = 1
        elif node_data.type == NodeType.HUB: t = 2
        elif node_data.type == NodeType.PORT: t = 3
        elif node_data.type == NodeType.CUSTOMER: t = 4
        else: t = 0
        node_types.append(t)
    node_types = torch.tensor(node_types, dtype=torch.float)
    
    data_list = []
    
    for i in range(num_samples):
        # Decide sample type
        sample_type = np.random.choice(['ideal', 'normal', 'elevated', 'crisis'], 
                                        p=[0.25, 0.35, 0.25, 0.15])
        
        # Initialize features
        x = torch.zeros(num_nodes, 6)
        x[:, 0] = node_types  # Type stays constant
        
        # Target risk
        y = torch.zeros(num_nodes, 1)
        
        if sample_type == 'ideal':
            # All features low/zero
            x[:, 1] = torch.rand(num_nodes) * 2  # Load 0-2
            x[:, 2] = torch.rand(num_nodes) * 1  # Traffic 0-1
            x[:, 3] = torch.rand(num_nodes) * 0.2  # Weather 0-0.2
            x[:, 4] = torch.rand(num_nodes) * 20  # Delay 0-20
            x[:, 5] = torch.rand(num_nodes) * 5  # Backlog 0-5
            y[:] = torch.rand(num_nodes, 1) * 0.15  # Risk 0-0.15
            
        elif sample_type == 'normal':
            # Normal operating conditions
            x[:, 1] = torch.rand(num_nodes) * 10  # Load 0-10
            x[:, 2] = torch.rand(num_nodes) * 4  # Traffic 0-4
            x[:, 3] = torch.rand(num_nodes) * 0.4  # Weather 0-0.4
            x[:, 4] = torch.rand(num_nodes) * 80 + 20  # Delay 20-100
            x[:, 5] = torch.rand(num_nodes) * 30  # Backlog 0-30
            y[:] = torch.rand(num_nodes, 1) * 0.2 + 0.15  # Risk 0.15-0.35
            
        elif sample_type == 'elevated':
            # Some nodes have elevated risk
            x[:, 1] = torch.rand(num_nodes) * 15  # Load 0-15
            x[:, 2] = torch.rand(num_nodes) * 6  # Traffic 0-6
            x[:, 3] = torch.rand(num_nodes) * 0.6  # Weather 0-0.6
            x[:, 4] = torch.rand(num_nodes) * 100 + 50  # Delay 50-150
            x[:, 5] = torch.rand(num_nodes) * 50  # Backlog 0-50
            
            # Random subset gets high risk
            high_risk_nodes = torch.rand(num_nodes) > 0.7
            y[high_risk_nodes] = torch.rand(high_risk_nodes.sum(), 1) * 0.3 + 0.5  # Risk 0.5-0.8
            y[~high_risk_nodes] = torch.rand((~high_risk_nodes).sum(), 1) * 0.25 + 0.25  # Risk 0.25-0.5
            
        else:  # crisis
            # High stress across the board, some nodes critical
            x[:, 1] = torch.rand(num_nodes) * 30 + 10  # Load 10-40
            x[:, 2] = torch.rand(num_nodes) * 5 + 5  # Traffic 5-10
            x[:, 3] = torch.rand(num_nodes) * 0.5 + 0.5  # Weather 0.5-1.0
            x[:, 4] = torch.rand(num_nodes) * 150 + 100  # Delay 100-250
            x[:, 5] = torch.rand(num_nodes) * 100 + 50  # Backlog 50-150
            
            # Most nodes high risk
            y[:] = torch.rand(num_nodes, 1) * 0.25 + 0.7  # Risk 0.7-0.95
        
        # Stack for temporal (3 steps - same values, slight noise)
        x_t1 = x.clone()
        x_t2 = x.clone() + torch.randn_like(x) * 0.1 * x.abs().mean()
        x_t3 = x.clone() + torch.randn_like(x) * 0.1 * x.abs().mean()
        x_stacked = torch.cat([x_t1, x_t2, x_t3], dim=1)  # [N, 18]
        
        # Edge attributes (static + noise)
        edge_attr = torch.rand(num_edges, 3) * 0.5 + 0.25  # [0.25, 0.75]
        
        data = Data(x=x_stacked, edge_index=edge_index, edge_attr=edge_attr, y=y)
        data_list.append(data)
    
    return data_list


def main():
    print("🚀 GNN Training with Synthetic Contrast Data\n")
    print("=" * 50)
    
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"Device: {device}")
    
    project_root = Path(__file__).parent
    
    # Build graph (same as app.py uses)
    print("\n📊 Building graph...")
    gb = GraphBuilder()
    map_path = project_root / "kroA100.txt"
    if map_path.exists():
        gb.create_from_tsplib(map_path, k_neighbors=6)
        print(f"   Loaded TSPLIB: {len(gb.nodes)} nodes")
    else:
        gb.create_random_graph(30, 4)
        print(f"   Random graph: {len(gb.nodes)} nodes")
    
    # Generate synthetic data
    print("\n🔧 Generating synthetic training data...")
    all_data = create_synthetic_dataset(gb, num_samples=2000)
    
    # Analyze distribution
    all_y = torch.cat([d.y for d in all_data]).view(-1).numpy()
    print(f"   Samples: {len(all_data)}")
    print(f"   Risk distribution:")
    print(f"      <0.2 (Ideal):    {(all_y < 0.2).mean()*100:.1f}%")
    print(f"      0.2-0.5 (Normal): {((all_y >= 0.2) & (all_y < 0.5)).mean()*100:.1f}%")
    print(f"      0.5-0.7 (Elevated): {((all_y >= 0.5) & (all_y < 0.7)).mean()*100:.1f}%")
    print(f"      >0.7 (Crisis):   {(all_y >= 0.7).mean()*100:.1f}%")
    
    # Split
    np.random.shuffle(all_data)
    n = len(all_data)
    train_data = all_data[:int(0.8*n)]
    val_data = all_data[int(0.8*n):int(0.9*n)]
    test_data = all_data[int(0.9*n):]
    
    print(f"\n📂 Split: Train={len(train_data)}, Val={len(val_data)}, Test={len(test_data)}")
    
    # Compute normalization on TRAIN only
    print("\n⚖️ Computing normalization...")
    all_x = torch.cat([d.x for d in train_data], dim=0)
    all_e = torch.cat([d.edge_attr for d in train_data], dim=0)
    
    x_mean = all_x.mean(dim=0)
    x_std = all_x.std(dim=0)
    e_mean = all_e.mean(dim=0)
    e_std = all_e.std(dim=0)
    
    # Prevent division by zero
    x_std = torch.where(x_std < 1e-5, torch.ones_like(x_std), x_std)
    e_std = torch.where(e_std < 1e-5, torch.ones_like(e_std), e_std)
    
    # Don't normalize categorical Type features (indices 0, 6, 12)
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
    print(f"   Saved scaler to {scaler_path}")
    
    # Normalize all data
    for d in train_data + val_data + test_data:
        d.x = (d.x - x_mean) / x_std
        d.edge_attr = (d.edge_attr - e_mean) / e_std
    
    # Data loaders
    train_loader = DataLoader(train_data, batch_size=32, shuffle=True)
    val_loader = DataLoader(val_data, batch_size=32, shuffle=False)
    test_loader = DataLoader(test_data, batch_size=32, shuffle=False)
    
    # Model
    print("\n🧠 Initializing model...")
    model = SupplyChainGNN(in_channels=18, hidden_channels=64, out_channels=1).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer, patience=10, factor=0.5)
    criterion = nn.MSELoss()  # Regression on risk score
    
    # Training
    print("\n🏋️ Training...")
    best_val_loss = float('inf')
    patience_counter = 0
    
    for epoch in range(100):
        model.train()
        total_loss = 0
        
        for batch in train_loader:
            batch = batch.to(device)
            optimizer.zero_grad()
            out = model(batch.x, batch.edge_index, batch.edge_attr, batch.batch)
            # Use sigmoid for output, MSE for loss
            pred = torch.sigmoid(out)
            loss = criterion(pred, batch.y)
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
            total_loss += loss.item()
        
        # Validation
        model.eval()
        val_loss = 0
        with torch.no_grad():
            for batch in val_loader:
                batch = batch.to(device)
                out = model(batch.x, batch.edge_index, batch.edge_attr, batch.batch)
                pred = torch.sigmoid(out)
                val_loss += criterion(pred, batch.y).item()
        val_loss /= len(val_loader)
        
        scheduler.step(val_loss)
        
        # Discrimination test every 10 epochs
        if epoch % 10 == 0:
            model.eval()
            with torch.no_grad():
                # Test on first batch from test set
                batch = next(iter(test_loader)).to(device)
                out = model(batch.x, batch.edge_index, batch.edge_attr, batch.batch)
                pred = torch.sigmoid(out)
                
                # Find ideal vs crisis nodes by their target
                ideal_mask = batch.y < 0.2
                crisis_mask = batch.y > 0.7
                
                ideal_pred = pred[ideal_mask].mean().item() if ideal_mask.sum() > 0 else 0
                crisis_pred = pred[crisis_mask].mean().item() if crisis_mask.sum() > 0 else 0
                
            print(f"   Epoch {epoch:3d} | Loss: {total_loss/len(train_loader):.4f} | "
                  f"Val: {val_loss:.4f} | Ideal: {ideal_pred:.3f} | Crisis: {crisis_pred:.3f} | "
                  f"Δ: {crisis_pred - ideal_pred:.3f}")
        
        # Early stopping
        if val_loss < best_val_loss:
            best_val_loss = val_loss
            patience_counter = 0
            torch.save(model.state_dict(), project_root / "models" / "supply_chain_gnn.pth")
        else:
            patience_counter += 1
            if patience_counter >= 20:
                print(f"   Early stopping at epoch {epoch}")
                break
    
    # Final evaluation
    print("\n📊 Final Evaluation:")
    model.load_state_dict(torch.load(project_root / "models" / "supply_chain_gnn.pth", weights_only=True))
    model.eval()
    
    # Test discrimination with synthetic ideal vs crisis
    print("\n🧪 Discrimination Test (Synthetic):")
    
    graph = gb.graph
    sorted_nodes = sorted(graph.nodes())
    num_nodes = len(sorted_nodes)
    
    # Get real node types
    node_types = []
    for node_id in sorted_nodes:
        node_data = graph.nodes[node_id]['data']
        if node_data.type == NodeType.WAREHOUSE: t = 1
        elif node_data.type == NodeType.HUB: t = 2
        elif node_data.type == NodeType.PORT: t = 3
        elif node_data.type == NodeType.CUSTOMER: t = 4
        else: t = 0
        node_types.append(t)
    
    # Build edge index
    src, dst = [], []
    for u, v in sorted(graph.edges()):
        src.append(sorted_nodes.index(u))
        dst.append(sorted_nodes.index(v))
        src.append(sorted_nodes.index(v))
        dst.append(sorted_nodes.index(u))
    edge_index = torch.tensor([src, dst], dtype=torch.long).to(device)
    edge_attr = torch.ones(len(src), 3).to(device) * 0.5
    edge_attr = (edge_attr - torch.tensor(scaler['edge_mean']).to(device)) / torch.tensor(scaler['edge_std']).to(device)
    
    # IDEAL input (zeros except type)
    x_ideal = torch.zeros(num_nodes, 6)
    x_ideal[:, 0] = torch.tensor(node_types)
    x_ideal_stacked = torch.cat([x_ideal]*3, dim=1).to(device)
    x_ideal_norm = (x_ideal_stacked - torch.tensor(scaler['x_mean']).to(device)) / torch.tensor(scaler['x_std']).to(device)
    
    # CRISIS input (high values)
    x_crisis = torch.zeros(num_nodes, 6)
    x_crisis[:, 0] = torch.tensor(node_types)
    x_crisis[:, 1] = 20.0   # Load
    x_crisis[:, 2] = 8.0    # Traffic
    x_crisis[:, 3] = 0.8    # Weather
    x_crisis[:, 4] = 150.0  # Delay
    x_crisis[:, 5] = 100.0  # Backlog
    x_crisis_stacked = torch.cat([x_crisis]*3, dim=1).to(device)
    x_crisis_norm = (x_crisis_stacked - torch.tensor(scaler['x_mean']).to(device)) / torch.tensor(scaler['x_std']).to(device)
    
    with torch.no_grad():
        ideal_out = torch.sigmoid(model(x_ideal_norm, edge_index, edge_attr))
        crisis_out = torch.sigmoid(model(x_crisis_norm, edge_index, edge_attr))
    
    print(f"   🟢 Ideal:  mean={ideal_out.mean().item():.4f}, max={ideal_out.max().item():.4f}")
    print(f"   🔴 Crisis: mean={crisis_out.mean().item():.4f}, max={crisis_out.max().item():.4f}")
    print(f"   📊 Discrimination: {crisis_out.mean().item() - ideal_out.mean().item():.4f}")
    
    # Recommended calibration
    ideal_mean = ideal_out.mean().item()
    crisis_mean = crisis_out.mean().item()
    
    print(f"\n📝 Recommended calibration for app.py:")
    print(f"   baseline = {ideal_mean:.2f}")
    print(f"   contrast = {0.7 / max(0.01, crisis_mean - ideal_mean):.1f}")
    print(f"   target_baseline = 0.15")
    
    print(f"\n✅ Model saved to: models/supply_chain_gnn.pth")


if __name__ == "__main__":
    main()
