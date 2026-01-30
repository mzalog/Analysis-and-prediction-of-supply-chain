"""
GNN Diagnostic Evaluation Script

Comprehensive tests to verify if GNN actually learns from graph structure:
1. Temporal holdout split (no future leakage)
2. Baseline comparisons (Constant, MLP without graph)
3. Feature ablation (with/without delay feature)
4. Metrics reporting (MAE, RMSE, R²)
"""

import sys
import os
from pathlib import Path

current_dir = Path(__file__).resolve().parent
src_path = current_dir.parent.parent.parent / "src"
if str(src_path) not in sys.path:
    sys.path.append(str(src_path))

import torch
import torch.nn as nn
import torch.nn.functional as F
import numpy as np
import pandas as pd
from dataclasses import dataclass
from typing import List, Tuple, Dict, Optional
from torch_geometric.data import Data
from torch_geometric.loader import DataLoader

from supply_chain.gnn.dataset import SupplyChainGraphDataset
from supply_chain.gnn.model import SupplyChainGNN
from supply_chain.simulation.graph import GraphBuilder
from supply_chain.config import REPORTS_DIR


@dataclass
class EvalMetrics:
    mae: float
    rmse: float
    r2: float
    
    def __str__(self):
        return f"MAE={self.mae:.4f}, RMSE={self.rmse:.4f}, R²={self.r2:.4f}"


class MLPBaseline(nn.Module):
    """MLP without graph structure - uses only node features."""
    def __init__(self, in_channels=5, hidden_channels=64, out_channels=1):
        super().__init__()
        self.fc1 = nn.Linear(in_channels, hidden_channels)
        self.fc2 = nn.Linear(hidden_channels, hidden_channels)
        self.fc3 = nn.Linear(hidden_channels, out_channels)
        self.sigmoid = nn.Sigmoid()
    
    def forward(self, x, edge_index=None, batch=None):
        # Ignore edge_index - no message passing
        x = F.relu(self.fc1(x))
        x = F.dropout(x, p=0.2, training=self.training)
        x = F.relu(self.fc2(x))
        x = self.sigmoid(self.fc3(x))
        return x


class ConstantBaseline:
    """Predicts mean of training set."""
    def __init__(self):
        self.mean_val = 0.0
        
    def fit(self, train_snapshots: List[Data]):
        all_y = torch.cat([s.y for s in train_snapshots])
        self.mean_val = all_y.mean().item()
        
    def predict(self, snapshot: Data) -> torch.Tensor:
        return torch.full_like(snapshot.y, self.mean_val)


def compute_metrics(y_true: torch.Tensor, y_pred: torch.Tensor) -> EvalMetrics:
    y_true = y_true.flatten()
    y_pred = y_pred.flatten()
    
    mae = torch.abs(y_true - y_pred).mean().item()
    rmse = torch.sqrt(((y_true - y_pred) ** 2).mean()).item()
    
    ss_res = ((y_true - y_pred) ** 2).sum()
    ss_tot = ((y_true - y_true.mean()) ** 2).sum()
    r2 = (1 - ss_res / (ss_tot + 1e-8)).item()
    
    return EvalMetrics(mae=mae, rmse=rmse, r2=r2)


def temporal_split(snapshots: List[Data], train_frac=0.7, val_frac=0.15) -> Tuple[List, List, List]:
    """Split snapshots chronologically (no shuffle)."""
    n = len(snapshots)
    train_end = int(n * train_frac)
    val_end = int(n * (train_frac + val_frac))
    
    return snapshots[:train_end], snapshots[train_end:val_end], snapshots[val_end:]


def remove_delay_feature(snapshot: Data) -> Data:
    """Creates copy with delay feature (index 4) zeroed out."""
    x_new = snapshot.x.clone()
    x_new[:, 4] = 0.0
    return Data(x=x_new, edge_index=snapshot.edge_index, y=snapshot.y)


def train_model(model, train_loader, device, epochs=20, lr=0.01):
    """Train a model and return it."""
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    criterion = nn.MSELoss()
    model.train()
    
    for epoch in range(epochs):
        total_loss = 0
        for batch in train_loader:
            batch = batch.to(device)
            optimizer.zero_grad()
            out = model(batch.x, batch.edge_index, batch.batch)
            loss = criterion(out, batch.y)
            loss.backward()
            optimizer.step()
            total_loss += loss.item()
    
    return model


def evaluate_model(model, test_snapshots: List[Data], device) -> EvalMetrics:
    """Evaluate model on test set."""
    model.eval()
    all_y_true = []
    all_y_pred = []
    
    with torch.no_grad():
        for snapshot in test_snapshots:
            snapshot = snapshot.to(device)
            pred = model(snapshot.x, snapshot.edge_index, None)
            all_y_true.append(snapshot.y)
            all_y_pred.append(pred)
    
    y_true = torch.cat(all_y_true)
    y_pred = torch.cat(all_y_pred)
    
    return compute_metrics(y_true, y_pred)


def run_diagnostics():
    """Main diagnostic runner."""
    print("=" * 60)
    print("GNN DIAGNOSTIC EVALUATION")
    print("=" * 60)
    
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"\n[DEVICE] {device}")
    
    # 1. Load Data
    data_path = Path(REPORTS_DIR) / "experiments" / "long_term_5y" / "simulated_data_5y.csv"
    if not data_path.exists():
        data_path = Path(REPORTS_DIR) / "experiments" / "experiment_massive" / "simulated_data.csv"
    
    print(f"\n[DATA] Loading data from: {data_path}")
    df = pd.read_csv(data_path)
    print(f"   Rows: {len(df):,}")
    
    # 2. Build Graph
    gb = GraphBuilder()
    map_path = Path("kroA100.txt")
    if map_path.exists():
        gb.create_from_tsplib(map_path, k_neighbors=4)
    else:
        gb.create_random_graph(30, 4)
    print(f"   Nodes: {len(gb.nodes)}, Edges: {gb.graph.number_of_edges()}")
    
    # 3. Create Snapshots
    print("\n[SNAPSHOTS] Creating temporal snapshots...")
    dataset = SupplyChainGraphDataset(gb, df, time_window_min=60*24)
    dataset.process()
    snapshots = dataset.snapshots
    print(f"   Total snapshots: {len(snapshots)}")
    
    # 4. Temporal Split
    train_snaps, val_snaps, test_snaps = temporal_split(snapshots)
    print(f"\n[SPLIT] Temporal Split:")
    print(f"   Train: {len(train_snaps)}, Val: {len(val_snaps)}, Test: {len(test_snaps)}")
    
    results = {}
    
    # =========================================================================
    # TEST 1: Constant Baseline
    # =========================================================================
    print("\n" + "-" * 40)
    print("TEST 1: Constant Baseline (Mean)")
    print("-" * 40)
    
    const_baseline = ConstantBaseline()
    const_baseline.fit(train_snaps)
    print(f"   Mean value: {const_baseline.mean_val:.4f}")
    
    all_y_true = torch.cat([s.y for s in test_snaps])
    all_y_pred = torch.cat([const_baseline.predict(s) for s in test_snaps])
    const_metrics = compute_metrics(all_y_true, all_y_pred)
    results["Constant"] = const_metrics
    print(f"   [OK] {const_metrics}")
    
    # =========================================================================
    # TEST 2: MLP Baseline (No Graph)
    # =========================================================================
    print("\n" + "-" * 40)
    print("TEST 2: MLP Baseline (No Graph Structure)")
    print("-" * 40)
    
    mlp_model = MLPBaseline(in_channels=5, hidden_channels=64, out_channels=1).to(device)
    train_loader = DataLoader(train_snaps, batch_size=8, shuffle=True)
    mlp_model = train_model(mlp_model, train_loader, device, epochs=20)
    mlp_metrics = evaluate_model(mlp_model, test_snaps, device)
    results["MLP (no graph)"] = mlp_metrics
    print(f"   [OK] {mlp_metrics}")
    
    # =========================================================================
    # TEST 3: GNN with All Features
    # =========================================================================
    print("\n" + "-" * 40)
    print("TEST 3: GNN (Full Features including Delay)")
    print("-" * 40)
    
    gnn_full = SupplyChainGNN(in_channels=5, hidden_channels=64, out_channels=1).to(device)
    train_loader = DataLoader(train_snaps, batch_size=8, shuffle=True)
    gnn_full = train_model(gnn_full, train_loader, device, epochs=20)
    gnn_full_metrics = evaluate_model(gnn_full, test_snaps, device)
    results["GNN (full)"] = gnn_full_metrics
    print(f"   [OK] {gnn_full_metrics}")
    
    # =========================================================================
    # TEST 4: GNN without Delay Feature (Ablation)
    # =========================================================================
    print("\n" + "-" * 40)
    print("TEST 4: GNN Ablation (Delay Feature Removed)")
    print("-" * 40)
    
    train_snaps_no_delay = [remove_delay_feature(s) for s in train_snaps]
    test_snaps_no_delay = [remove_delay_feature(s) for s in test_snaps]
    
    gnn_ablated = SupplyChainGNN(in_channels=5, hidden_channels=64, out_channels=1).to(device)
    train_loader_abl = DataLoader(train_snaps_no_delay, batch_size=8, shuffle=True)
    gnn_ablated = train_model(gnn_ablated, train_loader_abl, device, epochs=20)
    gnn_ablated_metrics = evaluate_model(gnn_ablated, test_snaps_no_delay, device)
    results["GNN (no delay)"] = gnn_ablated_metrics
    print(f"   [OK] {gnn_ablated_metrics}")
    
    # =========================================================================
    # TEST 5: MLP Ablation (without Delay)
    # =========================================================================
    print("\n" + "-" * 40)
    print("TEST 5: MLP Ablation (Delay Feature Removed)")
    print("-" * 40)
    
    mlp_ablated = MLPBaseline(in_channels=5, hidden_channels=64, out_channels=1).to(device)
    mlp_ablated = train_model(mlp_ablated, train_loader_abl, device, epochs=20)
    mlp_ablated_metrics = evaluate_model(mlp_ablated, test_snaps_no_delay, device)
    results["MLP (no delay)"] = mlp_ablated_metrics
    print(f"   [OK] {mlp_ablated_metrics}")
    
    # =========================================================================
    # SUMMARY
    # =========================================================================
    print("\n" + "=" * 60)
    print("SUMMARY REPORT")
    print("=" * 60)
    print(f"\n{'Model':<20} {'MAE':>10} {'RMSE':>10} {'R²':>10}")
    print("-" * 50)
    for name, m in results.items():
        print(f"{name:<20} {m.mae:>10.4f} {m.rmse:>10.4f} {m.r2:>10.4f}")
    
    # Analysis
    print("\n" + "=" * 60)
    print("ANALYSIS")
    print("=" * 60)
    
    # Check 1: Does GNN beat MLP?
    gnn_vs_mlp = results["GNN (full)"].mae - results["MLP (no graph)"].mae
    if gnn_vs_mlp < -0.01:
        print("[+] GNN beats MLP -> Graph structure provides value")
    elif gnn_vs_mlp > 0.01:
        print("[!] MLP beats GNN -> Graph structure NOT helping")
    else:
        print("[=] GNN ~ MLP -> Graph structure provides minimal value")
    
    # Check 2: Label leakage detection
    delay_impact_gnn = results["GNN (no delay)"].mae - results["GNN (full)"].mae
    delay_impact_mlp = results["MLP (no delay)"].mae - results["MLP (no graph)"].mae
    
    print(f"\n[ABLATION] Delay Feature Impact:")
    print(f"   GNN: +{delay_impact_gnn:.4f} MAE when delay removed")
    print(f"   MLP: +{delay_impact_mlp:.4f} MAE when delay removed")
    
    if delay_impact_gnn > 0.1 or delay_impact_mlp > 0.1:
        print("\n[ALERT] HIGH LABEL LEAKAGE DETECTED!")
        print("   Model relies heavily on delay feature (which defines target)")
    else:
        print("\n[OK] Delay feature has moderate/low impact")
    
    # Check 3: Does ablated GNN beat ablated MLP?
    ablated_gnn_vs_mlp = results["GNN (no delay)"].mae - results["MLP (no delay)"].mae
    if ablated_gnn_vs_mlp < -0.01:
        print("\n[+] Without delay: GNN still beats MLP -> Graph truly helps")
    else:
        print("\n[!] Without delay: GNN ~ MLP -> Graph value unclear")
    
    print("\n" + "=" * 60)
    

if __name__ == "__main__":
    run_diagnostics()
