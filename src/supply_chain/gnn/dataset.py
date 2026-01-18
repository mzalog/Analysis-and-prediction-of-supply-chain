
import torch
from torch_geometric.data import Data, Dataset
import pandas as pd
import numpy as np
import networkx as nx
from pathlib import Path
from tqdm import tqdm
import json
from supply_chain.simulation.schema import NodeType

class SupplyChainGraphDataset:
    """
    Manages the conversion of Supply Chain simulation data into Graph Snapshots
    compatible with PyTorch Geometric.
    """
    def __init__(self, graph_builder, dataframe: pd.DataFrame, time_window_min: int = 60):
        self.graph = graph_builder.graph
        self.scaler_path = Path(__file__).resolve().parent.parent.parent.parent / "models" / "gnn_scaler.json"
        
        # Parse timestamp string to datetime
        dataframe['timestamp'] = pd.to_datetime(dataframe['timestamp'])
        self.df = dataframe.sort_values('timestamp')
        
        # Convert to minutes from start (numeric)
        start_time = self.df['timestamp'].iloc[0]
        self.df['timestamp_numeric'] = (self.df['timestamp'] - start_time).dt.total_seconds() / 60.0
        
        self.time_window = time_window_min
        self.node_mapping = {n: i for i, n in enumerate(self.graph.nodes())}
        self.reverse_mapping = {i: n for n, i in self.node_mapping.items()}
        self.num_nodes = len(self.graph.nodes)
        
        # Pre-compute edge index (Static Topology)
        self.edge_index = self._build_edge_index()
        self.snapshots = []

    def _build_edge_index(self):
        """Converts NetworkX edges to PyG edge_index format."""
        src, dst = [], []
        for u, v in self.graph.edges():
            if u in self.node_mapping and v in self.node_mapping:
                src.append(self.node_mapping[u])
                dst.append(self.node_mapping[v])
                # Undirected graph assumption for roads usually, 
                # but let's keep it directed if defined that way.
                # If undirected in simulation, add reverse edge
                src.append(self.node_mapping[v])
                dst.append(self.node_mapping[u])
        
        return torch.tensor([src, dst], dtype=torch.long)
        
    def _build_edge_attr(self, timestamp_numeric: float):
        """
        Builds edge attributes [Num_Edges, 3] -> [Distance, Traffic, Weather]
        Dynamic based on timestamp (for traffic/weather noise).
        """
        feats = []
        for u, v in self.graph.edges():
            src_node = self.graph.nodes[u]['data']
            dst_node = self.graph.nodes[v]['data']
            
            # Static Distance
            # Heuristic: 0.01 deg ~= 1km
            dist = ((src_node.lat - dst_node.lat)**2 + (src_node.lon - dst_node.lon)**2) ** 0.5
            dist_norm = dist / 10.0 # Normalize roughly 0-1 range for typical map
            
            # Dynamic Traffic/Weather (Mean of endpoints)
            # Use pseudo-random spatial noise based on time
            # We don't have the Integration calib here easily, so we mimic logic or use simple sine
            time_h = timestamp_numeric / 60.0
            
            # Simple noise function
            import math
            def noise(lat, lon, t):
                val = math.sin(lon/5.0 + t/24.0) + math.cos(lat/5.0 + t/48.0)
                return (val + 2.0) / 4.0
            
            w_src = noise(src_node.lat, src_node.lon, time_h)
            w_dst = noise(dst_node.lat, dst_node.lon, time_h)
            weather = (w_src + w_dst) / 2.0
            
            t_src = noise(src_node.lat+10, src_node.lon+10, time_h)
            t_dst = noise(dst_node.lat+10, dst_node.lon+10, time_h)
            traffic = (t_src + t_dst) / 2.0
            
            feats.append([dist_norm, traffic, weather])
            
            # Undirected duplicate
            feats.append([dist_norm, traffic, weather])
            
        return torch.tensor(feats, dtype=torch.float)

    def process(self):
        """
        Groups data by time windows and creates graph snapshots with FUTURE targets.
        X(t) -> Y(t+1)
        """
        # 1. Bucketize timestamps
        self.df['time_bucket'] = (self.df['timestamp_numeric'] // self.time_window).astype(int)
        grouped = self.df.groupby('time_bucket')
        
        print(f"Stats: Processing {len(grouped)} temporal buckets...")
        
        # 2. Create raw snapshots (X_t, Y_t_actual) for each bucket
        raw_snapshots = []
        timestamps = []
        
        # Sort by bucket ID to ensure temporal order
        sorted_groups = sorted(grouped, key=lambda x: x[0])
        
        for _, group in tqdm(sorted_groups, desc="Building Snapshots"):
            snapshot = self._create_snapshot(group)
            raw_snapshots.append(snapshot)
            # Use first timestamp in group as ref
            if not group.empty:
                timestamps.append(group['timestamp_numeric'].iloc[0])
            else:
                timestamps.append(0.0)
            
        # 3. Temporal Stacking (Window = 3)
        # We need at least Window+1 snapshots to make 1 sample (Window inputs -> 1 Target)
        WINDOW_SIZE = 3
        
        if len(raw_snapshots) <= WINDOW_SIZE:
             print("❌ Not enough snapshots for windowing.")
             return

        for i in range(WINDOW_SIZE, len(raw_snapshots) - 1):
             # Input: Stack features from [i-2, i-1, i]
             # Target: Y from i+1 (Next Step Risk)
             
             # Stack Node Features
             stack_x = []
             for w in range(WINDOW_SIZE):
                 # index = i - (WINDOW_SIZE - 1) + w  => [i-2, i-1, i]
                 idx = i - (WINDOW_SIZE - 1) + w
                 stack_x.append(raw_snapshots[idx].x)
                 
             # Concatenate along Feature Dimension (dim=1)
             # Shape: [Num_Nodes, 5] * 3 -> [Num_Nodes, 15]
             x_windowed = torch.cat(stack_x, dim=1)
             
             # Edges & Attributes (Dynamic based on current time T=i)
             curr_time = timestamps[i]
             edge_attr = self._build_edge_attr(curr_time)
             
             y_target = raw_snapshots[i+1].y
             
             data = Data(
                 x=x_windowed,
                 edge_index=self.edge_index,
                 edge_attr=edge_attr,
                 y=y_target
             )
             
             self.snapshots.append(data)
            
        if len(self.snapshots) > 0:
             self._compute_and_save_scaler()
             self._normalize_data()
             
        print(f"✅ Created {len(self.snapshots)} windowed sequences (Input: T-2..T -> Target: T+1).")
        
    def _compute_and_save_scaler(self):
        """Computes Mean and Std for X and EdgeAttr based on current snapshots."""
        print("   Computing Scaler Stats...")
        
        # Collect all x and edge_attr
        all_x = torch.cat([data.x for data in self.snapshots], dim=0) # [Total_Nodes, 15]
        all_edge = torch.cat([data.edge_attr for data in self.snapshots], dim=0) # [Total_Edges, 3]
        
        # Compute Stats
        x_mean = all_x.mean(dim=0).tolist()
        x_std = all_x.std(dim=0).tolist()
        
        e_mean = all_edge.mean(dim=0).tolist()
        e_std = all_edge.std(dim=0).tolist()
        
        # Avoid div by zero
        x_std = [s if s > 1e-5 else 1.0 for s in x_std]
        e_std = [s if s > 1e-5 else 1.0 for s in e_std]
        
        stats = {
            "x_mean": x_mean, "x_std": x_std,
            "edge_mean": e_mean, "edge_std": e_std
        }
        
        self.scaler_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.scaler_path, 'w') as f:
            json.dump(stats, f)
        print(f"   Scaler saved to {self.scaler_path}")
        
    def _normalize_data(self):
        """Applies normalization to all snapshots in memory."""
        # Reload stats to be sure (or just use what we computed)
        with open(self.scaler_path, 'r') as f:
            stats = json.load(f)
            
        x_mean = torch.tensor(stats["x_mean"])
        x_std = torch.tensor(stats["x_std"])
        e_mean = torch.tensor(stats["edge_mean"])
        e_std = torch.tensor(stats["edge_std"])
        
        for data in self.snapshots:
            data.x = (data.x - x_mean) / x_std
            data.edge_attr = (data.edge_attr - e_mean) / e_std

    def _create_snapshot(self, group_df: pd.DataFrame) -> Data:
        # Initialize Node Features Matrix [Num_Nodes, Num_Features]
        # Features: 
        # 0: Node Type (Ordinal)
        # 1: Order Load (Count of events in this bucket)
        # 2: Traffic
        # 3: Weather
        # 4: Avg Delay (Current bucket) - Safe Feature for predicting Next Bucket Risk
        
        x = torch.zeros((self.num_nodes, 5), dtype=torch.float)
        y = torch.zeros((self.num_nodes, 1), dtype=torch.float) # Calculated Risk
        
        # Static Features (Type)
        for node_id, idx in self.node_mapping.items():
            node = self.graph.nodes[node_id]['data']
            # Feature 0: Type
            type_val = 0
            if node.type == NodeType.WAREHOUSE: type_val = 1
            elif node.type == NodeType.HUB: type_val = 2
            elif node.type == NodeType.PORT: type_val = 3
            elif node.type == NodeType.CUSTOMER: type_val = 4
            x[idx, 0] = type_val

        # Dynamic Features (Aggregated from DataFrame group)
        # Group by Node ID
        node_groups = group_df.groupby('node_id')
        
        for node_id, records in node_groups:
            if node_id not in self.node_mapping: continue
            
            idx = self.node_mapping[node_id]
            
            # Feature 1: Event Count (Load)
            count = len(records)
            x[idx, 1] = float(count)
            
            # Feature 2: High Traffic Flag (Avg of traffic level)
            avg_traffic = records['traffic_congestion_level'].mean()
            x[idx, 2] = float(avg_traffic) if not pd.isna(avg_traffic) else 0.0

            # Feature 3: Bad Weather Flag
            avg_weather = records['weather_condition_severity'].mean()
            x[idx, 3] = float(avg_weather) if not pd.isna(avg_weather) else 0.0
            
            # Feature 4: Avg Delay (Current Performance)
            avg_delay = records['delivery_time_deviation'].mean()
            x[idx, 4] = float(avg_delay) if not pd.isna(avg_delay) else 0.0

            # Target Calculation (for this bucket)
            # This y will be the TARGET for the PREVIOUS bucket in process()
            # If delay > 60 mins (1h), risk = 1.0, else mapped sigmoidally/linearly
            risk = 1.0 if avg_delay > 60 else (avg_delay / 60.0 if avg_delay > 0 else 0)
            y[idx, 0] = float(risk)

        # Construct Data Object
        data = Data(x=x, edge_index=self.edge_index, y=y)
        return data

    def get_loader(self, batch_size=32):
        from torch_geometric.loader import DataLoader
        return DataLoader(self.snapshots, batch_size=batch_size, shuffle=True)
