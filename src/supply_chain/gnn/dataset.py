
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
    def __init__(self, graph_builder, data_dir: Path = None, dataframe: pd.DataFrame = None, time_window_min: int = 60):
        self.graph = graph_builder.graph
        
        # Mode A: Load from Directory of Episodes (Preferred)
        self.episode_files = []
        if data_dir and data_dir.exists():
             self.episode_files = sorted(list(data_dir.glob("episode_*.csv")))
             
        # Mode B: Single DataFrame (Legacy/App inference)
        self.dataframe = dataframe
        
        self.time_window = time_window_min
        
        # CRITICAL: Sort nodes to ensure index stability across time steps and inference
        self.sorted_nodes = sorted(list(self.graph.nodes()))
        self.node_mapping = {n: i for i, n in enumerate(self.sorted_nodes)}
        self.reverse_mapping = {i: n for n, i in self.node_mapping.items()}
        self.num_nodes = len(self.graph.nodes)
        
        # Pre-compute edge index (Static Topology)
        self.edge_index = self._build_edge_index()
        
        # Storage: List of Lists (Episodes -> Snapshots)
        self.episodes = [] 
        # Flat list (optional, constructed dynamically or used for legacy)
        self._flat_snapshots = []

    def _build_edge_index(self):
        """Converts NetworkX edges to PyG edge_index format."""
        src, dst = [], []
        # Ensure iteration order is consistent if edge attributes depend on it?
        # Better to iterate sorted edges
        for u, v in sorted(self.graph.edges()):
            if u in self.node_mapping and v in self.node_mapping:
                src.append(self.node_mapping[u])
                dst.append(self.node_mapping[v])
                # Undirected assumption (add reverse info)
                src.append(self.node_mapping[v])
                dst.append(self.node_mapping[u])
        
        return torch.tensor([src, dst], dtype=torch.long)
        
    def _build_edge_attr(self, timestamp_numeric: float):
        """
        Builds edge attributes [Num_Edges, 3] -> [Distance, Traffic, Weather]
        Dynamic based on timestamp (for traffic/weather noise).
        """
        feats = []
        # MUST match _build_edge_index iteration order
        for u, v in sorted(self.graph.edges()):
            src_node = self.graph.nodes[u]['data']
            dst_node = self.graph.nodes[v]['data']
            
            # Static Distance
            # Heuristic: 0.01 deg ~= 1km
            dist = ((src_node.lat - dst_node.lat)**2 + (src_node.lon - dst_node.lon)**2) ** 0.5
            dist_norm = dist / 10.0 # Normalize roughly 0-1 range for typical map
            
            # Dynamic Traffic/Weather (Mean of endpoints)
            time_h = timestamp_numeric / 60.0
            
            # Simple noise function independent of integration dependencies
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
        Process available data into graph snapshots.
        Handles both Multi-Episode (Files) and Single-Stream (DataFrame).
        """
        if self.episode_files:
            print(f"Stats: Found {len(self.episode_files)} episodes in {self.episode_files[0].parent}")
            for fpath in tqdm(self.episode_files, desc="Processing Episodes"):
                df = pd.read_csv(fpath)
                episode_snaps = self._process_dataframe(df)
                if episode_snaps:
                    self.episodes.append(episode_snaps)
            
            print(f"✅ Processed {len(self.episodes)} episodes.")
            
        elif self.dataframe is not None:
            # Legacy/App mode
            snaps = self._process_dataframe(self.dataframe)
            self.episodes.append(snaps)
            
    @property
    def snapshots(self):
        """Flattened list of all snapshots (for backward compatibility)."""
        if not self._flat_snapshots and self.episodes:
            self._flat_snapshots = [s for ep in self.episodes for s in ep]
        return self._flat_snapshots

    def _process_dataframe(self, df: pd.DataFrame) -> list:
        """Helper to process a single DataFrame (one episode) into sequence."""
        # Reset history for this new episode
        self.last_delays = {}
        
        # Preprocess Time
        if 'timestamp' in df.columns and df['timestamp'].dtype == object:
             df['timestamp'] = pd.to_datetime(df['timestamp'])
             
        # Normalize time to minutes from start of THIS episode
        start_time = df['timestamp'].iloc[0]
        df['timestamp_numeric'] = (df['timestamp'] - start_time).dt.total_seconds() / 60.0
        
        # Ensure numeric types for aggregation (robust to mixed dtypes)
        numeric_cols = [
            "traffic_congestion_level",
            "weather_condition_severity",
            "delivery_time_deviation",
            "pending_orders_count",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        # 1. Bucketize
        df['time_bucket'] = (df['timestamp_numeric'] // self.time_window).astype(int)
        grouped = df.groupby('time_bucket')
        
        # 2. Raw Snapshots
        raw_snapshots = []
        timestamps = []
        sorted_groups = sorted(grouped, key=lambda x: x[0])
        
        for _, group in sorted_groups:
            snapshot = self._create_snapshot(group)
            raw_snapshots.append(snapshot)
            if not group.empty:
                timestamps.append(group['timestamp_numeric'].iloc[0])
            else:
                timestamps.append(0.0)
                
        # 3. Windowing & Padding
        WINDOW_SIZE = 3
        
        # Cold-Start Padding
        if raw_snapshots:
            padding = [raw_snapshots[0]] * (WINDOW_SIZE - 1)
            raw_snapshots = padding + raw_snapshots
            # Adjust timestamps? Not strictly needed for logic, but good for keeping sync
            timestamps = [timestamps[0]]*(WINDOW_SIZE-1) + timestamps
            
        processed_sequence = []
        
        if len(raw_snapshots) <= WINDOW_SIZE:
             return []

        FORECAST_HORIZON = 3
        
        # Loop until we have enough future data for lookahead
        for i in range(WINDOW_SIZE, len(raw_snapshots) - FORECAST_HORIZON):
             # Window: [i-2, i-1, i] -> Features
             stack_x = []
             for w in range(WINDOW_SIZE):
                 idx = i - (WINDOW_SIZE - 1) + w
                 stack_x.append(raw_snapshots[idx].x)
             
             x_windowed = torch.cat(stack_x, dim=1)
             
             # Edges
             curr_time = timestamps[i]
             edge_attr = self._build_edge_attr(curr_time)
             
             # Target: Early-warning event based on future delay/backlog
             future_delays = torch.stack(
                 [raw_snapshots[j].x[:, 4] for j in range(i + 1, i + 1 + FORECAST_HORIZON)],
                 dim=1,
             )
             future_backlogs = torch.stack(
                 [raw_snapshots[j].x[:, 5] for j in range(i + 1, i + 1 + FORECAST_HORIZON)],
                 dim=1,
             )

             max_delay = future_delays.max(dim=1).values
             max_backlog = future_backlogs.max(dim=1).values

             # Binary target: will a crisis happen within horizon?
             y_target = ((max_delay > 120.0) | (max_backlog > 80.0)).float().unsqueeze(1)
             
             data = Data(x=x_windowed, edge_index=self.edge_index, edge_attr=edge_attr, y=y_target)
             processed_sequence.append(data)
             
        return processed_sequence
        # NOTE: Normalization is now handled in train.py to allow proper Train-Split-Only fitting

    def _create_snapshot(self, group_df: pd.DataFrame) -> Data:
        # Initialize Node Features Matrix [Num_Nodes, Num_Features]
        # Features: 
        # 0: Node Type (Ordinal)
        # 1: Order Load (Count of events in this bucket)
        # 2: Traffic
        # 3: Weather
        # 4: Avg Delay (Current bucket)
        # 5: Backlog (Pending Orders Count) - EARLY WARNING SIGNAL
        
        x = torch.zeros((self.num_nodes, 6), dtype=torch.float)
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
        node_stats = {} # node_id -> (count, traffic, weather, delay, backlog)
        
        groups = group_df.groupby('node_id')
        
        # Helper to get stats or default
        def get_stats(nid):
            if nid in groups.groups:
                g = groups.get_group(nid)
                # Check if backlog column exists (backward compat)
                backlog = 0.0
                if 'pending_orders_count' in g.columns:
                     backlog = g['pending_orders_count'].max() # Use max backlog seen in bucket? Or mean? Max is safer for risk.
                
                return (
                    len(g),
                    g['traffic_congestion_level'].mean(),
                    g['weather_condition_severity'].mean(),
                    g['delivery_time_deviation'].mean(),
                    backlog
                )
            return (0, 0.0, 0.0, 0.0, 0.0)

        for node_id, idx in self.node_mapping.items():
            count, traf, weath, delay, backlog = get_stats(node_id)
            
            # Use defaults if NaN
            if pd.isna(traf): traf = 0.0
            if pd.isna(weath): weath = 0.0
            if pd.isna(delay): delay = 0.0
            if pd.isna(backlog): backlog = 0.0
            
            node_stats[node_id] = (count, traf, weath, delay, backlog)
            self.last_delays[node_id] = delay
        
        # Pass 3: Fill Tensor
        for node_id, idx in self.node_mapping.items():
            count, traf, weath, delay, backlog = node_stats[node_id]
            
            x[idx, 1] = float(count)
            x[idx, 2] = float(traf)
            x[idx, 3] = float(weath)
            x[idx, 4] = float(delay)
            x[idx, 5] = float(backlog)
            
            y[idx, 0] = 0.0
            
        # Skip original loop logic
        if False:
             pass

        # Construct Data Object
        data = Data(x=x, edge_index=self.edge_index, y=y)
        return data

    def get_loader(self, batch_size=32):
        from torch_geometric.loader import DataLoader
        return DataLoader(self.snapshots, batch_size=batch_size, shuffle=True)
