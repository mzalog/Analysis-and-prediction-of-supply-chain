
import torch
from torch_geometric.data import Data
import pandas as pd
import numpy as np
from pathlib import Path
from tqdm import tqdm
from supply_chain.simulation.schema import NodeType

class SupplyChainGraphDatasetV2:
    """
    V2 Dataset for Delay Regression.
    - Time Window: 60 minutes
    - Target: Future Log-Delay (Regression)
    """
    def __init__(self, graph_builder, data_dir: Path = None, dataframe: pd.DataFrame = None, time_window_min: int = 60):
        self.graph = graph_builder.graph
        
        # Mode A: Load from Directory of Episodes
        if data_dir and data_dir.exists():
             self.episode_files = sorted(list(data_dir.glob("scenario_*.csv")))
             
        # Mode B: Single DataFrame
        self.dataframe = dataframe
        
        self.time_window = time_window_min
        
        # Topology
        self.sorted_nodes = sorted(list(self.graph.nodes()))
        self.node_mapping = {n: i for i, n in enumerate(self.sorted_nodes)}
        self.num_nodes = len(self.graph.nodes)
        
        # Pre-compute edge index
        self.edge_index = self._build_edge_index()
        
        self.episodes = [] 
        self._flat_snapshots = []

    def _build_edge_index(self):
        src, dst = [], []
        for u, v in sorted(self.graph.edges()):
            if u in self.node_mapping and v in self.node_mapping:
                src.append(self.node_mapping[u])
                dst.append(self.node_mapping[v])
                # Undirected
                src.append(self.node_mapping[v])
                dst.append(self.node_mapping[u])
        return torch.tensor([src, dst], dtype=torch.long)
        
    def _build_edge_attr(self, timestamp_numeric: float):
        feats = []
        import math
        time_h = timestamp_numeric / 60.0
        
        def noise(lat, lon, t):
            val = math.sin(lon/5.0 + t/24.0) + math.cos(lat/5.0 + t/48.0)
            return (val + 2.0) / 4.0
            
        for u, v in sorted(self.graph.edges()):
            src_node = self.graph.nodes[u]['data']
            dst_node = self.graph.nodes[v]['data']
            
            dist = ((src_node.lat - dst_node.lat)**2 + (src_node.lon - dst_node.lon)**2) ** 0.5
            dist_norm = dist / 10.0
            
            w_src = noise(src_node.lat, src_node.lon, time_h)
            w_dst = noise(dst_node.lat, dst_node.lon, time_h)
            weather = (w_src + w_dst) / 2.0
            
            t_src = noise(src_node.lat+10, src_node.lon+10, time_h)
            t_dst = noise(dst_node.lat+10, dst_node.lon+10, time_h)
            traffic = (t_src + t_dst) / 2.0
            
            feats.append([dist_norm, traffic, weather])
            feats.append([dist_norm, traffic, weather])
            
        return torch.tensor(feats, dtype=torch.float)

    def process(self):
        if self.episode_files:
            print(f"Stats: Found {len(self.episode_files)} episodes.")
            for fpath in tqdm(self.episode_files, desc="Processing V2"):
                df = pd.read_csv(fpath)
                episode_snaps = self._process_dataframe(df)
                if episode_snaps:
                    self.episodes.append(episode_snaps)
            print(f"✅ Processed {len(self.episodes)} episodes.")
        elif self.dataframe is not None:
            snaps = self._process_dataframe(self.dataframe)
            self.episodes.append(snaps)
            
    @property
    def snapshots(self):
        if not self._flat_snapshots and self.episodes:
            self._flat_snapshots = [s for ep in self.episodes for s in ep]
        return self._flat_snapshots

    def _process_dataframe(self, df: pd.DataFrame) -> list:
        if 'timestamp' in df.columns and df['timestamp'].dtype == object:
             df['timestamp'] = pd.to_datetime(df['timestamp'])
             
        start_time = df['timestamp'].iloc[0]
        df['timestamp_numeric'] = (df['timestamp'] - start_time).dt.total_seconds() / 60.0
        
        numeric_cols = ["traffic_congestion_level", "weather_condition_severity", "delivery_time_deviation", "pending_orders_count"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        # 1. Bucketize (60 min default)
        df['time_bucket'] = (df['timestamp_numeric'] // self.time_window).astype(int)
        
        # 2. Vectorized Aggregation
        # Group by [time_bucket, node_id] once
        agg_funcs = {
            'traffic_congestion_level': 'mean',
            'weather_condition_severity': 'mean',
            'delivery_time_deviation': 'mean',
            'pending_orders_count': 'max',
            'node_id': 'size' # Count events
        }
        # Provide backup if columns missing
        for col in list(agg_funcs.keys()):
            if col not in df.columns and col != 'node_id':
                df[col] = 0.0

        # Aggregation
        grouped = df.groupby(['time_bucket', 'node_id']).agg({
             'traffic_congestion_level': 'mean',
             'weather_condition_severity': 'mean',
             'delivery_time_deviation': 'mean',
             'pending_orders_count': 'max',
        })
        # Add counts
        counts = df.groupby(['time_bucket', 'node_id']).size().rename('event_count')
        grouped = grouped.join(counts)

        # 3. Reindex to dense grid [All Buckets x All Nodes]
        all_buckets = sorted(df['time_bucket'].unique())
        if not all_buckets: return []
        
        # Create MultiIndex for complete cartesian product
        full_idx = pd.MultiIndex.from_product(
            [all_buckets, self.sorted_nodes], 
            names=['time_bucket', 'node_id']
        )
        
        # Reindex and fill missing with 0
        grouped_dense = grouped.reindex(full_idx, fill_value=0.0)
        
        # 4. Create Snapshots
        raw_snapshots = []
        timestamps = [b * self.time_window for b in all_buckets]
        
        # Precompute static type feature
        # x_static: [Num_Nodes, 1]
        x_static = torch.zeros((self.num_nodes, 1), dtype=torch.float)
        for i, node_id in enumerate(self.sorted_nodes):
            node = self.graph.nodes[node_id]['data']
            type_val = 0
            if node.type == NodeType.WAREHOUSE: type_val = 1
            elif node.type == NodeType.HUB: type_val = 2
            elif node.type == NodeType.PORT: type_val = 3
            elif node.type == NodeType.CUSTOMER: type_val = 4
            x_static[i, 0] = type_val
            
        # Iterate over buckets (much faster now, no inner groupby)
        for bucket in all_buckets:
            # Slice for this bucket: [Num_Nodes, Features]
            chunk = grouped_dense.loc[bucket]
            
            # Ensure order matches self.sorted_nodes (reindex handles this, but strictly checks)
            # chunk rows correspond to self.sorted_nodes in order
            
            # Features: [Type, Count, Traf, Weath, Delay, Backlog]
            # chunk columns: congestion, weather, deviation, pending, event_count
            
            # Construct X [Num_Nodes, 6]
            # Use torch.from_numpy for speed
            vals = torch.from_numpy(chunk.values).float() 
            # chunk.values columns order depends on agg/join order. 
            # Let's be explicit:
            
            x_dynamic = torch.zeros((self.num_nodes, 5), dtype=torch.float)
            x_dynamic[:, 0] = torch.from_numpy(chunk['event_count'].values).float()
            x_dynamic[:, 1] = torch.from_numpy(chunk['traffic_congestion_level'].values).float()
            x_dynamic[:, 2] = torch.from_numpy(chunk['weather_condition_severity'].values).float()
            x_dynamic[:, 3] = torch.from_numpy(chunk['delivery_time_deviation'].values).float()
            x_dynamic[:, 4] = torch.from_numpy(chunk['pending_orders_count'].values).float()
            
            x = torch.cat([x_static, x_dynamic], dim=1)
            
            data = Data(x=x, edge_index=self.edge_index)
            raw_snapshots.append(data)

        # 5. Windowing (Same as before)
        WINDOW_SIZE = 3
        if raw_snapshots:
            padding = [raw_snapshots[0]] * (WINDOW_SIZE - 1)
            raw_snapshots = padding + raw_snapshots
            timestamps = [timestamps[0]]*(WINDOW_SIZE-1) + timestamps
            
        processed_sequence = []
        if len(raw_snapshots) <= WINDOW_SIZE: return []

        FORECAST_HORIZON = 3
        
        for i in range(WINDOW_SIZE, len(raw_snapshots) - FORECAST_HORIZON):
             stack_x = []
             for w in range(WINDOW_SIZE):
                 idx = i - (WINDOW_SIZE - 1) + w
                 stack_x.append(raw_snapshots[idx].x)
             
             x_windowed = torch.cat(stack_x, dim=1)
             curr_time = timestamps[i]
             edge_attr = self._build_edge_attr(curr_time)
             
             # TARGET V2: MAX FUTURE DELAY
             future_delays = []
             for j in range(i+1, i+1+FORECAST_HORIZON):
                 future_delays.append(raw_snapshots[j].x[:, 4]) # Delay is at index 4
                 
             future_delays_stack = torch.stack(future_delays, dim=1)
             max_future_delay = future_delays_stack.max(dim=1).values
             
             # log1p target
             y_target = torch.log1p(max_future_delay.clamp(min=0.0)).unsqueeze(1)
             
             data = Data(x=x_windowed, edge_index=self.edge_index, edge_attr=edge_attr, y=y_target)
             processed_sequence.append(data)
             
        return processed_sequence

    def get_loader(self, batch_size=32):
        from torch_geometric.loader import DataLoader
        return DataLoader(self.snapshots, batch_size=batch_size, shuffle=True)
