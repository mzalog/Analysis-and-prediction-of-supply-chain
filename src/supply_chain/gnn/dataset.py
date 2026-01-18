
import torch
from torch_geometric.data import Data, Dataset
import pandas as pd
import numpy as np
import networkx as nx
from pathlib import Path
from tqdm import tqdm
from supply_chain.simulation.schema import NodeType

class SupplyChainGraphDataset:
    """
    Manages the conversion of Supply Chain simulation data into Graph Snapshots
    compatible with PyTorch Geometric.
    """
    def __init__(self, graph_builder, dataframe: pd.DataFrame, time_window_min: int = 60):
        self.graph = graph_builder.graph
        
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

    def process(self):
        """
        Groups data by time windows and creates graph snapshots.
        Each snapshot represents the state of the network at a specific time block.
        """
        # 1. Bucketize timestamps
        self.df['time_bucket'] = (self.df['timestamp_numeric'] // self.time_window).astype(int)
        grouped = self.df.groupby('time_bucket')
        
        print(f"Stats: Processing {len(grouped)} temporal snapshots...")
        
        for _, group in tqdm(grouped):
            snapshot = self._create_snapshot(group)
            self.snapshots.append(snapshot)
            
        print(f"✅ Created {len(self.snapshots)} graph snapshots.")

    def _create_snapshot(self, group_df: pd.DataFrame) -> Data:
        # Initialize Node Features Matrix [Num_Nodes, Num_Features]
        # Features: 
        # 0: Node Type (Ordinal)
        # 1: Order Load (Count of events in this bucket)
        # 2: Avg Delay (in this bucket) - TARGET for some tasks, Feature for others?
        # Let's say we want to predict "Risk" for the NEXT bucket. 
        # But for now, let's build features representing the CURRENT state.
        
        x = torch.zeros((self.num_nodes, 5), dtype=torch.float)
        y = torch.zeros((self.num_nodes, 1), dtype=torch.float) # Risk Score
        
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
            x[idx, 1] = count
            
            # Feature 2: High Traffic Flag (Avg of traffic level)
            avg_traffic = records['traffic_congestion_level'].mean()
            x[idx, 2] = avg_traffic if not pd.isna(avg_traffic) else 0.0

            # Feature 3: Bad Weather Flag
            avg_weather = records['weather_condition_severity'].mean()
            x[idx, 3] = avg_weather if not pd.isna(avg_weather) else 0.0
            
            # Feature 4: Avg Delay (Current Performance)
            avg_delay = records['delivery_time_deviation'].mean()
            x[idx, 4] = avg_delay if not pd.isna(avg_delay) else 0.0

            # Target: Inefficiency Score (Simple heuristic for now)
            # If delay > 30 mins, risk = 1.0, else mapped sigmoidally
            risk = 1.0 if avg_delay > 60 else (avg_delay / 60.0 if avg_delay > 0 else 0)
            y[idx, 0] = risk

        # Construct Data Object
        data = Data(x=x, edge_index=self.edge_index, y=y)
        return data

    def get_loader(self, batch_size=32):
        from torch_geometric.loader import DataLoader
        return DataLoader(self.snapshots, batch_size=batch_size, shuffle=True)
