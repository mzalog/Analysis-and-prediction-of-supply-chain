
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.nn import SAGEConv, global_mean_pool

class SupplyChainGNN(torch.nn.Module):
    """
    Graph Neural Network for Supply Chain Risk Prediction.
    Uses GraphSAGE to aggregate neighborhood information (Traffic/Congestion propagation).
    """
    def __init__(self, in_channels=5, hidden_channels=64, out_channels=1):
        super(SupplyChainGNN, self).__init__()
        
        # Layer 1: Aggregates info from direct neighbors (1-hop)
        # Input features: [Type, Load, Traffic, Weather, CurrentDelay]
        self.conv1 = SAGEConv(in_channels, hidden_channels)
        
        # Layer 2: Aggregates info from neighbors of neighbors (2-hop)
        # This captures "Ripple Effects" in the supply chain
        self.conv2 = SAGEConv(hidden_channels, hidden_channels)
        
        # Layer 3: Output Head
        self.lin = nn.Linear(hidden_channels, out_channels)
        self.sigmoid = nn.Sigmoid()

    def forward(self, x, edge_index, batch=None):
        """
        x: Node features [Num_Nodes, Num_Features]
        edge_index: Graph connectivity [2, Num_Edges]
        batch: Batch vector (for mini-batching multiple snapshots)
        """
        
        # 1. Message Passing (Layer 1)
        x = self.conv1(x, edge_index)
        x = F.relu(x)
        x = F.dropout(x, p=0.2, training=self.training)
        
        # 2. Message Passing (Layer 2)
        x = self.conv2(x, edge_index)
        x = F.relu(x)
        
        # 3. Readout (Node-level prediction)
        # We want risk PER NODE, so we don't pool the whole graph.
        # If we wanted "Global Supply Chain Risk", we would use global_mean_pool(x, batch)
        
        x = self.lin(x)
        x = self.sigmoid(x)
        
        return x
