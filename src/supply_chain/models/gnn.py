
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.nn import GATv2Conv

class SupplyChainGNN(torch.nn.Module):
    """
    ST-GNN (Spatio-Temporal Graph Neural Network)
    - Input: Stacked features [T-2, T-1, T] -> 15 channels
    - Architecture: GATv2 (Attention) with Edge Attributes
    - Task: Binary Risk Classification (0-1 Probability)
    """
    def __init__(self, in_channels=15, hidden_channels=64, out_channels=1):
        super(SupplyChainGNN, self).__init__()
        
        # 0. Embeddings
        # Node Type is Feature 0 (Indices: 0, 5, 10 in stacked vector). 
        # But for simplicity, we treat first channel of 'Current' T as the Type.
        self.type_embedding = nn.Embedding(num_embeddings=5, embedding_dim=8)
        
        # Calculate Input dimension after embedding concat
        # We have 15 raw features. 
        # We will replace the "Type" columns (indices 0, 5, 10) with shared embedding?
        # Or just embed the "Current" type and concat with everything else.
        # Let's keep it simple: Embed "Current Type" (Index 10 for T), concat with full 15 inputs.
        # Input = 15 + 8 = 23.
        
        self.input_proj = nn.Linear(in_channels + 8, hidden_channels)
        
        # Layer 1: GATv2 with Edge Attributes
        # Edge Attr Dim = 3 (Dist, Traffic, Weather)
        self.conv1 = GATv2Conv(hidden_channels, hidden_channels, heads=4, concat=False, edge_dim=3)
        
        # Layer 2: GATv2
        self.conv2 = GATv2Conv(hidden_channels, hidden_channels, heads=4, concat=False, edge_dim=3)
        
        # Layer 3: Output Head
        self.lin = nn.Linear(hidden_channels, out_channels)
        # No sigmoid here if using BCEWithLogitsLoss, BUT user asked for probabilities in inference 
        # usually. Train loop uses logits? Let's return Logits and handle sigmoid outside or in Loss.
        # Wait, app.py expects [0,1]. Let's stick to Sigmoid for inference convenience, 
        # but usage with BCEWithLogitsLoss requires removing it OR using BCELoss.
        # Let's use BCELoss (requires Sigmoid output).
        
    def forward(self, x, edge_index, edge_attr=None, batch=None):
        """
        x: [Num_Nodes, 15] (Stacked T-2..T)
        """
        
        # 1. Embedding Highlighting
        # Dynamic Type Index Calculation
        # x shape: [N, channels]. Steps = 3. Features per step = channels // 3.
        # "Current" Type feature is at the start of the 3rd chunk (T).
        step_dim = x.size(1) // 3
        type_idx = 2 * step_dim + 0 # Start of 3rd chunk
        
        current_type = x[:, type_idx].long()
        # Safety: Clamp to valid range [0, 4] to prevent CUDA Assert failures
        current_type = torch.clamp(current_type, 0, 4)
        current_type = torch.clamp(current_type, min=0, max=4)
        
        type_emb = self.type_embedding(current_type) # [N, 8]
        
        # Concat everything
        x = torch.cat([x, type_emb], dim=1) # [N, 23]
        
        x = self.input_proj(x)
        x = F.relu(x)
        
        # 2. Graph Attention (Layer 1)
        x = self.conv1(x, edge_index, edge_attr=edge_attr)
        x = F.relu(x)
        x = F.dropout(x, p=0.2, training=self.training)
        
        # 3. Graph Attention (Layer 2)
        x = self.conv2(x, edge_index, edge_attr=edge_attr)
        x = F.relu(x)
        
        # 4. Readout
        x = self.lin(x)
        return x # Return Logits (for BCEWithLogitsLoss)
