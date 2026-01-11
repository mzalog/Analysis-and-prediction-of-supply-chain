import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader
from .network import SupplyChainNet
from .evaluate import evaluate_model
from pathlib import Path

def train_model(train_loader: DataLoader, val_loader: DataLoader, input_size: int, epochs: int = 10, lr: float = 0.001, save_path: Path = None):
    model = SupplyChainNet(input_size)
    criterion = nn.BCELoss()
    optimizer = optim.Adam(model.parameters(), lr=lr)
    
    best_val_loss = float('inf')
    
    for epoch in range(epochs):
        model.train()
        running_loss = 0.0
        for inputs, targets in train_loader:
            optimizer.zero_grad()
            outputs = model(inputs)
            loss = criterion(outputs, targets)
            loss.backward()
            optimizer.step()
            running_loss += loss.item()
            
        # Validation
        val_loss = 0.0
        model.eval()
        with torch.no_grad():
            for inputs, targets in val_loader:
                outputs = model(inputs)
                loss = criterion(outputs, targets)
                val_loss += loss.item()
        
        avg_train_loss = running_loss / len(train_loader)
        avg_val_loss = val_loss / len(val_loader)
        
        print(f"Epoch {epoch+1}/{epochs}, Train Loss: {avg_train_loss:.4f}, Val Loss: {avg_val_loss:.4f}")
        
        if avg_val_loss < best_val_loss:
            best_val_loss = avg_val_loss
            if save_path:
                torch.save(model.state_dict(), save_path)
                
    metrics = evaluate_model(model, val_loader)
    return model, metrics
