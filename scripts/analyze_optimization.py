
import sys
import pandas as pd
import torch
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt

project_root = Path("d:/dev/Analysis-and-prediction-of-supply-chain")
sys.path.append(str(project_root))
sys.path.append(str(project_root / "src"))

from supply_chain.config import DatasetSchema, REPORTS_DIR
from supply_chain.data.preprocessing import PreprocessingConfig, TabularPreprocessor
from supply_chain.model.network import SupplyChainNet

def analyze_optimization():
    print("Loading Massive Dataset...")
    data_path = Path(REPORTS_DIR) / "experiments" / "experiment_massive" / "simulated_data.csv"
    model_path = Path(REPORTS_DIR) / "experiments" / "experiment_massive" / "model.pth"
    
    if not data_path.exists():
        print(f"Error: Data file not found at {data_path}")
        print("Please run 'python run_massive.py' first.")
        return

    df = pd.read_csv(data_path)
    print(f"Loaded {len(df)} rows.")

    # Preprocess
    schema = DatasetSchema()
    pp_config = PreprocessingConfig(schema)
    preprocessor = TabularPreprocessor(pp_config)
    
    # We need to replicate the exact steps used in training
    # Only use columns that exist in the dataframe
    preprocessor.fit(df)
    X = preprocessor.transform(df)
    y = df[schema.target_column].values
    
    feature_names = preprocessor.feature_names_out
    input_size = len(feature_names)
    
    # Load Model
    model = SupplyChainNet(input_size)
    if model_path.exists():
        model.load_state_dict(torch.load(model_path))
        print("Loaded trained model.")
    else:
        print("Warning: Model checkpoint not found, using untrained model (results will be random).")
    
    model.eval()
    X_tensor = torch.tensor(X, dtype=torch.float32)
    y_tensor = torch.tensor(y, dtype=torch.float32).unsqueeze(1)
    
    # --- Permutation Importance ---
    print("\nCalculating Feature Importance (Permutation Method)...")
    
    # 1. Baseline Loss
    criterion = torch.nn.BCELoss()
    with torch.no_grad():
        baseline_preds = model(X_tensor)
        baseline_loss = criterion(baseline_preds, y_tensor).item()
    
    importances = []
    for i, col_name in enumerate(feature_names):
        # Permute column i
        X_permuted = X_tensor.clone()
        idx = torch.randperm(X_permuted.shape[0])
        X_permuted[:, i] = X_permuted[idx, i]
        
        with torch.no_grad():
            permuted_preds = model(X_permuted)
            permuted_loss = criterion(permuted_preds, y_tensor).item()
            
        # Importance = Increase in Loss
        importance = permuted_loss - baseline_loss
        importances.append((col_name, importance))
        
    # Sort
    importances.sort(key=lambda x: x[1], reverse=True)
    
    # --- Report Insights ---
    print("\n" + "="*50)
    print("OPTIMIZATION INSIGHTS REPORT")
    print("="*50)
    print("Based on AI analysis of 14 days of operation:")
    
    print("\nTOP 5 FACTORS CAUSING DELAYS/FAILURES:")
    for i, (name, score) in enumerate(importances[:5]):
        print(f"{i+1}. {name: <30} (Impact Score: {score:.4f})")
        
    # Generate Recommendations
    print("\n" + "-"*50)
    print("RECOMMENDED ACTIONS:")
    print("-"*50)
    
    top_factor = importances[0][0]
    
    if "route_risk_level" in top_factor:
        print("🔴 CRITICAL: High Risk Routes are the primary cause of failure.")
        print("   -> OPTIMIZATION: Update Route Planning algorithm to penalize high-risk edges.")
        print("   -> OPTIMIZATION: Implement 'Safe Corridor' routing for high-value cargo.")
        
    elif "delay_probability" in top_factor:
        print("🔴 CRITICAL: General delay probability is high.")
        print("   -> OPTIMIZATION: This suggests systemic issues. improvements in Driver Training or Vehicle maintenance.")
        
    elif "traffic" in top_factor:
        print("🔴 CRITICAL: Traffic is the biggest bottleneck.")
        print("   -> OPTIMIZATION: Implement 'Night Shifts' to avoid rush hours.")
        print("   -> OPTIMIZATION: Use smarter routing (graph weights) to bypass congested edges.")
        
    elif "weather" in top_factor:
        print("🔴 CRITICAL: Weather is destroying the schedule.")
        print("   -> OPTIMIZATION: Increase safety buffers in 'Expected Time of Arrival' (ETA).")
        print("   -> OPTIMIZATION: Switch to more robust transport modes for critical orders.")
        
    elif "warehouse" in top_factor or "loading" in top_factor:
        print("🔴 CRITICAL: Warehouse Efficiency is low.")
        print("   -> OPTIMIZATION: Invest in 'Handling Equipment' (forklifts/robots).")
        print("   -> OPTIMIZATION: Pre-stage cargo before truck arrival.")
        
    elif "fuel" in top_factor or "shipping" in top_factor:
        print("🔴 CRITICAL: Cost/Fuel Efficiency factors are correlated with risk.")
        print("   -> OPTIMIZATION: Check fleet maintenance state.")
        
    else:
        print(f"🔴 CRITICAL: {top_factor} is the main driver.")
        print("   -> ACTION: Investigate data distribution for this feature.")
        
    print("="*50)

if __name__ == "__main__":
    analyze_optimization()
