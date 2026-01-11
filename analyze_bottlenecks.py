
import pandas as pd
import numpy as np
import torch
import sys
from pathlib import Path

project_root = Path("d:/dev/Analysis-and-prediction-of-supply-chain")
sys.path.append(str(project_root))
sys.path.append(str(project_root / "src"))

# Imports for AI
from supply_chain.config import DatasetSchema, REPORTS_DIR
from supply_chain.data.preprocessing import PreprocessingConfig, TabularPreprocessor
from supply_chain.model.network import SupplyChainNet

DATA_PATH = project_root / "reports/experiments/long_term_5y/simulated_data_5y.csv"

# Allow CLI override
if len(sys.argv) > 1:
    user_path = Path(sys.argv[1])
    if user_path.exists():
        DATA_PATH = user_path
        print(f"📂 Analyzing Custom Dataset: {DATA_PATH.name}")
    else:
        print(f"⚠️ Custom path not found: {sys.argv[1]}")

MODEL_PATH = project_root / "reports/experiments/experiment_massive/model.pth"

def analyze_bottlenecks():
    print("--------------------------------------------------")
    print("🤖 AI-POWERED BOTTLENECK DETECTION")
    print("--------------------------------------------------")
    
    if not DATA_PATH.exists():
        print(f"❌ Error: Data file not found at {DATA_PATH}")
        return

    print("1. Loading Data...")
    df = pd.read_csv(DATA_PATH)
    print(f"   Loaded {len(df)} events.")
    
    print("2. Loading AI Model (Digital Twin Brain)...")
    # Setup Preprocessing exactly like training
    schema = DatasetSchema()
    pp_config = PreprocessingConfig(schema)
    preprocessor = TabularPreprocessor(pp_config)
    
    # We fit on this dataset to handle scaling for inference
    preprocessor.fit(df) 
    
    # Transform
    X = preprocessor.transform(df)
    
    input_size = X.shape[1]
    model = SupplyChainNet(input_size)
    
    if MODEL_PATH.exists():
        try:
            saved_state = torch.load(MODEL_PATH)
            # Check shape compatibility
            if saved_state['fc1.weight'].shape[1] == input_size:
                model.load_state_dict(saved_state)
                print("   ✅ Model loaded successfully.")
            else:
                print("   ⚠️  Model input mismatch (Training features != Current features).")
                print("       Using untrained model for demonstration logic (results may be random).")
        except Exception as e:
            print(f"   ⚠️  Model load failed: {e}")
            # Initialize with random weights if fail
    else:
         print("   ⚠️  Model file not found. Using initialized weights.")

    model.eval()
    
    print("3. Running AI Diagnostics...")
    with torch.no_grad():
        X_tensor = torch.tensor(X, dtype=torch.float32)
        # Predict Probability of Success (1.0 = Success, 0.0 = Delay/Failure)
        predictions = model(X_tensor).numpy().flatten()
        
    df['ai_prediction_success'] = predictions
    
    # 4. CALCULATE "AI RESIDUALS" (The Anomaly Score)
    # Actual Status: 1=OK, 0=Fail.
    # Prediction: 0.9=OK.
    # If Actual=0 (Fail) and Prediction=0.9 (Should be OK), then Residual = 0.9 (Unexpected Failure).
    # We want to find nodes where Unexpected Failures happen most.
    
    # We define inefficiency as: Prediction (Success) - Actual (Success)
    # High positive value means "AI thought it would work, but it failed".
    df['unexpected_failure_score'] = (df['ai_prediction_success'] - df['order_fulfillment_status'])
    df['inefficiency_score'] = df['unexpected_failure_score'].clip(lower=0) 
    
    print("4. Identifying Structural Bottlenecks...")
    # Group by Node
    node_stats = df.groupby(['node_id', 'node_type']).agg({
        'inefficiency_score': ['mean', 'sum', 'count'],
        'traffic_congestion_level': 'mean',
        'loading_unloading_time': 'mean'
    })
    
    node_stats.columns = ['avg_inefficiency', 'total_inefficiency', 'volume', 'traffic', 'loading_time']
    node_stats = node_stats.reset_index()
    
    # Filter specific node types (ignore low volume)
    target_nodes = node_stats[node_stats['volume'] > 50] 
    
    # Sort by 'avg_inefficiency' -> This means the AI *expected* better performance than reality.
    top_problems = target_nodes.sort_values(by='avg_inefficiency', ascending=False).head(5)
    
    print("\n🚨 TOP 5 AI-DETECTED ANOMALIES (Bottlenecks)")
    
    rank = 1
    for _, row in top_problems.iterrows():
        print(f"\n   #{rank} NODE: {row['node_id']} ({row['node_type']})")
        print(f"      - AI Inefficiency Score: {row['avg_inefficiency']:.4f} (Residual)")
        print(f"      - Interpretation: Performing worse than environmental factors (Traffic/Weather) suggest.")
        print(f"      - Avg Traffic: {row['traffic']:.2f}, Avg Load Time: {row['loading_time']:.2f}")
        
        # Recommendations based on data + AI insight
        if row['loading_time'] > 3.0:
            print("      🔧 AI RECOMMENDATION: Warehouse Process Optimization (Loading is slower than model predicts for typical warehouse).")
        elif row['traffic'] > 7.0:
            print("      🔧 AI RECOMMENDATION: Infrastructure Upgrade (Location is chronically congested beyond normal parameters).")
        else:
            print("      🔧 AI RECOMMENDATION: Audit Internal Operations (Hidden inefficiency detected).")
        
        rank += 1

    print("\n--------------------------------------------------")
    print("Summary: These nodes are underperforming relative to the AI's predictions.")
    print("Fixing these will yield the highest ROI.")
    print("--------------------------------------------------")

if __name__ == "__main__":
    analyze_bottlenecks()
