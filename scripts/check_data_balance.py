
import pandas as pd
from pathlib import Path

def check_balance():
    print("📊 Analysing Data Balance...")
    project_root = Path(".").resolve()
    data_path = project_root / "data" / "raw" / "simulated_supply_chain_data_2021_2025.csv"
    
    if not data_path.exists():
        print("❌ Data not found.")
        return

    df = pd.read_csv(data_path)
    total = len(df)
    
    # Calculate Risk (Logic from dataset.py)
    # risk = 1.0 if avg_delay > 60 else (avg_delay / 60.0 if avg_delay > 0 else 0)
    
    # We need to replicate the grouping logic roughly
    # Group by node_id and some time bucket? 
    # Or just check raw 'delivery_time_deviation' stats which drive the risk
    
    print(f"Total Records: {total}")
    
    if 'delivery_time_deviation' not in df.columns:
        print("❌ 'delivery_time_deviation' column missing.")
        return

    delays = df['delivery_time_deviation']
    
    print("\n--- Delay Statistics (Raw) ---")
    print(delays.describe())
    
    # Bucket Analysis (Assuming 1-hour buckets like dataset.py)
    # We can't perfectly replicate without the graph object, but we can check if HIGH delays exist.
    
    high_risk_count = len(df[df['delivery_time_deviation'] > 60])
    medium_risk_count = len(df[(df['delivery_time_deviation'] > 15) & (df['delivery_time_deviation'] <= 60)])
    low_risk_count = len(df[df['delivery_time_deviation'] <= 15])
    
    print("\n--- Event Risk Distribution (Approx) ---")
    print(f"High Risk (>60m):   {high_risk_count} ({high_risk_count/total*100:.2f}%)")
    print(f"Med Risk (15-60m):  {medium_risk_count} ({medium_risk_count/total*100:.2f}%)")
    print(f"Low Risk (<15m):    {low_risk_count} ({low_risk_count/total*100:.2f}%)")
    
    if high_risk_count == 0:
        print("\n❌ CRITICAL: No High Risk events in dataset. Model cannot learn to predict them!")

if __name__ == "__main__":
    check_balance()
