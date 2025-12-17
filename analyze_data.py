import pandas as pd
import sys

try:
    df = pd.read_csv(r"d:\dev\big-data-supply-chain\data\raw\dynamic_supply_chain_logistics_dataset.csv")
    
    print("--- Loading/Unloading Time Stats ---")
    if "loading_unloading_time" in df.columns:
        stats = df["loading_unloading_time"].describe()
        print(stats)
    else:
        print("Column 'loading_unloading_time' not found.")

    print("\n--- Lead Time (days) Stats ---")
    if "lead_time_days" in df.columns:
        stats = df["lead_time_days"].describe()
        print(stats)
    else:
        print("Column 'lead_time_days' not found.")
        
except Exception as e:
    print(f"Error: {e}")
