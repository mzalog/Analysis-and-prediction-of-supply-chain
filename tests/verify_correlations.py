import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path

def load_and_prep_data(filepath):
    """Loads CSV and normalizes column names."""
    try:
        df = pd.read_csv(filepath)
        # Normalize columns: lowercase, replace spaces/slashes with underscore
        df.columns = [c.lower().replace(' ', '_').replace('/', '_').replace('(', '').replace(')', '') for c in df.columns]
        return df
    except Exception as e:
        print(f"Error loading {filepath}: {e}")
        return None

def compare_datasets():
    base_dir = Path(__file__).parents[1]
    raw_data_path = base_dir / "data/raw/dynamic_supply_chain_logistics_dataset.csv"
    sim_data_path = base_dir / "data/raw/simulated_kaggle_compatible.csv"

    print(f"Loading Original: {raw_data_path}")
    df_orig = load_and_prep_data(raw_data_path)
    
    print(f"Loading Simulated: {sim_data_path}")
    df_sim = load_and_prep_data(sim_data_path)

    if df_orig is None or df_sim is None:
        return

    # Identify common numerical columns
    num_cols_orig = set(df_orig.select_dtypes(include=[np.number]).columns)
    num_cols_sim = set(df_sim.select_dtypes(include=[np.number]).columns)
    
    common_cols = list(num_cols_orig.intersection(num_cols_sim))
    common_cols.sort()
    
    print(f"\nCommon Numerical Columns ({len(common_cols)}):")
    print(", ".join(common_cols))

    # Calculate Correlations
    corr_orig = df_orig[common_cols].corr()
    corr_sim = df_sim[common_cols].corr()

    # Compare Statistics
    print("\n--- Statistical Comparison (Mean) ---")
    stats_diff = pd.DataFrame({
        'Original Mean': df_orig[common_cols].mean(),
        'Simulated Mean': df_sim[common_cols].mean(),
        'Diff %': ((df_sim[common_cols].mean() - df_orig[common_cols].mean()) / df_orig[common_cols].mean() * 100).round(2)
    })
    print(stats_diff)

    # Compare Correlations
    print("\n--- Correlation Matrix Difference (Sim - Orig) > 0.3 ---")
    corr_diff = corr_sim - corr_orig
    
    # Filter for significant differences
    high_diff = corr_diff[(corr_diff.abs() > 0.3) & (corr_diff != 0.0)]
    if not high_diff.dropna(how='all').dropna(axis=1, how='all').empty:
        print(high_diff.dropna(how='all').dropna(axis=1, how='all'))
    else:
        print("No major correlation discrepancies (> 0.3) found.")

    # Specific check for key supply chain relationships
    #  lead_time_days vs shipping_costs, or route_risk_level vs delay_probability
    key_pairs = [
        ('route_risk_level', 'delay_probability'),
        ('loading_unloading_time', 'warehouse_inventory_level'),
        ('shipping_costs', 'lead_time_days')
    ]
    
    print("\n--- Key Relationship Check ---")
    for col1, col2 in key_pairs:
        if col1 in common_cols and col2 in common_cols:
            c_orig = corr_orig.loc[col1, col2]
            c_sim = corr_sim.loc[col1, col2]
            print(f"{col1} vs {col2}: Orig={c_orig:.2f}, Sim={c_sim:.2f} (Diff={c_sim-c_orig:.2f})")

if __name__ == "__main__":
    compare_datasets()
