
import sys
from pathlib import Path

# Add project root to sys.path
project_root = Path("d:/dev/Analysis-and-prediction-of-supply-chain")
sys.path.append(str(project_root))
sys.path.append(str(project_root / "src"))

from supply_chain.research.experiments import run_experiment

try:
    print("Running Fast Baseline Experiment...")
    # Reduced scale: 3 days, 150 orders (quicker)
    report = run_experiment(
        experiment_name="baseline_fast",
        num_trucks=15, 
        num_orders=150, 
        duration_days=3,
        epochs=10
    )
    print("Baseline Experiment Completed Successfully!")
    print("\nMetrics:")
    for k, v in report["metrics"].items():
        print(f"  {k}: {v}")
        
except Exception as e:
    print(f"Experiment Failed: {e}")
    import traceback
    traceback.print_exc()
