
import sys
from pathlib import Path

project_root = Path("d:/dev/Analysis-and-prediction-of-supply-chain")
sys.path.append(str(project_root))
sys.path.append(str(project_root / "src"))

from supply_chain.research.experiments import run_experiment

try:
    print("Running CHAOS Experiment...")
    
    # Overrides to make the world "worse"
    chaos_overrides = {
        "traffic_congestion_level": {"mean": 8.0, "min": 5.0}, # High traffic
        "weather_condition_severity": {"mean": 0.8, "min": 0.5}, # Bad weather
        "supplier_reliability_score": {"mean": 0.3}, # Unreliable suppliers
        "delay_probability": {"mean": 0.8}, # High chance of delay
        "disruption_likelihood_score": {"mean": 0.7}
    }

    report = run_experiment(
        experiment_name="experiment_chaos",
        num_trucks=20, 
        num_orders=200, 
        duration_days=3,
        epochs=15,
        calibration_overrides=chaos_overrides
    )
    print("Chaos Experiment Completed!")
    print("\nMetrics:")
    for k, v in report["metrics"].items():
        print(f"  {k}: {v}")
        
except Exception as e:
    print(f"Chaos Failed: {e}")
    import traceback
    traceback.print_exc()
