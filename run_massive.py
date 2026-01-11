
import sys
from pathlib import Path

project_root = Path("d:/dev/Analysis-and-prediction-of-supply-chain")
sys.path.append(str(project_root))
sys.path.append(str(project_root / "src"))

from supply_chain.research.experiments import run_experiment

try:
    print("Running MASSIVE Data Generation...")
    print("This may take a minute or two. Simulating 2 weeks of operations...")
    
    # We use "Chaos" settings to ensuring meaningful failures occur
    chaos_overrides = {
        "traffic_congestion_level": {"mean": 7.0, "min": 2.0}, 
        "weather_condition_severity": {"mean": 0.6},
        "delay_probability": {"mean": 0.6}
    }

    # 14 Days, 2000 Orders, 30 Trucks
    report = run_experiment(
        experiment_name="experiment_massive",
        num_trucks=30, 
        num_orders=2000, 
        duration_days=14,
        epochs=15,
        calibration_overrides=chaos_overrides
    )
    
    print("\nMassive Experiment Completed!")
    print(f"Data saved to: {Path('reports/experiments/experiment_massive/simulated_data.csv').absolute()}")
    print("\nMetrics (Initial Training):")
    for k, v in report["metrics"].items():
        print(f"  {k}: {v}")
        
except Exception as e:
    print(f"Massive Run Failed: {e}")
    import traceback
    traceback.print_exc()
