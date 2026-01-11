import pandas as pd
import json
import torch
from pathlib import Path
from typing import Dict, Any

from supply_chain.simulation.graph import GraphBuilder
from supply_chain.simulation.engine import SimulationEngine
from supply_chain.simulation.schema import Event, EventType, NodeType
from supply_chain.simulation.integration import StatsCalibrator, DataConverter
from supply_chain.config import DATA_RAW_DIR, REPORTS_DIR, DatasetSchema
from supply_chain.data.cleaner import DataCleaner
from supply_chain.data.preprocessing import PreprocessingConfig, TabularPreprocessor
from supply_chain.model.dataset import SupplyChainDataset
from supply_chain.model.train import train_model
from sklearn.model_selection import train_test_split
from torch.utils.data import DataLoader

import random

def run_experiment(
    experiment_name: str,
    num_trucks: int = 15,
    num_orders: int = 50,
    epochs: int = 5,
    duration_days: int = 7,
    calibration_overrides: Dict[str, Dict[str, float]] = None, # New parameter
    output_dir: Path = None
) -> Dict[str, Any]:
    
    if output_dir is None:
        output_dir = Path(REPORTS_DIR) / "experiments" / experiment_name
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. Run Simulation
    print(f"[{experiment_name}] Starting Simulation ({duration_days} days)...")
    gb = GraphBuilder()
    gb.create_random_graph(num_nodes=15, k_neighbors=3)
    engine = SimulationEngine(gb)
    
    # Spawn trucks
    valid_spawn_nodes = [n.id for n in gb.nodes.values() if n.type not in [NodeType.CUSTOMER]]
    for i in range(num_trucks):
        start_node = random.choice(valid_spawn_nodes)
        engine.schedule_event(Event(0.0, f"T{i+1}", start_node, EventType.TRUCK_SPAWN))
        
    # Generate orders
    all_node_ids = list(gb.nodes.keys())
    # Adjust order generation window to match duration
    max_creation_time = (duration_days * 24 * 60) - 600 # Stop creating orders 10h before end
    
    for i in range(num_orders):
        creation_time = random.uniform(0, max_creation_time)
        origin = random.choice(all_node_ids)
        dest = random.choice(all_node_ids)
        while dest == origin:
            dest = random.choice(all_node_ids)
        
        engine.schedule_event(Event(
            creation_time, "SYSTEM", origin, EventType.ORDER_CREATED,
            details={"order_id": f"ORD{i+1}", "origin": origin, "destination": dest}
        ))
        
    engine.run(duration=duration_days * 24 * 60)
    
    # Convert to DataFrame
    calibrator = StatsCalibrator(Path(DATA_RAW_DIR) / "dynamic_supply_chain_logistics_dataset.csv")
    calibrator.load_and_calibrate()
    
    # Apply overrides
    if calibration_overrides:
        print(f"[{experiment_name}] Applying calibration overrides...")
        for col, stats in calibration_overrides.items():
            if col in calibrator.stats:
                calibrator.stats[col].update(stats)
            else:
                calibrator.stats[col] = stats
    
    df_simulated = DataConverter.events_to_dataframe(engine.processed_events, calibrator, engine, gb)
    df_simulated.to_csv(output_dir / "simulated_data.csv", index=False)
    
    # 2. Preprocess
    print(f"[{experiment_name}] Preprocessing...")
    schema = DatasetSchema()
    cleaner = DataCleaner(schema)
    df_clean = cleaner.clean(df_simulated)
    
    pp_config = PreprocessingConfig(schema)
    preprocessor = TabularPreprocessor(pp_config)
    features_array = preprocessor.fit_transform(df_clean)
    
    # 3. Train Model
    print(f"[{experiment_name}] Training...")
    y = df_clean[schema.target_column].values
    X_train, X_val, y_train, y_val = train_test_split(features_array, y, test_size=0.2, random_state=42)
    
    train_loader = DataLoader(SupplyChainDataset(X_train, y_train), batch_size=32, shuffle=True)
    val_loader = DataLoader(SupplyChainDataset(X_val, y_val), batch_size=32, shuffle=False)
    
    input_size = X_train.shape[1]
    model_path = output_dir / "model.pth"
    _, metrics = train_model(train_loader, val_loader, input_size, epochs=epochs, save_path=model_path)
    
    # Save Report
    report = {
        "experiment_name": experiment_name,
        "config": {
            "num_trucks": num_trucks,
            "num_orders": num_orders,
            "epochs": epochs
        },
        "metrics": metrics
    }
    
    with open(output_dir / "report.json", "w") as f:
        json.dump(report, f, indent=4)
        
    print(f"[{experiment_name}] Experiment Completed. Metrics: {metrics}")
    return report
