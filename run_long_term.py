
import sys
import pandas as pd
import random
import time
from pathlib import Path
from tqdm import tqdm

project_root = Path("d:/dev/Analysis-and-prediction-of-supply-chain")
sys.path.append(str(project_root))
sys.path.append(str(project_root / "src"))

from supply_chain.simulation.graph import GraphBuilder
from supply_chain.simulation.engine import SimulationEngine
from supply_chain.simulation.schema import Event, EventType, NodeType, Node, Edge
from supply_chain.simulation.integration import DataConverter, StatsCalibrator
from supply_chain.simulation.graph import haversine_distance
from supply_chain.config import DATA_RAW_DIR, REPORTS_DIR

def run_long_term_simulation(years=5):
    print(f"Starting Long-Term Simulation: {years} Years")
    print("Strategy: Month-by-Month execution with Seasonality injection.")
    
    # Setup Paths
    output_dir = Path(REPORTS_DIR) / "experiments" / "long_term_5y"
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "simulated_data_5y.csv"
    
    # 1. Initialize World (Scale: KROA100)
    gb = GraphBuilder()
    
    # Try looking for kroA100.txt in likely places
    possible_paths = [
        Path(DATA_RAW_DIR).parent / "kroA100.txt",
        Path(DATA_RAW_DIR) / "kroA100.txt",
        project_root / "data" / "kroA100.txt",
        project_root / "kroA100.txt"
    ]
    tsp_path = None
    for p in possible_paths:
        if p.exists():
            tsp_path = p
            break
            
    if tsp_path:
        print(f"Loading Map from: {tsp_path}")
        gb.create_from_tsplib(tsp_path, k_neighbors=6)
    else:
        print("Warning: kroA100.txt not found! Falling back to Random Graph.")
        gb.create_random_graph(num_nodes=40, k_neighbors=5)

    engine = SimulationEngine(gb)
    
    # Calibrator
    calibrator = StatsCalibrator(Path(DATA_RAW_DIR) / "dynamic_supply_chain_logistics_dataset.csv")
    calibrator.load_and_calibrate()
    
    # Spawn Trucks (Adjusted for kroA100 scale - MAX CAPACITY)
    # kroA100 has 100 nodes.
    num_trucks = 400 
    valid_spawn_nodes = [n.id for n in gb.nodes.values() if n.type not in [NodeType.CUSTOMER]]
    for i in range(num_trucks):
        start_node = random.choice(valid_spawn_nodes)
        engine.schedule_event(Event(0.0, f"T{i+1}", start_node, EventType.TRUCK_SPAWN))
        
    start_time = time.time()
    total_months = years * 12
    
    # Helper to append CSV
    header_written = False
    
    # Simulation Loop (Month by Month)
    # Assuming 1 Month = 30 Days = 43200 Minutes
    minutes_per_month = 30 * 24 * 60
    
    target_orders_per_month_base = 6000 
    
    all_node_ids = list(gb.nodes.keys())
    
    for month in range(1, total_months + 1):
        year_idx = (month - 1) // 12
        month_idx = (month - 1) % 12 + 1 # 1-12
        
        # --- SEASONALITY LOGIC ---
        # Winter (Dec, Jan, Feb): Worse Weather, High Traffic
        is_winter = month_idx in [12, 1, 2]
        # Q4 (Oct, Nov, Dec): High Demand
        is_peak_season = month_idx in [10, 11, 12]
        
        # Dynamic Calibration Overrides
        current_overrides = {}
        target_orders = target_orders_per_month_base
        
        if is_winter:
            current_overrides["weather_condition_severity"] = {"mean": 0.8, "min": 0.4}
            current_overrides["traffic_congestion_level"] = {"mean": 8.0}
        else:
            current_overrides["weather_condition_severity"] = {"mean": 0.2} # Nice Summer
        
        if is_peak_season:
            target_orders = int(target_orders_per_month_base * 1.5) # +50% demand
            current_overrides["supplier_reliability_score"] = {"mean": 0.6} # Suppliers stressed
        
        print(f"Simulating Month {month}/{total_months} (Year {year_idx+1}, M{month_idx}). Orders: {target_orders}. Winter: {is_winter}")

        # Update Calibrator with Seasonality
        for col, stats in current_overrides.items():
            if col in calibrator.stats:
                calibrator.stats[col].update(stats)
            else:
                calibrator.stats[col] = stats
                
        # Schedule Orders for this month
        # Time window: from current engine time to end of month
        start_t = engine.current_time
        end_t = start_t + minutes_per_month
        
        for i in range(target_orders):
            creation_time = random.uniform(start_t, end_t - 600)
            origin = random.choice(all_node_ids)
            dest = random.choice(all_node_ids)
            while dest == origin: dest = random.choice(all_node_ids)
            
            engine.schedule_event(Event(
                creation_time, "SYSTEM", origin, EventType.ORDER_CREATED,
                details={"order_id": f"ORD_Y{year_idx}_M{month_idx}_{i}", "origin": origin, "destination": dest}
            ))
            
        # Run Simulation Chunk
        engine.run(duration=minutes_per_month)
        
        # Extract Data & Flush
        # Note: integration.py typically processes ALL events. 
        # For efficiency, we should ideally clear processed_events, 
        # BUT Graph/Truck state relies on history.
        # We will extract only NEW events.
        
        new_events = [e for e in engine.processed_events if e.time >= start_t and e.time < end_t]
        
        if new_events:
            df_chunk = DataConverter.events_to_dataframe(new_events, calibrator, engine, gb)
            
            # Add seasonality meta-features (optional, but engine doesn't track date naturally)
            df_chunk["month"] = month_idx
            df_chunk["is_peak_season"] = 1 if is_peak_season else 0
            
            mode = 'w' if not header_written else 'a'
            header = not header_written
            df_chunk.to_csv(csv_path, mode=mode, header=header, index=False)
            header_written = True
            
        # Optimization: Clear old processed events to save RAM
        # We keep the objects (trucks/orders) but drop event history from list
        # We keep last few for context if needed, but for now clear all purely handled
        engine.processed_events = [] 
        
    print(f"\nLong-Term Simulation Completed in {time.time() - start_time:.1f}s")
    print(f"Data saved to {csv_path}")

if __name__ == "__main__":
    run_long_term_simulation(years=5)
