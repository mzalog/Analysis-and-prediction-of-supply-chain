
import sys
import pandas as pd
import random
import time
from pathlib import Path
from datetime import datetime

project_root = Path("d:/dev/Analysis-and-prediction-of-supply-chain")
sys.path.append(str(project_root))
sys.path.append(str(project_root / "src"))

from supply_chain.simulation.graph import GraphBuilder
from supply_chain.simulation.engine import SimulationEngine
from supply_chain.simulation.schema import Event, EventType, NodeType
from supply_chain.simulation.integration import DataConverter, StatsCalibrator
from supply_chain.config import DATA_RAW_DIR, REPORTS_DIR

def run_long_term_simulation(years=4):
    print(f"Starting Long-Term Simulation: {years} Years (2021-2025)")
    print("Strategy: Month-by-Month execution with Seasonality injection.")
    
    # Setup Paths
    output_dir = Path(project_root) / "data" / "raw"
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "simulated_supply_chain_data_2021_2025.csv"
    
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
    num_trucks = 400 
    valid_spawn_nodes = [n.id for n in gb.nodes.values() if n.type not in [NodeType.CUSTOMER]]
    for i in range(num_trucks):
        start_node = random.choice(valid_spawn_nodes)
        engine.schedule_event(Event(0.0, f"T{i+1}", start_node, EventType.TRUCK_SPAWN))
        
    start_time = time.time()
    total_months = years * 12
    
    # Helper to append CSV
    header_written = False
    
    # Define Start Date
    start_date = datetime(2021, 1, 1)
    
    # Simulation Loop (Month by Month)
    minutes_per_month = 30 * 24 * 60
    
    target_orders_per_month_base = 6000 
    
    all_node_ids = list(gb.nodes.keys())
    
    for month in range(1, total_months + 1):
        year_idx = (month - 1) // 12
        month_idx = (month - 1) % 12 + 1 # 1-12
        current_year = 2021 + year_idx
        
        # --- SEASONALITY LOGIC ---
        is_winter = month_idx in [12, 1, 2]
        is_peak_season = month_idx in [10, 11, 12]
        
        current_overrides = {}
        target_orders = target_orders_per_month_base
        
        if is_winter:
            current_overrides["weather_condition_severity"] = {"mean": 0.8, "min": 0.4}
            current_overrides["traffic_congestion_level"] = {"mean": 8.0}
        else:
            current_overrides["weather_condition_severity"] = {"mean": 0.2} 
        
        if is_peak_season:
            target_orders = int(target_orders_per_month_base * 1.5) 
            current_overrides["supplier_reliability_score"] = {"mean": 0.6}
            
        # Randomize order volume (+/- 20%) to create variety
        fluctuation = random.uniform(0.8, 1.2)
        target_orders = int(target_orders * fluctuation)
        
        print(f"Simulating Month {month}/{total_months} ({current_year}-{month_idx:02d}). Orders: {target_orders}. Winter: {is_winter}")

        for col, stats in current_overrides.items():
            if col in calibrator.stats:
                calibrator.stats[col].update(stats)
            else:
                calibrator.stats[col] = stats
                
        start_t = engine.current_time
        end_t = start_t + minutes_per_month
        
        # --- CRITICAL FIX: MONTHLY RESET ---
        # If we don't clear the backlog, year 4 has 200,000 pending orders.
        # This simulates "Orders from previous month rolled over or cancelled"
        # We perform a "Soft Reset" of the queue to keep utilization realistic.
        leftover = len(engine.pending_orders)
        if leftover > 500:
            # Keep only last 500 orders, cancel rest effectively (remove from queue)
            # This prevents infinite growth of utilization ratio
            engine.pending_orders = engine.pending_orders[-500:] 
            
        for i in range(target_orders):
            creation_time = random.uniform(start_t, end_t - 600)
            origin = random.choice(all_node_ids)
            dest = random.choice(all_node_ids)
            while dest == origin: dest = random.choice(all_node_ids)
            
            engine.schedule_event(Event(
                creation_time, "SYSTEM", origin, EventType.ORDER_CREATED,
                details={"order_id": f"ORD_Y{year_idx}_M{month_idx}_{i}", "origin": origin, "destination": dest}
            ))
            
        # BUG FIX: Use absolute end_t, not relative duration
        engine.run(duration=end_t)
        
        new_events = [e for e in engine.processed_events if e.time >= start_t and e.time < end_t]
        
        if new_events:
            df_chunk = DataConverter.events_to_dataframe(new_events, calibrator, engine, gb, start_date=start_date)
            df_chunk["month"] = month_idx
            df_chunk["year"] = current_year
            df_chunk["is_peak_season"] = 1 if is_peak_season else 0
            
            mode = 'w' if not header_written else 'a'
            header = not header_written
            df_chunk.to_csv(csv_path, mode=mode, header=header, index=False)
            header_written = True
            
        engine.processed_events = [] 
        
    print(f"\nLong-Term Simulation Completed in {time.time() - start_time:.1f}s")
    print(f"Data saved to {csv_path}")

if __name__ == "__main__":
    run_long_term_simulation(years=4)
