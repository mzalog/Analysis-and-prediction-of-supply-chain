
import sys
import pandas as pd
import random
import time
from pathlib import Path
from datetime import datetime, timedelta

# Add src to path
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))
sys.path.append(str(project_root / "src"))

from supply_chain.simulation.graph import GraphBuilder
from supply_chain.simulation.engine import SimulationEngine
from supply_chain.simulation.schema import Event, EventType, NodeType
from supply_chain.simulation.integration import DataConverter, StatsCalibrator
from supply_chain.config import DATA_RAW_DIR

def run_timed_simulation(duration_str="3 months"):
    print(f"🚀 Initializing Simulation for {duration_str}...")
    
    # 1. Setup Graph & Engine
    gb = GraphBuilder()
    
    # Try to load kroA100
    tsp_path = project_root / "data" / "kroA100.txt"
    if not tsp_path.exists():
        tsp_path = project_root / "kroA100.txt"
        
    if tsp_path.exists():
        print(f"   Using Map: {tsp_path.name}")
        gb.create_from_tsplib(tsp_path, k_neighbors=4)
    else:
        print("   Warning: Using Random Graph (kroA100 not found)")
        gb.create_random_graph(20, 3)
        
    engine = SimulationEngine(gb)
    
    # 2. Setup Digital Twin Data (Calibrator)
    # Ensure all data is passed by loading the calibrator
    calib_path = Path(DATA_RAW_DIR) / "dynamic_supply_chain_logistics_dataset.csv"
    if calib_path.exists():
        calibrator = StatsCalibrator(calib_path)
        calibrator.load_and_calibrate()
        print("   ✅ Data Calibrator Loaded (Historical Distributions)")
    else:
        print("   ⚠️ Data Calibrator using Defaults (File not found)")
        calibrator = StatsCalibrator()

    # 3. Spawn Massive Fleet (Scale for 3 months)
    # Ensure enough trucks to keep it moving
    valid_starts = [n.id for n in gb.nodes.values() if n.type != NodeType.CUSTOMER]
    num_trucks = 50
    print(f"   🚛 Spawning {num_trucks} trucks...")
    for i in range(num_trucks):
        start_node = random.choice(valid_starts)
        engine.schedule_event(Event(0.0, f"T{i}", start_node, EventType.TRUCK_SPAWN))

    # 4. Define Duration
    # 3 months = 90 days
    days = 90
    total_minutes = days * 24 * 60
    
    # Base Date for Visualization
    start_date = datetime(2024, 1, 1, 0, 0)
    
    print(f"   ⏱️  Target Duration: {total_minutes} minutes ({days} days)")
    print(f"   📅 Simulation Interval: {start_date} -> {start_date + timedelta(minutes=total_minutes)}")
    
    # 5. Populate Initial Orders
    # Create enough initial orders to kickstart the system
    all_nodes = list(gb.nodes.keys())
    for i in range(50):
        engine.schedule_event(Event(
            random.uniform(0, 120), 
            "SYS", random.choice(all_nodes), EventType.ORDER_CREATED,
            details={"order_id": f"INIT_{i}", "origin": random.choice(all_nodes), "destination": random.choice(all_nodes)}
        ))

    # 6. Run Loop with Progress
    # We will step manually to show progress
    start_time_proc = time.time()
    
    # Only print every N steps or M minutes of sim time
    last_print = -1.0
    print_interval_sim_min = 60 * 24 # Print every Sim Day
    
    print("\n▶️ Starting Simulation...\n")
    
    try:
        while engine.current_time < total_minutes:
            if not engine.event_queue:
                print("   ⚠️ Event Queue Empty! Spawning more orders...")
                # Auto-replenish to keep it alive for 3 months
                for _ in range(10):
                     engine.schedule_event(Event(
                        engine.current_time + random.uniform(1, 60), 
                        "SYS", random.choice(all_nodes), EventType.ORDER_CREATED,
                        details={"order_id": f"AUTO_{int(engine.current_time)}_{random.randint(0,999)}", 
                                 "origin": random.choice(all_nodes), 
                                 "destination": random.choice(all_nodes)}
                    ))
            
            engine.step()
            
            # Progress Display
            if engine.current_time - last_print >= print_interval_sim_min:
                last_print = engine.current_time
                curr_date = start_date + timedelta(minutes=engine.current_time)
                
                # Dynamic Order Injection for Long Term
                # Every day inject ~50 new orders to simulate continuous operation
                for _ in range(random.randint(30, 60)):
                     engine.schedule_event(Event(
                        engine.current_time + random.uniform(1, 1440), 
                        "SYS", random.choice(all_nodes), EventType.ORDER_CREATED,
                        details={"order_id": f"DAY_{curr_date.day}_{random.randint(0,9999)}", 
                                 "origin": random.choice(all_nodes), 
                                 "destination": random.choice(all_nodes)}
                    ))
                
                # Fancy Output
                progress = (engine.current_time / total_minutes) * 100
                sys.stdout.write(f"\r⏳ Progress: {progress:5.1f}% | Date: {curr_date.strftime('%Y-%m-%d %H:%M')} | Events: {len(engine.processed_events)}")
                sys.stdout.flush()

    except KeyboardInterrupt:
        print("\n🛑 Stopped by user.")
        
    end_time_proc = time.time()
    elapsed = end_time_proc - start_time_proc
    
    print(f"\n\n✅ Done! Simulator ran for {days} days in {elapsed:.2f} real seconds.")
    print(f"📊 Total Events Processed: {len(engine.processed_events)}")
    
    # Validation
    if len(engine.processed_events) < 1000:
        print("⚠️ Warning: Low event count. Did the simulation stall?")
    else:
        print("👍 Simulation appears healthy.")

if __name__ == "__main__":
    run_timed_simulation()
