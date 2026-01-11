
import sys
import pandas as pd
import numpy as np
import random
import time
from pathlib import Path

project_root = Path("d:/dev/Analysis-and-prediction-of-supply-chain")
sys.path.append(str(project_root))
sys.path.append(str(project_root / "src"))

from supply_chain.simulation.graph import GraphBuilder
from supply_chain.simulation.engine import SimulationEngine
from supply_chain.simulation.schema import Event, EventType, NodeType, Node, Edge
from supply_chain.simulation.integration import DataConverter, StatsCalibrator
from supply_chain.config import DATA_RAW_DIR, REPORTS_DIR

def run_scenarios():
    output_dir = Path(REPORTS_DIR) / "experiments" / "scenarios"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. Setup Simulation (Base)
    print("Initializing Simulation for Scenarios...")
    gb = GraphBuilder()
    
    # 1. Setup Simulation (Base)
    print("Initializing Simulation for Scenarios...")
    gb = GraphBuilder()
    
    # Robust Path Finding for kroA100
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
        gb.create_random_graph(30, 4)
        
    engine = SimulationEngine(gb)
    calibrator = StatsCalibrator(Path(DATA_RAW_DIR) / "dynamic_supply_chain_logistics_dataset.csv")
    calibrator.load_and_calibrate()
    
    # Spawn Trucks (Massive Scale)
    num_trucks = 400
    valid_nodes = [n.id for n in gb.nodes.values() if n.type != NodeType.CUSTOMER]
    for i in range(num_trucks):
        engine.schedule_event(Event(0.0, f"T{i}", random.choice(valid_nodes), EventType.TRUCK_SPAWN))
        
    # Generate Baseline Content for 3 Months (Dense)
    start_t = time.time()
    months = 3
    minutes = months * 30 * 24 * 60
    target_orders = 15000 # Large dataset
    
    # Schedule Orders
    all_nodes = list(gb.nodes.keys())
    for i in range(target_orders):
        t = random.uniform(0, minutes)
        engine.schedule_event(Event(t, "SYS", random.choice(all_nodes), EventType.ORDER_CREATED, 
                                  details={"order_id": f"S_ORD_{i}", "origin": random.choice(all_nodes), "destination": random.choice(all_nodes)}))
    
    print("Running Baseline Simulation...")
    engine.run(minutes)
    
    df = DataConverter.events_to_dataframe(engine.processed_events, calibrator, engine, gb)
    print(f"Generated Baseline: {len(df)} events.")
    
    # ==========================================
    # GENERATE 10 DISTINCT SCENARIOS
    # ==========================================
    print("\n⚡ Injecting Anomalies (Creating 10 Test Words)...")
    
    # helper
    def save_scenario(df_mod, name, description):
        p = output_dir / f"scenario_{name}.csv"
        df_mod.to_csv(p, index=False)
        print(f"   [{name}] Generated: {description}")
        return p

    # 1. OBVIOUS FAIL: N1 always fails
    df_1 = df.copy()
    target_1 = "N1"
    mask_1 = df_1['node_id'] == target_1
    df_1.loc[mask_1, 'order_fulfillment_status'] = 0.0
    df_1.loc[mask_1, 'delivery_time_deviation'] = 180.0 # 3 hr delay
    save_scenario(df_1, "01_obvious_fail", f"Node {target_1} always fails.")

    # 2. SUBTLE INEFFICIENCY: N5 fails 20% more often than expected
    df_2 = df.copy()
    target_2 = "N5"
    mask_2 = df_2['node_id'] == target_2
    indices_2 = df_2[mask_2].index
    sabotage_2 = np.random.choice(indices_2, size=int(len(indices_2)*0.25), replace=False)
    df_2.loc[sabotage_2, 'order_fulfillment_status'] = 0.0
    df_2.loc[sabotage_2, 'delivery_time_deviation'] = 45.0
    save_scenario(df_2, "02_subtle_inefficiency", f"Node {target_2} has hidden 25% failure rate.")
    
    # 3. WEEKEND SLUMP: N10 collapses on Sat/Sun
    df_3 = df.copy()
    target_3 = "N10"
    df_3['dt'] = pd.to_datetime(df_3['timestamp'])
    df_3['is_weekend'] = df_3['dt'].dt.dayofweek >= 5
    mask_3 = (df_3['node_id'] == target_3) & (df_3['is_weekend'])
    df_3.loc[mask_3, 'order_fulfillment_status'] = 0.0
    df_3.loc[mask_3, 'delivery_time_deviation'] = 90.0
    df_3.drop(columns=['dt', 'is_weekend'], inplace=True)
    save_scenario(df_3, "03_weekend_slump", f"Node {target_3} fails only on Weekends.")

    # 4. RAIN PHOBIA: N20 collapses if weather > 0.5
    df_4 = df.copy()
    target_4 = "N20"
    mask_4 = (df_4['node_id'] == target_4) & (df_4['weather_condition_severity'] > 0.5)
    df_4.loc[mask_4, 'order_fulfillment_status'] = 0.0
    df_4.loc[mask_4, 'delivery_time_deviation'] = 100.0
    save_scenario(df_4, "04_rain_phobia", f"Node {target_4} cannot handle Rain (Weather > 0.5).")

    # 5. TRAFFIC CHOKE: N30 dies if traffic > 7.0 (Non-linear)
    df_5 = df.copy()
    target_5 = "N30"
    mask_5 = (df_5['node_id'] == target_5) & (df_5['traffic_congestion_level'] > 7.0)
    df_5.loc[mask_5, 'order_fulfillment_status'] = 0.0
    df_5.loc[mask_5, 'delivery_time_deviation'] = 150.0
    save_scenario(df_5, "05_traffic_choke", f"Node {target_5} collapses under Heavy Traffic (>7.0).")

    # 6. ZOMBIE FLEET: Trucks T50-T60 always late
    df_6 = df.copy()
    zombies = [f"T{i}" for i in range(50, 61)]
    mask_6 = df_6['truck_id'].isin(zombies)
    df_6.loc[mask_6, 'order_fulfillment_status'] = 0.0
    df_6.loc[mask_6, 'delivery_time_deviation'] = 60.0
    save_scenario(df_6, "06_zombie_fleet", f"Trucks T50-T60 have mechanical issues (Always Late).")

    # 7. ROUTE CURSE: N40 -> N41 always fails
    # To detect 'Route', we look at current node. If Current=N41 and Prev=N40... hard to track "Prev" in flat CSV.
    # Proxy: Fail events at N41.
    df_7 = df.copy()
    target_7 = "N41"
    # Sabotage all arrivals at N41 (simulating incoming route failure)
    mask_7 = (df_7['node_id'] == target_7) & (df_7['event_type'] == "ARRIVAL_NODE")
    df_7.loc[mask_7, 'order_fulfillment_status'] = 0.0
    df_7.loc[mask_7, 'delivery_time_deviation'] = 120.0
    save_scenario(df_7, "07_route_curse", f"Network Segment ending at {target_7} is broken.")

    # 8. COLD CHAIN BREAK: Temp > 5.0 causes Failure at N50
    df_8 = df.copy()
    target_8 = "N50"
    mask_8 = (df_8['node_id'] == target_8) & (df_8['iot_temperature'] > 5.0)
    df_8.loc[mask_8, 'order_fulfillment_status'] = 0.0
    df_8.loc[mask_8, 'delivery_time_deviation'] = 300.0 # Spoilage
    save_scenario(df_8, "08_cold_chain_break", f"Node {target_8} has broken fridges (Temp > 5.0 fails).")

    # 9. MORNING RUSH: 8am-10am failures at N60
    df_9 = df.copy()
    target_9 = "N60"
    df_9['dt'] = pd.to_datetime(df_9['timestamp'])
    df_9['hour'] = df_9['dt'].dt.hour
    mask_9 = (df_9['node_id'] == target_9) & (df_9['hour'].between(8, 10))
    df_9.loc[mask_9, 'order_fulfillment_status'] = 0.0
    df_9.loc[mask_9, 'delivery_time_deviation'] = 45.0
    df_9.drop(columns=['dt', 'hour'], inplace=True)
    save_scenario(df_9, "09_morning_rush", f"Node {target_9} cannot handle Morning Rush (8-10am).")
    
    # 10. RANDOM CHAOS: 5% random failure everywhere (Baseline Noise Check)
    df_10 = df.copy()
    indices_10 = df_10.index
    sabotage_10 = np.random.choice(indices_10, size=int(len(indices_10)*0.05), replace=False)
    df_10.loc[sabotage_10, 'order_fulfillment_status'] = 0.0
    df_10.loc[sabotage_10, 'delivery_time_deviation'] = random.uniform(10, 100)
    save_scenario(df_10, "10_random_chaos", f"Global 5% Entropy increase (Noise Test).")

    print("\n✅ All 10 Scenarios Generated Successfully!")

if __name__ == "__main__":
    run_scenarios()
