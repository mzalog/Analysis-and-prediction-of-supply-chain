import random
import sys
import time
from datetime import datetime
from pathlib import Path

project_root = Path("d:/dev/Analysis-and-prediction-of-supply-chain")
sys.path.append(str(project_root))
sys.path.append(str(project_root / "src"))

from supply_chain.config import DATA_RAW_DIR
from supply_chain.simulation.engine import SimulationEngine
from supply_chain.simulation.graph import GraphBuilder
from supply_chain.simulation.integration import DataConverter, StatsCalibrator
from supply_chain.simulation.schema import Event, EventType, NodeType

def run_episodes(num_episodes=10, months_per_episode=3):
    print(f"🚀 Starting Episode Generation: {num_episodes} Episodes x {months_per_episode} Months")
    
    # Setup Paths
    output_dir = Path(project_root) / "data" / "raw"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Clean old episodes
    for old_file in output_dir.glob("episode_*.csv"):
        old_file.unlink()
    
    # Calibrator (Loaded once, reused)
    calibrator = StatsCalibrator(Path(DATA_RAW_DIR) / "dynamic_supply_chain_logistics_dataset.csv")
    calibrator.load_and_calibrate()
    
    start_global = time.time()
    
    for episode_idx in range(num_episodes):
        print(f"\n🎬 [Episode {episode_idx+1}/{num_episodes}] Initializing...")
        
        # 1. Initialize World (New Seed/Topology per Episode)
        # Random seed based on time + episode to ensure variety
        random.seed(time.time() + episode_idx)
        
        gb = GraphBuilder()
        # 50% chance to use Real Map (if exists), 50% Random Graph 
        # This forces model to learn general GNN rules, not just memorize one map layout!
        # (Or strict usage of kroA100 if preferred, but variety is better)
        tsp_path = project_root / "kroA100.txt"
        
        if tsp_path.exists():
             gb.create_from_tsplib(tsp_path, k_neighbors=6)
        else:
             # Fallback only if file missing
             gb.create_random_graph(num_nodes=30, k_neighbors=4)
             
        engine = SimulationEngine(gb)
        
        # Spawn Trucks
        num_trucks = random.randint(200, 500)
        valid_spawn_nodes = [n.id for n in gb.nodes.values() if n.type not in [NodeType.CUSTOMER]]
        if not valid_spawn_nodes: valid_spawn_nodes = list(gb.nodes.keys())
        
        for i in range(num_trucks):
            start_node = random.choice(valid_spawn_nodes)
            engine.schedule_event(Event(0.0, f"T{i+1}", start_node, EventType.TRUCK_SPAWN))
            
        # 2. Episode Bias (Balancing Helper - Mixed Regimes)
        # We want a spectrum: Normal -> Stressed -> Crisis
        rng = random.random()
        episode_type = "NORMAL"
        bias_weather_mean = 0.0
        bias_traffic_mean = 0.0
        
        if rng < 0.5:
            episode_type = "NORMAL"
        elif rng < 0.8:
            episode_type = "STRESSED" # Moderate
            # Random mix
            if random.random() < 0.5: bias_weather_mean = random.uniform(0.3, 0.6)
            else: bias_traffic_mean = random.uniform(2.0, 5.0)
        else:
            episode_type = "CRISIS" # High
            if random.random() < 0.5: bias_weather_mean = random.uniform(0.7, 0.9)
            else: bias_traffic_mean = random.uniform(6.0, 9.0)
        
        print(f"   Type: {episode_type}")

        # 3. Sim Loop
        csv_path = output_dir / f"episode_{episode_idx}.csv"
        header_written = False
        start_date = datetime(2023, 1, 1) # Arbitrary fixed start for consistency
        minutes_per_month = 30 * 24 * 60
        total_months = months_per_episode
        
        for month in range(1, total_months + 1):
            
            # Param Updates
            current_overrides = {}
            target_orders = 5000
            
            # Base Stats from Calibrator + Bias
            if bias_weather_mean > 0:
                current_overrides["weather_condition_severity"] = {"mean": bias_weather_mean, "min": bias_weather_mean * 0.5}
            
            if bias_traffic_mean > 0:
                current_overrides["traffic_congestion_level"] = {"mean": bias_traffic_mean}
            
            # Apply Params
            for col, stats in current_overrides.items():
                if col in calibrator.stats:
                    calibrator.stats[col].update(stats)
            
            # Run Month
            start_t = engine.current_time
            end_t = start_t + minutes_per_month
            
            # Generate Orders
            all_ids = list(gb.nodes.keys())
            for i in range(target_orders):
                ct = random.uniform(start_t, end_t - 300)
                u, v = random.choice(all_ids), random.choice(all_ids)
                if u != v:
                    engine.schedule_event(Event(ct, "SYS", u, EventType.ORDER_CREATED, details={"order_id":f"{i}", "origin":u, "destination":v}))
            
            engine.run(duration=end_t)
            
            # Export
            new_events = [e for e in engine.processed_events if e.time >= start_t and e.time < end_t]
            if new_events:
                df_chunk = DataConverter.events_to_dataframe(new_events, calibrator, engine, gb, start_date=start_date)
                df_chunk["episode_id"] = episode_idx
                
                mode = 'w' if not header_written else 'a'
                header = not header_written
                df_chunk.to_csv(csv_path, mode=mode, header=header, index=False)
                header_written = True
            
            engine.processed_events = []
            
            # Cleanup backlog to prevent OOM/slowdown
            engine.pending_orders = engine.pending_orders[-500:]

        print(f"   ✅ Saved {csv_path}")

    print(f"\n🎉 Generation Complete. Time: {time.time() - start_global:.1f}s")
    print(f"   Output: {output_dir}/episode_*.csv")

if __name__ == "__main__":
    run_episodes(num_episodes=10, months_per_episode=3)
