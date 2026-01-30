import streamlit as st
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import time
import random
from typing import Dict, List, Any
import sys
import os
import math
import base64
import json
from pathlib import Path
import torch
import numpy as np
import pydeck as pdk
import hashlib

# Ensure the project src directory is importable when running via Streamlit.
current_dir = os.path.dirname(os.path.abspath(__file__))
# src/supply_chain/app -> src is 2 levels up
src_dir = str(Path(current_dir).resolve().parents[1]) # parents[0] is supply_chain, parents[1] is src
if src_dir not in sys.path:
    sys.path.append(src_dir)

from supply_chain.simulation.graph import GraphBuilder
from supply_chain.simulation.engine import SimulationEngine
from supply_chain.simulation.schema import Event, EventType, NodeType, TruckStatus
from supply_chain.simulation.visualization import SimulationVisualizer
from supply_chain.simulation.integration import DataConverter, StatsCalibrator
from supply_chain.models.mlp import SupplyChainNet
from supply_chain.models.gnn import SupplyChainGNN
from supply_chain.models.inference import get_current_graph_snapshot
from supply_chain.data.preprocessing import TabularPreprocessor, PreprocessingConfig
from supply_chain.config import DatasetSchema, REPORTS_DIR
from supply_chain.app.components.map import render_pydeck_map

# Page Config
st.set_page_config(
    page_title="Supply Chain Digital Twin & AI Ops",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Styles ---
st.markdown("""
<style>
    .metric-card {
        background-color: #1E1E1E;
        padding: 15px;
        border-radius: 10px;
        border: 1px solid #333;
        text-align: center;
    }
    .metric-value {
        font-size: 24px;
        font-weight: bold;
        color: #4CAF50;
    }
    .metric-label {
        font-size: 14px;
        color: #AAA;
    }
    .risk-high { color: #FF4444; font-weight: bold; }
    .risk-med { color: #FFAA00; font-weight: bold; }
    .risk-low { color: #44FF44; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

# --- Model loading (MLP + GNN) ---
@st.cache_resource
def load_ai_assets():
    """Loads the trained MLP model and fits the preprocessor on historical data."""
    try:
        # src/supply_chain/app/main.py -> project root is 3 levels up
        project_root = Path(__file__).resolve().parents[3]
        raw_dir = project_root / "data" / "raw"
        model_path = project_root / "models" / "supply_chain_mlp.pth"

        candidate_data_paths = [
            raw_dir / "simulated_supply_chain_data_2021_2025.csv",
            raw_dir / "simulated_supply_chain_data.csv",
            raw_dir / "dynamic_supply_chain_logistics_dataset.csv",
        ]
        data_path = next((p for p in candidate_data_paths if p.exists()), None)
        
        if data_path is None or not model_path.exists():
            st.error(
                "Missing assets: Data=%s, Model=%s"
                % (data_path is not None, model_path.exists())
            )
            return None, None
            
        # 1. Fit Preprocessor
        df = pd.read_csv(data_path)
        schema = DatasetSchema()
        pp_config = PreprocessingConfig(schema)
        preprocessor = TabularPreprocessor(pp_config)
        preprocessor.fit(df)
        
        # 2. Load Model
        input_size = len(preprocessor.feature_names_out)
        model = SupplyChainNet(input_size)
        model.load_state_dict(torch.load(model_path, map_location=torch.device('cpu')))
        model.eval()
        
        return model, preprocessor
    except Exception as e:
        st.error(f"Failed to load AI assets: {e}")
        return None, None

@st.cache_resource
def load_gnn_model():
    """Loads the trained GNN model (V2 preferred)."""
    try:
        project_root = Path(__file__).resolve().parents[3]
        
        # Try V2 (Regression) first
        model_path_v2 = project_root / "models" / "supply_chain_gnn_v2.pth"
        scaler_path_v2 = project_root / "models" / "gnn_v2_scaler.json"
        
        is_v2 = False
        model_path = None
        scaler_path = None
        
        if model_path_v2.exists():
            model_path = model_path_v2
            scaler_path = scaler_path_v2
            is_v2 = True
        else:
            model_path = project_root / "models" / "supply_chain_gnn.pth"
            scaler_path = project_root / "models" / "gnn_scaler.json"

        if not model_path.exists():
            return None, None, False
            
        # Initialize Architecture 
        model = SupplyChainGNN(in_channels=18, hidden_channels=64, out_channels=1)
        model.load_state_dict(torch.load(model_path, map_location=torch.device('cpu')))
        model.eval()
        
        # Load Scaler
        scaler = None
        if scaler_path and scaler_path.exists():
            with open(scaler_path, 'r') as f:
                scaler = json.load(f)
        else:
            st.warning("GNN Scaler not found. Inference will be unnormalized.")
            
        return model, scaler, is_v2
    except Exception as e:
        st.error(f"Failed to load GNN Model: {e}")
        return None, None, False

model, preprocessor = load_ai_assets()
gnn_model, gnn_scaler, is_gnn_v2 = load_gnn_model()


# --- Helper Functions ---

def init_simulation():
    """Initialize the simulation engine and graph if not already present."""
    if 'engine' not in st.session_state:
        with st.spinner("Initializing Simulation Model..."):
            # 1. Build Graph
            gb = GraphBuilder()
            
            graph_source = st.session_state.get('graph_source', 'Random')
            
            if graph_source == 'TSPLIB File':
                tsplib_path = st.session_state.get('tsplib_path', 'kroA100.txt')
                if os.path.exists(tsplib_path):
                     gb.create_from_tsplib(Path(tsplib_path), k_neighbors=4)
                elif os.path.exists(os.path.join("..", tsplib_path)):
                     gb.create_from_tsplib(Path(os.path.join("..", tsplib_path)), k_neighbors=4)
                else:
                     st.error(f"File not found: {tsplib_path}. Falling back to random graph.")
                     gb.create_random_graph(num_nodes=15, k_neighbors=3)
            else:
                num_nodes = st.session_state.get('num_nodes', 15)
                gb.create_random_graph(num_nodes=num_nodes, k_neighbors=3)
            
            # 2. Initialize Engine
            engine = SimulationEngine(gb)
            
            # 3. Spawn initial trucks
            valid_spawn_nodes = [
                n.id for n in gb.nodes.values() 
                if n.type not in [NodeType.CUSTOMER, NodeType.INSPECTION]
            ]
            if not valid_spawn_nodes: valid_spawn_nodes = list(gb.nodes.keys())
            
            num_trucks = st.session_state.get('num_trucks', 10)
            for i in range(num_trucks):
                start_node = random.choice(valid_spawn_nodes)
                engine.schedule_event(Event(
                    time=0.0,
                    truck_id=f"T{i+1}",
                    node_id=start_node,
                    event_type=EventType.TRUCK_SPAWN
                ))
                
            # 4. Generate Random Orders
            all_node_ids = list(gb.nodes.keys())
            for i in range(20):
                creation_time = random.uniform(0, 600.0)
                origin = random.choice(all_node_ids)
                destination = random.choice(all_node_ids)
                while destination == origin:
                    destination = random.choice(all_node_ids)
                    
                engine.schedule_event(Event(
                    creation_time,
                    truck_id="SYSTEM",
                    node_id=origin,
                    event_type=EventType.ORDER_CREATED,
                    details={"order_id": f"ORD{i+1}", "origin": origin, "destination": destination}
                ))
            
            st.session_state.engine = engine
            st.session_state.graph_builder = gb
            st.session_state.simulation_time = 0.0
            st.session_state.running = False
            
            # Initialize simple calibrator for inference context
            st.session_state.calibrator = StatsCalibrator() # Uses default
            
            # Reset GNN History on simulation reset
            if 'gnn_history' in st.session_state:
                del st.session_state.gnn_history

def get_sim_time_string():
    """Convert simulation minutes to date string"""
    # Use Today at 8:00 as start
    base_date = pd.Timestamp.now().normalize() + pd.Timedelta(hours=8)
    
    # Prefer Engine time if available and more recent
    current_mins = 0.0
    if 'engine' in st.session_state:
         current_mins = st.session_state.engine.current_time
    elif 'simulation_time' in st.session_state:
         current_mins = st.session_state.simulation_time
        
    current_date = base_date + pd.Timedelta(minutes=current_mins)
    return current_date.strftime("%Y-%m-%d %H:%M")

def reset_simulation():
    """Force re-initialization of the simulation (e.g., when graph changes)."""
    keys = ['engine', 'graph_builder', 'simulation_time', 'running', 'gnn_history', 'calibrator', 'risk_ema']
    for k in keys:
        if k in st.session_state:
            del st.session_state[k]
    init_simulation()

# Initialize Simulation State
init_simulation()
engine = st.session_state.engine
graph_builder = st.session_state.graph_builder # Use full name for clarity
gb = graph_builder

# --- Sidebar ---
st.sidebar.header("Configuration")

with st.sidebar.expander("🗺️ Map Legend", expanded=False):
    st.markdown("**Truck Status:**")
    st.markdown("⚪ **Idle** (Grey)")
    st.markdown("🔵 **To Pickup** (Blue)")
    st.markdown("🟢 **To Delivery** (Green)")
    st.markdown("🔴 **Resting** (Red)")
    
    st.markdown("**Locations:**")
    st.markdown("🟣 **Warehouse** (Purple)")
    st.markdown("🟡 **Customer** (Gold)")
    st.markdown("🟠 **Hub** (Orange)")
    st.markdown("🔵 **Port** (Cyan)")
    st.markdown("🛑 **Inspection** (Pink)")
    
    st.markdown("**AI Layers:**")
    st.markdown("🔴 **Spatial Risk** (Dynamic Red Circles)")


graph_source = st.sidebar.radio("Graph Source", ["Random", "TSPLIB File"], index=0)
st.session_state.graph_source = graph_source

if st.session_state.graph_source == "Random":
    st.session_state.num_nodes = st.sidebar.slider("Number of Nodes", 10, 50, 15)
else:
    st.session_state.tsplib_path = st.sidebar.text_input("TSPLIB File Path", "kroA100.txt")

if "last_graph_source" not in st.session_state:
    st.session_state.last_graph_source = graph_source

if st.session_state.last_graph_source != graph_source:
    st.session_state.last_graph_source = graph_source
    if "view_state" in st.session_state:
        del st.session_state.view_state
    reset_simulation()

st.session_state.num_trucks = st.sidebar.slider("Number of Trucks", 5, 30, 20)
sim_speed = st.sidebar.slider("Simulation Speed (steps/frame)", 1, 10, 2)

show_gnn_risk = st.sidebar.checkbox("👁️ Show AI Spatial Risk (GNN)", value=True)

# --- GNN Debug Tools ---
with st.sidebar.expander("🛠️ GNN Debug / Interpretation", expanded=False):
    st.markdown("Run sanity checks on the model.")
    if st.button("Run Permutation Test"):
        if gnn_model and 'engine' in st.session_state:
            with st.spinner("Running Permutation Test..."):
                snap = get_current_graph_snapshot(
                    st.session_state.engine, 
                    st.session_state.graph_builder,
                    node_overrides=st.session_state.get('node_overrides', {})
                )
                
                # 1. Normal Prediction
                # We need history... for test let's fake it with duplicates (Cold Start)
                x3 = torch.cat([snap.x]*3, dim=1)
                e_attr = snap.edge_attr
                
                # Normalize using scaler if available
                if gnn_scaler:
                     xm = torch.tensor(gnn_scaler["x_mean"])
                     xs = torch.tensor(gnn_scaler["x_std"])
                     x3_norm = (x3 - xm) / xs
                     
                     em = torch.tensor(gnn_scaler["edge_mean"])
                     es = torch.tensor(gnn_scaler["edge_std"])
                     e_norm = (e_attr - em) / es
                else:
                     x3_norm = x3
                     e_norm = e_attr
                
                with torch.no_grad():
                    logits_orig = gnn_model(x3_norm, snap.edge_index, e_norm)
                    probs_orig = torch.sigmoid(logits_orig)
                    
                # 2. Permuted Features (Scramble rows of X)
                idx = torch.randperm(x3_norm.size(0))
                x3_perm = x3_norm[idx]
                
                with torch.no_grad():
                     logits_perm = gnn_model(x3_perm, snap.edge_index, e_norm)
                     probs_perm = torch.sigmoid(logits_perm)
                     
                # 3. Ablated Edges (Zero attributes)
                e_zero = torch.zeros_like(e_norm)
                with torch.no_grad():
                     logits_abl = gnn_model(x3_norm, snap.edge_index, e_zero)
                     probs_abl = torch.sigmoid(logits_abl)
                     
                st.write("**Original Mean Risk:**", f"{probs_orig.mean().item():.4f}")
                
                mae_perm = (probs_orig - probs_perm).abs().mean().item()
                st.write("**Permutation MAE:**", f"{mae_perm:.4f}")
                if mae_perm < 0.05:
                     st.error("⚠️ Model insensitive to feature order! (Possible Over-smoothing or Bug)")
                else:
                     st.success("✅ Model sensitive to features.")
                     
                mae_abl = (probs_orig - probs_abl).abs().mean().item()
                st.write("**Edge Ablation MAE:**", f"{mae_abl:.4f}")
                if mae_abl < 0.01:
                     st.warning("⚠️ Edge Attributes used minimally.")
                else:
                     st.info(f"ℹ️ Edge Attrs contribute {mae_abl:.4f} to output.")
        else:
            st.error("Model or Engine not ready.")

# --- Sabotage Panel ---
st.sidebar.markdown("---")
st.sidebar.subheader("🔥 Chaos / Sabotage")
if 'node_overrides' not in st.session_state:
    st.session_state.node_overrides = {}

if 'graph_builder' in st.session_state and st.session_state.graph_builder:
    sabotage_node = st.sidebar.selectbox("Select Node to Sabotage", list(st.session_state.graph_builder.nodes.keys()))
    if sabotage_node:
        col_sab1, col_sab2 = st.sidebar.columns(2)
        # Default to high values for immediate demo effect
        s_traffic = col_sab1.slider("Traffic", 0.0, 1.0, 0.9, key="sab_traffic")
        s_weather = col_sab2.slider("Weather", 0.0, 1.0, 0.8, key="sab_weather")
        
        if st.sidebar.button("💥 Apply Chaos"):
            st.session_state.node_overrides[sabotage_node] = {
                "traffic": s_traffic,
                "weather": s_weather
            }
            st.sidebar.success(f"Sabotaged {sabotage_node}!")

        if st.sidebar.button("🧹 Clear Chaos"):
            st.session_state.node_overrides = {}
            st.sidebar.info("All chaos cleared.")
else:
    st.sidebar.info("Start simulation to enable sabotage.")

# --- Auto Chaos Mode ---
auto_chaos = st.sidebar.checkbox("🌪️ Enable Dynamic Chaos Mode", value=False, help="Automatically sabotages random nodes periodically.")
test_mode = st.sidebar.checkbox("🧪 Test Mode: Ideal Background", value=True, help="Suppresses random traffic/weather on non-sabotaged nodes to isolate sabotage signal. RECOMMENDED FOR DEMO.")

if auto_chaos:
    targets = list(st.session_state.get('node_overrides', {}).keys())
    if targets:
        st.sidebar.markdown(f"**🔥 Active Targets:** {', '.join(targets)}")
    else:
        st.sidebar.markdown("*(No active chaos yet)*")

if st.sidebar.button("Reset Simulation"):
    reset_simulation()
    if "view_state" in st.session_state:
        del st.session_state.view_state

# Chaos Logic Helper
def manage_dynamic_chaos(engine, graph_builder):
    if not auto_chaos: return
    
    # Probability to start new chaos
    if random.random() < 0.05: # 5% chance per frame
        # Pick random node
        nodes = list(graph_builder.nodes.keys())
        target = random.choice(nodes)
        
        # Apply Sabotage
        st.session_state.node_overrides[target] = {
            "traffic": random.uniform(0.7, 1.0), # High traffic
            "weather": random.uniform(0.4, 0.9)
        }
        
    # Probability to clear chaos
    # We iterate copy to allow modification
    current_targets = list(st.session_state.node_overrides.keys())
    for target in current_targets:
        if random.random() < 0.02: # 2% chance to clear existing chaos
             del st.session_state.node_overrides[target]



# --- Main Page ---
st.title("Supply Chain Digital Twin & AI Ops")

tabs = st.tabs(["🔴 Live Operation", "🔮 AI Predictions (UC1/UC2)", "🚧 Risk Analysis (UC3)"])

with tabs[0]:
    st.markdown("Real-time simulation of logistics network.")
    
    # --- Clock Fragment ---
    @st.fragment(run_every=1)
    def clock_display():
        sim_time_str = get_sim_time_string()
        st.markdown(f"### 🕒 {sim_time_str}")
        st.caption(f"Simulation Clock (Start: Today 08:00)")
        
    with st.sidebar:
        clock_display()
    
    st.sidebar.slider("UI refresh (seconds)", 0.1, 2.0, 0.2, key="ui_refresh")
    col_ctrl1, col_ctrl2 = st.columns([1, 5])
    
    def start(): st.session_state.running = True
    def pause(): st.session_state.running = False
    
    col_ctrl1.button("Start / Resume", type="primary", on_click=start, disabled=st.session_state.running)
    col_ctrl1.button("Pause", on_click=pause, disabled=not st.session_state.running)
    
    run_every = st.session_state.ui_refresh if st.session_state.running else None
    
    @st.fragment(run_every=run_every)
    def live_panel():
        engine = st.session_state.engine
        
        if st.session_state.running:
            for _ in range(sim_speed):
                if engine.event_queue:
                    engine.step()
            
            # Sync session state time for other components
            st.session_state.simulation_time = engine.current_time
            
            # Dynamic Chaos Update
            if auto_chaos:
                manage_dynamic_chaos(engine, gb)

            # Infinite Simulation Logic: Respawn orders if running low
            # Count active orders (Assigned/Created but not Completed)
            active_orders = sum(1 for o in engine.orders.values() if o.status not in ["COMPLETED", "CANCELLED"])
            
            # Dynamic Threshold: Ensure we always have more orders than trucks
            target_orders = st.session_state.get('num_trucks', 20) + 15
            
            if active_orders < target_orders: 
                # Spawn new batch
                all_ids = list(gb.nodes.keys())
                for _ in range(10): # Spawn larger batch
                    origin = random.choice(all_ids)
                    dest = random.choice(all_ids)
                    while dest == origin: dest = random.choice(all_ids)
                    
                    new_oid = f"ORD_{int(engine.current_time)}_{random.randint(100,999)}"
                    engine.schedule_event(Event(
                        engine.current_time + random.uniform(0, 10), # Slight delay
                        "SYSTEM", origin, EventType.ORDER_CREATED,
                        details={"order_id": new_oid, "origin": origin, "destination": dest}
                    ))
                    
        # Metrics
        completed = [o for o in engine.orders.values() if o.status == "COMPLETED"]
        cancelled = [o for o in engine.orders.values() if o.status == "CANCELLED"]
        in_progress = [o for o in engine.orders.values() if o.status == "ASSIGNED"]
        pending_count = len(engine.pending_orders)
        active_trucks = sum(1 for t in engine.trucks.values() if t.status != TruckStatus.IDLE)
        
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Delivered", len(completed))
        c2.metric("In Progress", len(in_progress))
        c3.metric("Pending", pending_count)
        c4.metric("Cancelled", len(cancelled))
        c5.metric("Active Trucks", f"{active_trucks}/{len(engine.trucks)}")
        
        deck = render_pydeck_map(
            engine, 
            gb, 
            gnn_model=gnn_model, 
            gnn_scaler=gnn_scaler, 
            is_gnn_v2=is_gnn_v2, 
            show_gnn_risk=show_gnn_risk, 
            test_mode=test_mode,
            node_overrides=st.session_state.get('node_overrides', {})
        )
        if "view_state" in st.session_state:
            deck.initial_view_state = st.session_state.view_state
        else:
            st.session_state.view_state = deck.initial_view_state
            
        st.pydeck_chart(deck, use_container_width=True, key="sim_map")
    
    live_panel()

# --- AI Predictions Tab ---
with tabs[1]:
    st.header("🔮 AI Prediction Center")
    if model is None:
        st.warning("⚠️ AI Model not loaded. Run 'run_chaos.py' first to generate model.")
    else:
        c1, c2 = st.columns([1, 1])
        
        with c1:
            st.subheader("UC1: Delivery Delay Prediction")
            st.markdown("Select an active truck to predict if its delivery will be successful.")
            
            # Filter active trucks with orders
            active_trucks = [t for t in engine.trucks.values() if t.assigned_order_id]
            truck_options = {t.id: f"{t.id} (Order: {t.assigned_order_id})" for t in active_trucks}
            
            if not active_trucks:
                st.info("No active trucks with orders currently.")
            else:
                selected_truck_id = st.selectbox("Select Truck", list(truck_options.keys()), format_func=lambda x: truck_options[x])
                
                if st.button("Predict Outcome"):
                    truck = engine.trucks[selected_truck_id]
                    order = engine.orders[truck.assigned_order_id]
                    
                    # Construct feature vector dynamically
                    # We spoof an 'ARRIVAL' event at current time to generate features
                    dummy_event = Event(engine.current_time, truck.id, truck.current_node_id, EventType.ARRIVAL_NODE)
                    row_dict = DataConverter._create_row(
                        dummy_event, st.session_state.calibrator, engine, gb, 
                        start_date=pd.Timestamp.now(), include_context=False
                    )
                    
                    # Create DF and Preprocess
                    df_single = pd.DataFrame([row_dict])
                    try:
                        features = preprocessor.transform(df_single)
                        features_tensor = torch.tensor(features, dtype=torch.float32)
                        
                        with torch.no_grad():
                            prob = model(features_tensor).item()
                            
                        st.metric("Probability of On-Time Arrival", f"{prob*100:.1f}%")
                        
                        # Calculate ETA Heuristic
                        try:
                            # 1. Start Time of current order
                            start_time = order.creation_time
                            # 2. Distance remaining? 
                            # Simplification: Distance from current node to dest
                            if truck.route and truck.current_node_index < len(truck.route):
                                # Just calculate straight line distance + small buffer for remaining legs
                                current_n = gb.nodes[truck.current_node_id]
                                dest_n = gb.nodes[order.destination_node_id]
                                from supply_chain.simulation.graph import haversine_distance
                                dist_km = haversine_distance(current_n.lat, current_n.lon, dest_n.lat, dest_n.lon)
                                avg_speed = 60.0 # km/h
                                est_hours = dist_km / avg_speed
                                eta_sim_time = engine.current_time + (est_hours * 60) # minutes
                                
                                st.metric("Estimated Arrival Duration", f"{int(est_hours)}h {int((est_hours%1)*60)}m")
                        except Exception as calc_err:
                            st.info(f"ETA Calc unavailable: {calc_err}")

                        if prob > 0.8:
                            st.success(f"✅ Likely to arrive within ~{int(est_hours)}h.")
                        elif prob > 0.5:
                            st.warning("⚠️ Risk of missing the target window.")
                        else:
                            st.error("🚨 High Risk: Likely to be late!")
                            
                    except Exception as e:
                        st.error(f"Prediction error: {e}")
                        
        with c2:
            st.subheader("UC2: Smart Inventory Recommendations")
            st.markdown("AI analysis of historical demand vs current stock.")
            
            warehouses = [n for n in gb.nodes.values() if n.type == NodeType.WAREHOUSE]
            if not warehouses:
                st.info("No warehouses in current graph.")
            else:
                wh_data = []
                for wh in warehouses:
                    # Mocking inventory data for demo (Digital Twin state)
                    current_stock = 1000 - wh.busy_count * 50 # Heuristic
                    predicted_demand = 1200 # Placeholder for time-series forecast
                    
                    status = "OK"
                    if current_stock < 200: status = "CRITICAL LOW"
                    elif current_stock < 500: status = "ORDER SOON"
                    
                    wh_data.append({
                        "Warehouse ID": wh.id,
                        "Current Stock": current_stock,
                        "Predicted Demand (7d)": predicted_demand,
                        "Status": status
                    })
                    
                st.dataframe(pd.DataFrame(wh_data), hide_index=True)

# --- Risk Analysis Tab ---
with tabs[2]:
    st.header("UC3: Logistics Risk Visualization")
    
    st.markdown("### Risk Heatmap")
    st.info("Visualizing high-risk zones based on MLP Chaos Model (Predicted Delay Probability).")
    
    if model and preprocessor:
        heatmap_data = []
        
        # Iterate over all nodes to probe "Risk" using the MLP
        # We simulate a "virtual truck" arriving at each node
        for node_id in gb.nodes:
            node = gb.nodes[node_id]
            
            # 1. Create Dummy Event for feature extraction
            # We use "SYSTEM" as truck_id, and current node as origin
            dummy_event = Event(engine.current_time, "SYSTEM", node_id, EventType.ARRIVAL_NODE)
            
            # 2. Extract Features (reuses logic from UC1, including Sabotage overrides!)
            try:
                row_dict = DataConverter._create_row(
                    dummy_event, st.session_state.calibrator, engine, gb, 
                    start_date=pd.Timestamp.now(), include_context=False
                )
                
                # 3. Preprocess & Predict
                df_single = pd.DataFrame([row_dict])
                features = preprocessor.transform(df_single)
                features_tensor = torch.tensor(features, dtype=torch.float32)
                
                with torch.no_grad():
                    prob = model(features_tensor).item()
                
                heatmap_data.append({
                    "lat": node.lat,
                    "lon": node.lon,
                    "weight": prob # 0.0 to 1.0
                })
            except Exception as e:
                # Fallback or skip if feature extraction fails
                pass
            
        df_heat = pd.DataFrame(heatmap_data)
        
        deck_heat = pdk.Deck(
            layers=[
                 pdk.Layer(
                    "HeatmapLayer",
                    data=df_heat,
                    get_position=["lon", "lat"],
                    get_weight="weight",
                    radius_pixels=60,
                ),
            pdk.Layer(
                "ScatterplotLayer",
                data=df_heat,
                get_position=["lon", "lat"],
                get_radius=1000,
                get_color=[255, 255, 255],
                opacity=0.3
            )
        ],
        initial_view_state=pdk.ViewState(
            latitude=df_heat["lat"].mean(),
            longitude=df_heat["lon"].mean(),
            zoom=6
        ),
        map_style="mapbox://styles/mapbox/dark-v10"
    )
    
    st.pydeck_chart(deck_heat)
    
    st.markdown("### Key Risk Indicators")
    c1, c2 = st.columns(2)
    c1.metric("Avg Traffic Level", f"{df_heat['weight'].mean()*10:.1f}/10")
    c2.metric("Network Disruption Risk", "Medium", delta_color="off")
