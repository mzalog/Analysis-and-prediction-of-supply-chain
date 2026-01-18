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
from pathlib import Path
import torch
import numpy as np
import pydeck as pdk

# Add src to path so we can import supply_chain package
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(current_dir)
if src_dir not in sys.path:
    sys.path.append(src_dir)

from supply_chain.simulation.graph import GraphBuilder
from supply_chain.simulation.engine import SimulationEngine
from supply_chain.simulation.schema import Event, EventType, NodeType, TruckStatus
from supply_chain.simulation.visualization import SimulationVisualizer
from supply_chain.simulation.integration import DataConverter, StatsCalibrator
from supply_chain.model.network import SupplyChainNet
from supply_chain.data.preprocessing import TabularPreprocessor, PreprocessingConfig
from supply_chain.config import DatasetSchema, REPORTS_DIR

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

# --- AI Loading ---
@st.cache_resource
def load_ai_assets():
    """Loads the trained MLP model and fits the preprocessor on historical data."""
    try:
        experiment_dir = Path(REPORTS_DIR) / "experiments" / "experiment_chaos"
        data_path = experiment_dir / "simulated_data.csv"
        model_path = experiment_dir / "model.pth"
        
        if not data_path.exists() or not model_path.exists():
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
    """Loads the trained GNN model."""
    try:
        from supply_chain.gnn.model import SupplyChainGNN
        model_path = Path(REPORTS_DIR) / "experiments" / "gnn_model" / "gnn_model.pth"
        
        if not model_path.exists():
            return None
            
        # Initialize Architecture (Standard params from train.py)
        model = SupplyChainGNN(in_channels=5, hidden_channels=64, out_channels=1)
        model.load_state_dict(torch.load(model_path, map_location=torch.device('cpu')))
        model.eval()
        return model
    except Exception as e:
        st.error(f"Failed to load GNN Model: {e}")
        return None

model, preprocessor = load_ai_assets()
gnn_model = load_gnn_model()


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

def reset_simulation():
    if 'engine' in st.session_state:
        del st.session_state.engine
    init_simulation()

def get_current_graph_snapshot(engine, graph_builder):
    """
    Extracts current simulation state into a PyG Data object for GNN inference.
    Fast extraction without pandas overhead.
    """
    from torch_geometric.data import Data
    
    graph = graph_builder.graph
    node_mapping = {n: i for i, n in enumerate(graph.nodes())}
    num_nodes = len(graph.nodes)
    
    # 1. Edge Index
    src, dst = [], []
    for u, v in graph.edges():
        if u in node_mapping and v in node_mapping:
            src.append(node_mapping[u])
            dst.append(node_mapping[v])
            src.append(node_mapping[v]) # Undirected for message passing
            dst.append(node_mapping[u])
    edge_index = torch.tensor([src, dst], dtype=torch.long)
    
    # 2. Node Features [Num_Nodes, 5]
    # Features: [Type, Load, Traffic, Weather, Delay]
    x = torch.zeros((num_nodes, 5), dtype=torch.float)
    
    # Traffic/Weather from calibrator context (or simplified global state)
    # Ideally, we should pull from specific node state if engine tracks it physically.
    # For now, we simulate "local" conditions based on the calibrator or engine internals.
    
    for node_id, idx in node_mapping.items():
        node_data = graph.nodes[node_id]['data']
        
        # Feature 0: Type
        type_val = 0
        if node_data.type == NodeType.WAREHOUSE: type_val = 1
        elif node_data.type == NodeType.HUB: type_val = 2
        elif node_data.type == NodeType.PORT: type_val = 3
        elif node_data.type == NodeType.CUSTOMER: type_val = 4
        x[idx, 0] = type_val
        
        # Feature 1: Load (Pending Orders at this node)
        load = sum(1 for o in engine.orders.values() if o.origin_node_id == node_id and o.status == "ASSIGNED")
        x[idx, 1] = load
        
        # Feature 2 & 3: Context (Resampled for demo "live" volatility)
        if hasattr(st.session_state, 'calibrator'):
             x[idx, 2] = st.session_state.calibrator.sample("traffic_congestion_level")
             x[idx, 3] = st.session_state.calibrator.sample("weather_condition_severity")
        
        # Feature 4: Current Delays (Avg delay of recent arrivals at this node)
        x[idx, 4] = 0.0 # Placeholder for live simulation
        
    return Data(x=x, edge_index=edge_index)


def render_pydeck_map(engine, graph_builder, show_gnn_risk=False):
    """Render the graph using PyDeck."""
    graph = graph_builder.graph

    # Load Icon Atlas (Served via Streamlit Static Files if configured, else default)
    # Using simple color coding for now as fallback
    
    # --- Prepare Data for Layers ---
    
    # 1. Nodes Data
    nodes_data = []
    # Styles mapping to RGB colors (Distinct from Truck Status colors)
    styles = {
        NodeType.WAREHOUSE: [138, 43, 226],    # BlueViolet
        NodeType.CUSTOMER: [255, 215, 0],      # Gold
        NodeType.HUB: [255, 140, 0],           # DarkOrange 
        NodeType.PORT: [0, 255, 255],          # Cyan
        NodeType.INSPECTION: [255, 105, 180]   # HotPink
    }
    
    for node_id in graph.nodes():
        node = graph.nodes[node_id]['data']
        color = styles.get(node.type, [128, 128, 128])
        
        pending_count = 0
        for oid in engine.pending_orders:
            if engine.orders[oid].origin_node_id == node_id:
                pending_count += 1
                
        nodes_data.append({
            "id": node_id,
            "type": node.type.value if hasattr(node.type, "value") else str(node.type),
            "lon": node.lon,
            "lat": node.lat,
            "color": color,
            "radius": 5000 if node.type in [NodeType.HUB, NodeType.PORT] else 3000,
            "pending": str(pending_count) if pending_count > 0 else "",
        })
        
    # 2. Edges Data
    edges_data = []
    for u, v, data in graph.edges(data=True):
        u_node = graph.nodes[u]['data']
        v_node = graph.nodes[v]['data']
        edges_data.append({
            "source": [u_node.lon, u_node.lat],
            "target": [v_node.lon, v_node.lat],
            "color": [100, 100, 100, 100] 
        })

    # 3. Trucks Data
    trucks_data = []
    status_colors = {
        TruckStatus.IDLE: [128, 128, 128],
        TruckStatus.EN_ROUTE_TO_PICKUP: [0, 0, 255],
        TruckStatus.EN_ROUTE_TO_DELIVERY: [0, 255, 0],
        TruckStatus.RESTING: [255, 0, 0]
    }
    
    for truck in engine.trucks.values():
        lon, lat = 0.0, 0.0
        
        if truck.current_node_id in graph.nodes:
            start_node = graph.nodes[truck.current_node_id]['data']
            lon, lat = start_node.lon, start_node.lat
            
            if truck.current_leg_duration > 0 and truck.route and truck.current_node_index < len(truck.route):
                end_node_id = truck.route[truck.current_node_index]
                if end_node_id in graph.nodes:
                    end_node = graph.nodes[end_node_id]['data']
                    elapsed = engine.current_time - truck.current_leg_start_time
                    t = elapsed / truck.current_leg_duration
                    t = max(0.0, min(1.0, t))
                    lon = start_node.lon + (end_node.lon - start_node.lon) * t
                    lat = start_node.lat + (end_node.lat - start_node.lat) * t
        
        offset_seed = int(truck.id[1:]) if truck.id[1:].isdigit() else hash(truck.id)
        lon_offset = (offset_seed % 5 - 2) * 0.005
        lat_offset = (offset_seed % 7 - 3) * 0.005
        lon += lon_offset
        lat += lat_offset
        
        trucks_data.append({
            "id": truck.id,
            "lon": lon,
            "lat": lat,
            "color": status_colors.get(truck.status, [255, 255, 255]),
            "status": truck.status.value,
            "icon": "🚚"
        })

    # --- Layers ---
    layers = [
        pdk.Layer(
            "LineLayer",
            edges_data,
            get_source_position="source",
            get_target_position="target",
            get_color="color",
            get_width=2,
            pickable=False,
        ),
        pdk.Layer(
            "ScatterplotLayer",
            nodes_data,
            get_position=["lon", "lat"],
            get_color="color",
            get_radius="radius",
            pickable=True,
            auto_highlight=True,
            opacity=0.8,
            radius_min_pixels=8,  
            radius_max_pixels=20,
        ),
        pdk.Layer(
            "TextLayer",
            nodes_data,
            get_position=["lon", "lat"],
            get_text="type",
            get_color=[200, 200, 200],
            get_size=12,
            get_pixel_offset=[0, 20]
        ),
        pdk.Layer(
            "TextLayer",
            nodes_data,
            get_position=["lon", "lat"],
            get_text="pending",
            get_color=[255, 255, 255],
            get_size=14,
            get_background_color=[255, 0, 0],
            font_weight="bold",
            get_pixel_offset=[15, -15]
        ),
        pdk.Layer(
            "ScatterplotLayer",
            trucks_data,
            get_position=["lon", "lat"],
            get_color="color",
            get_radius=4000, 
            pickable=True,
            opacity=0.5,
        ),
        pdk.Layer(
            "TextLayer",
            trucks_data,
            get_position=["lon", "lat"],
            get_text="icon",
            get_size=25,
            pickable=True,
        ),
        pdk.Layer(
            "TextLayer",
            trucks_data,
            get_position=["lon", "lat"],
            get_text="id",
            get_color=[255, 255, 255],
            get_size=10,
            get_pixel_offset=[0, 18]
        )
    ]
    
    # --- GNN Visualization Layer ---
    if show_gnn_risk:
        if gnn_model:
            # Inference
            snapshot = get_current_graph_snapshot(engine, graph_builder)
            with torch.no_grad():
                risk_scores = gnn_model(snapshot.x, snapshot.edge_index)
            
            gnn_data = []
            max_risk = risk_scores.max().item()
            for i, score in enumerate(risk_scores):
                node_id = list(snapshot.x.size())[0] # Helper only, mapping needed
                # Reconstruct mapping (Assuming order is preserved from graph.nodes())
                node_id = list(graph.nodes())[i]
                node = graph.nodes[node_id]['data']
                
                risk_val = score.item()
                # Color Gradient: Green (0) -> Red (1)
                r = int(255 * risk_val)
                g = int(255 * (1 - risk_val))
                
                gnn_data.append({
                    "lon": node.lon,
                    "lat": node.lat,
                    "radius": 6000 * risk_val, # Bigger radius = Higher Risk
                    "color": [r, g, 0, 150], # Semi-transparent
                    "risk": f"{risk_val:.2f}"
                })
                
            layers.append(pdk.Layer(
                "ScatterplotLayer",
                gnn_data,
                get_position=["lon", "lat"],
                get_fill_color="color",
                get_radius="radius",
                pickable=True,
                opacity=0.6,
                stroked=True,
                filled=True,
                radius_min_pixels=5,
                radius_max_pixels=40
            ))
        else:
            # Cannot show warning easily in map renderer, avoiding side effects
            pass

    lats = [n['lat'] for n in nodes_data]
    lons = [n['lon'] for n in nodes_data]
    center_lat = sum(lats) / len(lats) if lats else 50.0
    center_lon = sum(lons) / len(lons) if lons else 19.0

    if lats and lons:
        min_lat, max_lat = min(lats), max(lats)
        min_lon, max_lon = min(lons), max(lons)
        lat_span = max(max_lat - min_lat, 0.1)
        lon_span = max(max_lon - min_lon, 0.1)
        max_span = max(lat_span, lon_span)
        zoom = 9.5 - math.log2(max_span)
    else:
        zoom = 6

    view_state = pdk.ViewState(
        latitude=center_lat,
        longitude=center_lon,
        zoom=zoom,
        pitch=0,
    )

    return pdk.Deck(
        layers=layers,
        initial_view_state=view_state,
        tooltip={"text": "{id}\n{type}\n{status}\nRisk: {risk}"}, # Added Risk to tooltip
        map_style="mapbox://styles/mapbox/dark-v10"
    )

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
show_gnn_risk = st.sidebar.checkbox("👁️ Show AI Spatial Risk (GNN)", value=False)

if st.sidebar.button("Reset Simulation"):
    reset_simulation()
    if "view_state" in st.session_state:
        del st.session_state.view_state

# Initialize
init_simulation()
engine = st.session_state.engine
gb = st.session_state.graph_builder

# --- Main Page ---
st.title("Supply Chain Digital Twin & AI Ops")

tabs = st.tabs(["🔴 Live Operation", "🔮 AI Predictions (UC1/UC2)", "🚧 Risk Analysis (UC3)"])

with tabs[0]:
    st.markdown("Real-time simulation of logistics network.")
    
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
                    
        # Metrics
        completed = [o for o in engine.orders.values() if o.status == "COMPLETED"]
        cancelled = [o for o in engine.orders.values() if o.status == "CANCELLED"]
        in_progress = [o for o in engine.orders.values() if o.status == "ASSIGNED"]
        active_trucks = sum(1 for t in engine.trucks.values() if t.status != TruckStatus.IDLE)
        
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Delivered", len(completed))
        c2.metric("In Progress", len(in_progress))
        c3.metric("Cancelled", len(cancelled))
        c4.metric("Active Trucks", f"{active_trucks}/{len(engine.trucks)}")
        
        deck = render_pydeck_map(engine, gb, show_gnn_risk)
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
    st.info("Visualizing high-risk zones based on Traffic Congestion and Weather Severity.")
    
    # Generate Heatmap Data
    heatmap_data = []
    
    # We'll use node locations, and add "weight" based on simulated risk factors
    # For demo, we resample environment factors
    for node_id in gb.nodes:
        node = gb.get_node(node_id)
        # Randomly sample risk factors to simulate "current" environmental state
        traffic = st.session_state.calibrator.sample("traffic_congestion_level")
        weather = st.session_state.calibrator.sample("weather_condition_severity")
        risk_score = (traffic / 10.0 + weather) / 2.0
        
        heatmap_data.append({
            "lat": node.lat,
            "lon": node.lon,
            "weight": risk_score
        })
        
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
