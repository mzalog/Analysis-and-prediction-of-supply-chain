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
        # Data still in experiments folder for fitting preprocessor
        # Data still in experiments folder for fitting preprocessor
        project_root = Path(__file__).parent.parent.parent
        data_path = project_root / "data" / "raw" / "simulated_supply_chain_data_2021_2025.csv"
        
        # Model moved to models/
        # Adjust path relative to project root or use absolute logic if needed
        # Assuming app.py is run from project root or src is in path
        project_root = Path(__file__).parent.parent.parent
        model_path = project_root / "models" / "supply_chain_mlp.pth"
        
        if not data_path.exists() or not model_path.exists():
            st.error(f"Missing assets: Data={data_path.exists()}, Model={model_path.exists()}")
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
        
        project_root = Path(__file__).parent.parent.parent
        model_path = project_root / "models" / "supply_chain_gnn.pth"
        
        if not model_path.exists():
            return None
            
        # Initialize Architecture (Standard params from train.py)
        # ST-GNN now expects 15 channels (3 steps * 5 features)
        model = SupplyChainGNN(in_channels=15, hidden_channels=64, out_channels=1)
        model.load_state_dict(torch.load(model_path, map_location=torch.device('cpu')))
        model.eval()
        
        # Load Scaler
        scaler_path = project_root / "models" / "gnn_scaler.json"
        scaler = None
        if scaler_path.exists():
            with open(scaler_path, 'r') as f:
                scaler = json.load(f)
        else:
            st.warning("GNN Scaler not found. Inference will be unnormalized (likely poor).")
            
        return model, scaler
    except Exception as e:
        st.error(f"Failed to load GNN Model: {e}")
        return None, None

model, preprocessor = load_ai_assets()
gnn_model, gnn_scaler = load_gnn_model()


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

    init_simulation()
    # Reset GNN History on simulation reset
    if 'gnn_history' in st.session_state:
        del st.session_state.gnn_history

def compute_edge_attrs(graph, node_mapping, current_time_min):
    """
    Computes dynamic edge attributes [Dist, Traffic, Weather] for GNN.
    Matches logic in dataset.py.
    """
    edge_attrs = []
    # Iterate exactly as edge_index is built (source-major, then undirected dup)
    # But wait, edge_index must align with this list. 
    # To be safe, we iterate sorted edges or store map? 
    # PyG convention: edge_attr row i corresponds to edge_index col i.
    # We must ensure loop order is IDENTICAL to edge_index construction.
    
    # We will return list of lists, caller converts to tensor.
    
    # Helper noise 
    time_h = current_time_min / 60.0
    def noise(lat, lon, t):
        val = math.sin(lon/5.0 + t/24.0) + math.cos(lat/5.0 + t/48.0)
        return (val + 2.0) / 4.0

    # Must match get_current_graph_snapshot's edge iteration order!
    # It iterates: for u, v in graph.edges(): append(u,v), append(v,u)
    # So we do the same.
    
    for u, v in graph.edges():
        if u in node_mapping and v in node_mapping:
            src_node = graph.nodes[u]['data']
            dst_node = graph.nodes[v]['data']
            
            dist = ((src_node.lat - dst_node.lat)**2 + (src_node.lon - dst_node.lon)**2) ** 0.5
            dist_norm = dist / 10.0
            
            w_src = noise(src_node.lat, src_node.lon, time_h)
            w_dst = noise(dst_node.lat, dst_node.lon, time_h)
            weather = (w_src + w_dst) / 2.0
            
            t_src = noise(src_node.lat+10, src_node.lon+10, time_h)
            t_dst = noise(dst_node.lat+10, dst_node.lon+10, time_h)
            traffic = (t_src + t_dst) / 2.0
            
            # Forward Edge
            edge_attrs.append([dist_norm, traffic, weather])
            # Backward Edge
            edge_attrs.append([dist_norm, traffic, weather])
            
    return torch.tensor(edge_attrs, dtype=torch.float)

def get_current_graph_snapshot(engine, graph_builder):
    """
    Extracts current simulation state into a PyG Data object for GNN inference.
    Uses REAL simulation state (queues, history) and DETERMINISTIC environment factors.
    """
    from torch_geometric.data import Data
    
    
    graph = graph_builder.graph
    # CRITICAL: Sort nodes by ID to ensure alignment across partial snapshots!
    sorted_nodes = sorted(graph.nodes())
    node_mapping = {n: i for i, n in enumerate(sorted_nodes)}
    num_nodes = len(sorted_nodes)
    
    # 1. Edge Index
    src, dst = [], []
    for u, v in graph.edges():
        if u in node_mapping and v in node_mapping:
            src.append(node_mapping[u])
            dst.append(node_mapping[v])
            src.append(node_mapping[v]) # Undirected for message passing
            dst.append(node_mapping[u])
    edge_index = torch.tensor([src, dst], dtype=torch.long)
    
    # 1.5 Edge Attrs
    edge_attr = compute_edge_attrs(graph, node_mapping, engine.current_time)
    
    # 2. Node Features [Num_Nodes, 5]
    # Features: [Type, Load, Traffic, Weather, Delay]
    x = torch.zeros((num_nodes, 5), dtype=torch.float)
    
    # Helper for Delay Stat (Average service time of last N completed events)
    # This is expensive if history is huge, but fine for demo scale.
    node_delays = {}
    for ev in reversed(engine.processed_events[-500:]): # Look at last 500 events
        if ev.event_type == EventType.END_SERVICE and ev.node_id in node_mapping:
            if ev.node_id not in node_delays:
                node_delays[ev.node_id] = []
            node_delays[ev.node_id].append(ev.details.get("service_duration", 0.0))
    
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
        x[idx, 1] = float(load)
        
        # Feature 4: Current Delays / Congestion (Calculate BASELINE first)
        # Metric: Average Service Time + Queue Penalty
        recent_times = node_delays.get(node_id, [])
        avg_svc = sum(recent_times) / len(recent_times) if recent_times else 0.0
        
        # Queue penalty: If queue is long, "delay expectation" rises
        queue_len = len(node_data.queue)
        # Heuristic: Delay = Avg Service + (Queue * 5 mins)
        estimated_delay = avg_svc + (queue_len * 5.0)

        # Feature 2 & 3: Context (Deterministic or Override)
        # Check for manual overrides ("Sabotage")
        overrides = st.session_state.get('node_overrides', {})
        
        if node_id in overrides:
             # User sabotaged this node! 
             # Force High Traffic (Feature 2)
             x[idx, 2] = overrides[node_id].get('traffic', 0.5) * 10.0 
             # Force High Weather Severity (Feature 3)
             x[idx, 3] = overrides[node_id].get('weather', 0.5)
             
             # CRITICAL: Also force Start Delay (Feature 4) to ensure GNN reacts immediately
             # 60.0 mins = Risk 1.0 roughly
             if x[idx, 2] > 7.0: # If traffic is high
                 # Deterministic "Expected" sabotage delay, or maintain RNG for jitter
                 # We need a stable random source here if we want jitter without global pollution
                 # But sticking to user request: "forced = 45.0 + rng.uniform(0, 30)"
                 
                 # Initialize private RNG for this node/time
                 # Use stable hash for seeding instead of hash()
                 node_hash = int(hashlib.md5(str(node_id).encode()).hexdigest(), 16)
                 seed_val = node_hash + int(engine.current_time / 60.0)
                 rng = random.Random(seed_val)
                 
                 forced = 45.0 + rng.uniform(0, 30) # 45-75 mins delay
                 
                 # Take MAX of real estimation vs forced delay
                 # This ensures we don't accidentally LOWER the risk if the node is ALREADY clogged naturally
                 x[idx, 4] = max(float(estimated_delay), float(forced))
             else:
                 x[idx, 4] = float(estimated_delay)
                 
        else:
            # Deterministic Seeding based on Node + Time (Hour)
            # This ensures stable predictions within the same simulation hour
            # Use stable hash for seeding instead of hash()
            node_hash = int(hashlib.md5(str(node_id).encode()).hexdigest(), 16)
            seed_val = node_hash + int(engine.current_time / 60.0)
            
            # Use private RNG instance to avoid polluting global random state
            rng = random.Random(seed_val)
            
            # Simulate "Live" Traffic/Weather
            traffic_sim = rng.uniform(0, 10)
            weather_sim = rng.uniform(0, 1)
            
            x[idx, 2] = float(traffic_sim)
            x[idx, 3] = float(weather_sim)
            
            # Normal delay estimation
            x[idx, 4] = float(estimated_delay)
        
    return Data(x=x, edge_index=edge_index, edge_attr=edge_attr)


def render_pydeck_map(engine, graph_builder, show_gnn_risk=False):
    """Render the graph using PyDeck and handle GNN History/Inference."""
    graph = graph_builder.graph
    
    # Initialize History Buffer if needed
    if 'gnn_history' not in st.session_state:
         # Deque of tensors [Num_Nodes, 5]
         st.session_state.gnn_history = []

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
            # Inference
            snapshot = get_current_graph_snapshot(engine, graph_builder)
            
            # HISTORY MANAGEMENT
            history = st.session_state.gnn_history
            
            # Add current features to history
            # Clone to detach from graph changes if any (though x is new tensor)
            history.append(snapshot.x)
            
            # Maintain max size 3
            if len(history) > 3:
                history.pop(0)
            
            # Prepare Stacked Input
            # If we don't have 3 steps yet, pad with the oldest available
            # e.g. [T0] -> [T0, T0, T0]
            # [T0, T1] -> [T0, T0, T1]
            input_stack = []
            if len(history) == 0:
                 # Should not happen as we just appended
                 input_stack = [snapshot.x] * 3
            elif len(history) == 1:
                 input_stack = [history[0]] * 3
            elif len(history) == 2:
                 input_stack = [history[0], history[0], history[1]]
            else:
                 input_stack = list(history) # [t-2, t-1, t]
                 
            # Concatenate features [Num_Nodes, 15]
            x_windowed = torch.cat(input_stack, dim=1)
            edge_attr_inf = snapshot.edge_attr
            
            # NORMALIZATION
            if gnn_scaler:
                # x [N, 15]
                xm = torch.tensor(gnn_scaler["x_mean"])
                xs = torch.tensor(gnn_scaler["x_std"])
                x_windowed = (x_windowed - xm) / xs
                
                # edge [E, 3]
                em = torch.tensor(gnn_scaler["edge_mean"])
                es = torch.tensor(gnn_scaler["edge_std"])
                edge_attr_inf = (edge_attr_inf - em) / es
            
            with torch.no_grad():
                # Correct call with 15-dim input and edge attributes
                logits = gnn_model(x_windowed, snapshot.edge_index, edge_attr_inf)
                risk_scores = torch.sigmoid(logits)
            
            gnn_data = []
            max_risk = risk_scores.max().item()
            for i, score in enumerate(risk_scores):
                # Use sorted nodes to match mapping logic in get_current_graph_snapshot
                node_id = sorted(graph.nodes())[i] 
                node = graph.nodes[node_id]['data']
                
                # Get input features for this node to debug
                x_val_delay = snapshot.x[i, 4].item()
                
                risk_val = score.item()
                # Color Gradient: Green (0) -> Red (1)
                r = int(255 * risk_val)
                g = int(255 * (1 - risk_val))
                
                # Debug Sabotage
                if risk_val < 0.1 and x_val_delay > 10.0:
                     print(f"⚠️ DEBUG: Node {node_id} has HIGH DELAY ({x_val_delay}) bu LOW PREDICTION ({risk_val})")
                
                gnn_data.append({
                    "lon": node.lon,
                    "lat": node.lat,
                    "weight": risk_val,   # For Heatmap
                    "risk": f"{risk_val:.2f}"
                })
                
            layers.append(pdk.Layer(
                "HeatmapLayer",
                gnn_data,
                get_position=["lon", "lat"],
                get_weight="weight",
                radius_pixels=80,      # "Smuga" effect radius
                intensity=1.5,
                threshold=0.05,        # Filter low risk noise
                opacity=0.6,
                # Gradient: Transparent -> Red
                color_range=[
                    [255, 255, 178],   # Light Yellow
                    [254, 204, 92],
                    [253, 141, 60],
                    [240, 59, 32],
                    [189, 0, 38]       # Deep Red
                ]
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

# --- Initialize Simulation (Before UI) ---
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
show_gnn_risk = st.sidebar.checkbox("👁️ Show AI Spatial Risk (GNN)", value=False)

# --- Sabotage Panel ---
st.sidebar.markdown("---")
st.sidebar.subheader("🔥 Chaos / Sabotage")
if 'node_overrides' not in st.session_state:
    st.session_state.node_overrides = {}

if 'graph_builder' in st.session_state and st.session_state.graph_builder:
    sabotage_node = st.sidebar.selectbox("Select Node to Sabotage", list(st.session_state.graph_builder.nodes.keys()))
    if sabotage_node:
        col_sab1, col_sab2 = st.sidebar.columns(2)
        s_traffic = col_sab1.slider("Traffic", 0.0, 1.0, 0.5, key="sab_traffic")
        s_weather = col_sab2.slider("Weather", 0.0, 1.0, 0.5, key="sab_weather")
        
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
            # Dynamic Chaos Update
            if auto_chaos:
                manage_dynamic_chaos(engine, gb)

            # Infinite Simulation Logic: Respawn orders if running low
            # Count active orders (Assigned/Created but not Completed)
            active_orders = sum(1 for o in engine.orders.values() if o.status not in ["COMPLETED", "CANCELLED"])
            if active_orders < 15: # Sustain at least 15 active orders
                # Spawn new batch
                all_ids = list(gb.nodes.keys())
                for _ in range(5):
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
