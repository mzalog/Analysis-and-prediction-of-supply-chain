import streamlit as st
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import time
import random
from typing import Dict, List
import sys
import os
import random
import math
import time
import base64
from pathlib import Path

# Add src to path so we can import supply_chain package
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(current_dir)
if src_dir not in sys.path:
    sys.path.append(src_dir)

from supply_chain.simulation.graph import GraphBuilder
from supply_chain.simulation.engine import SimulationEngine
from supply_chain.simulation.schema import Event, EventType, NodeType, TruckStatus
from supply_chain.simulation.visualization import SimulationVisualizer

# Page Config
st.set_page_config(
    page_title="Supply Chain Digital Twin",
    page_icon="🚚",
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
</style>
""", unsafe_allow_html=True)

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
                # Try to find the file
                if os.path.exists(tsplib_path):
                     gb.create_from_tsplib(Path(tsplib_path), k_neighbors=4)
                elif os.path.exists(os.path.join("..", tsplib_path)):
                     gb.create_from_tsplib(Path(os.path.join("..", tsplib_path)), k_neighbors=4)
                else:
                     st.error(f"File not found: {tsplib_path}. Falling back to random graph.")
                     gb.create_random_graph(num_nodes=15, k_neighbors=3)
            else:
                # Use sidebar params if available, else defaults
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
                    time=creation_time,
                    truck_id="SYSTEM",
                    node_id=origin,
                    event_type=EventType.ORDER_CREATED,
                    details={"order_id": f"ORD{i+1}", "origin": origin, "destination": destination}
                ))
            
            st.session_state.engine = engine
            st.session_state.graph_builder = gb
            st.session_state.simulation_time = 0.0
            st.session_state.running = False

def reset_simulation():
    if 'engine' in st.session_state:
        del st.session_state.engine
    init_simulation()

import pydeck as pdk

def render_pydeck_map(engine, graph_builder):
    """Render the graph using PyDeck."""
    graph = graph_builder.graph

    graph = graph_builder.graph

    # Load Icon Atlas (Served via Streamlit Static Files)
    # Using localhost URL ensures PyDeck treats it as a remote resource 
    # and doesn't try to open it locally (which causes FileNotFoundError).
    icon_atlas = "http://localhost:8501/app/static/icon_atlas.png"

    # Icon Mapping (Assuming 1024x1024 image with 5 icons in a row)
    # Mapping: Warehouse, Customer, Hub, Port, Inspection
    icon_mapping = {
        "warehouse":   {"x": 0,    "y": 0, "width": 204, "height": 1024, "mask": True},
        "customer":    {"x": 205,  "y": 0, "width": 204, "height": 1024, "mask": True},
        "hub":         {"x": 410,  "y": 0, "width": 204, "height": 1024, "mask": True},
        "port":        {"x": 615,  "y": 0, "width": 204, "height": 1024, "mask": True},
        "inspection":  {"x": 820,  "y": 0, "width": 204, "height": 1024, "mask": True},
        "default":     {"x": 0,    "y": 0, "width": 204, "height": 1024, "mask": True}
    }
    
    # --- Prepare Data for Layers ---
    
    # 1. Nodes Data
    nodes_data = []
    # Styles mapping to RGB colors (Distinct from Truck Status colors)
    # Truck: Idle(Grey), Pickup(Blue), Delivery(Green), Resting(Red)
    styles = {
        NodeType.WAREHOUSE: [138, 43, 226],    # BlueViolet (Distinct from Blue truck)
        NodeType.CUSTOMER: [255, 215, 0],      # Gold (Distinct from Green/Red)
        NodeType.HUB: [255, 140, 0],           # DarkOrange 
        NodeType.PORT: [0, 255, 255],          # Cyan
        NodeType.INSPECTION: [255, 105, 180]   # HotPink
    }
    
    for node_id in graph.nodes():
        node = graph.nodes[node_id]['data']
        color = styles.get(node.type, [128, 128, 128])
        
        # Pending orders count
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
            "color": [100, 100, 100, 100] # Gray, semi-transparent
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
        # Interpolation Logic
        lon, lat = 0.0, 0.0
        
        # Get start node pos
        if truck.current_node_id in graph.nodes:
            start_node = graph.nodes[truck.current_node_id]['data']
            lon, lat = start_node.lon, start_node.lat
            
            # Interpolate if moving
            if truck.current_leg_duration > 0 and truck.route and truck.current_node_index < len(truck.route):
                end_node_id = truck.route[truck.current_node_index]
                if end_node_id in graph.nodes:
                    end_node = graph.nodes[end_node_id]['data']
                    
                    elapsed = engine.current_time - truck.current_leg_start_time
                    t = elapsed / truck.current_leg_duration
                    t = max(0.0, min(1.0, t))
                    
                    lon = start_node.lon + (end_node.lon - start_node.lon) * t
                    lat = start_node.lat + (end_node.lat - start_node.lat) * t
        
        # Deterministic offset to separate overlapping trucks without jitter
        # Use hash of truck ID to get a stable small offset
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
        # Edges
        pdk.Layer(
            "LineLayer",
            edges_data,
            get_source_position="source",
            get_target_position="target",
            get_color="color",
            get_width=2,
            pickable=False,
        ),
        # Nodes (Scatterplot - Main representation)
        pdk.Layer(
            "ScatterplotLayer",
            nodes_data,
            get_position=["lon", "lat"],
            get_color="color",
            get_radius="radius",
            pickable=True,
            auto_highlight=True,
            opacity=0.8,
            stroked=True,
            filled=True,
            radius_min_pixels=8,  
            radius_max_pixels=20,
            get_line_color=[255, 255, 255],
            get_line_width=100
        ),
        # Node Labels (Restored)
        pdk.Layer(
            "TextLayer",
            nodes_data,
            get_position=["lon", "lat"],
            get_text="type",
            get_color=[200, 200, 200],
            get_size=12,
            get_alignment_baseline="top",
            get_pixel_offset=[0, 20]
        ),
        # Pending Orders Badges
        pdk.Layer(
            "TextLayer",
            nodes_data,
            get_position=["lon", "lat"],
            get_text="pending",
            get_color=[255, 255, 255],
            get_size=14,
            get_alignment_baseline="center",
            get_background_color=[255, 0, 0],
            get_border_color=[255, 255, 255],
            get_border_width=1,
            font_weight="bold",
            get_pixel_offset=[15, -15]
        ),
        # Trucks (Halo/Background)
        pdk.Layer(
            "ScatterplotLayer",
            trucks_data,
            get_position=["lon", "lat"],
            get_color="color",
            get_radius=4000, 
            pickable=True,
            auto_highlight=True,
            stroked=True,
            filled=True,
            opacity=0.5,
            radius_min_pixels=15,
            radius_max_pixels=25,
            get_line_color=[255, 255, 255],
            get_line_width=100
        ),
        # Truck Icons
        pdk.Layer(
            "TextLayer",
            trucks_data,
            get_position=["lon", "lat"],
            get_text="icon",
            get_size=25,
            get_alignment_baseline="center",
            pickable=True,
            font_family='"Segoe UI Emoji", "Apple Color Emoji", "Noto Color Emoji", sans-serif',
            font_settings={"sdf": False}
        ),
        # Truck Labels
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

    # View State (Center on mean of nodes)
    lats = [n['lat'] for n in nodes_data]
    lons = [n['lon'] for n in nodes_data]
    center_lat = sum(lats) / len(lats) if lats else 50.0
    center_lon = sum(lons) / len(lons) if lons else 19.0

    import math
    
    if lats and lons:
        min_lat, max_lat = min(lats), max(lats)
        min_lon, max_lon = min(lons), max(lons)
        
        # Add some padding
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
        tooltip={"text": "{id}\n{type}\n{status}"},
        map_style="mapbox://styles/mapbox/dark-v10"
    )


# Sidebar
st.sidebar.header("Configuration")

# Legend in Sidebar
with st.sidebar.expander("🗺️ Map Legend", expanded=True):
    st.markdown("**Truck Status:**")
    st.markdown("⚪ **Idle** (Grey)")
    st.markdown("🔵 **To Pickup** (Blue)")
    st.markdown("🟢 **To Delivery** (Green)")
    st.markdown("🔴 **Resting** (Red)")
    st.markdown("---")
    st.markdown("**Locations:**")
    st.markdown("🟣 **Warehouse** (Purple)")
    st.markdown("🟡 **Customer** (Gold)")
    st.markdown("🟠 **Hub** (Orange)")
    st.markdown("🔵 **Port** (Cyan)")
    st.markdown("🛑 **Inspection** (Pink)")

# Graph Source Selection
graph_source = st.sidebar.radio(
    "Graph Source", 
    ["Random", "TSPLIB File"],
    index=0
)

st.session_state.graph_source = graph_source

if st.session_state.graph_source == "Random":
    st.session_state.num_nodes = st.sidebar.slider("Number of Nodes", 10, 50, 15)
else:
    st.session_state.tsplib_path = st.sidebar.text_input("TSPLIB File Path", "kroA100.txt")

# Detect change and reset view
if "last_graph_source" not in st.session_state:
    st.session_state.last_graph_source = graph_source

if st.session_state.last_graph_source != graph_source:
    st.session_state.last_graph_source = graph_source
    if "view_state" in st.session_state:
        del st.session_state.view_state
    reset_simulation()

st.session_state.num_trucks = st.sidebar.slider("Number of Trucks", 5, 30, 10)

sim_speed = st.sidebar.slider("Simulation Speed (steps/frame)", 1, 10, 1)

if st.sidebar.button("Reset Simulation"):
    reset_simulation()
    if "view_state" in st.session_state:
        del st.session_state.view_state

# Initialize
init_simulation()
engine = st.session_state.engine
gb = st.session_state.graph_builder

# Header
st.title("Supply Chain Digital Twin")
st.markdown("Real-time simulation of logistics network with autonomous agents.")

st.sidebar.slider("UI refresh (seconds)", 0.1, 2.0, 0.2, key="ui_refresh")

# Controls
col_ctrl1, col_ctrl2 = st.columns([1, 5])

def start():
    st.session_state.running = True

def pause():
    st.session_state.running = False

col_ctrl1.button("Start / Resume", type="primary", on_click=start, disabled=st.session_state.running)
col_ctrl1.button("Pause", on_click=pause, disabled=not st.session_state.running)

run_every = st.session_state.ui_refresh if st.session_state.running else None

@st.fragment(run_every=run_every)
def live_panel():
    engine = st.session_state.engine
    gb = st.session_state.graph_builder

    # Simulation step
    if st.session_state.running:
        for _ in range(sim_speed):
            if engine.event_queue:
                engine.step()

    # Metrics
    completed = [o for o in engine.orders.values() if o.status == "COMPLETED"]
    cancelled = [o for o in engine.orders.values() if o.status == "CANCELLED"]
    in_progress = [o for o in engine.orders.values() if o.status == "ASSIGNED"]
    active_trucks = sum(1 for t in engine.trucks.values() if t.status != TruckStatus.IDLE)

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Time", f"{engine.current_time:.1f}m")
    c2.metric("Delivered", len(completed))
    c3.metric("In Progress", len(in_progress))
    c4.metric("Cancelled", len(cancelled))
    c5.metric("Trucks", f"{active_trucks}/{len(engine.trucks)}")
    c6.metric("Event Queue", len(engine.event_queue))

    deck = render_pydeck_map(engine, gb)

    if "view_state" in st.session_state:
        deck.initial_view_state = st.session_state.view_state
    else:
        st.session_state.view_state = deck.initial_view_state

    st.pydeck_chart(deck, use_container_width=True, key="sim_map")
    
live_panel()
