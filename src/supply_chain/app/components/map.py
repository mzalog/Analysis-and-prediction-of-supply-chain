import pydeck as pdk
import math
import streamlit as st
import torch
from supply_chain.simulation.schema import NodeType, TruckStatus
from supply_chain.models.inference import get_current_graph_snapshot

def render_pydeck_map(
    engine, 
    graph_builder, 
    gnn_model=None, 
    gnn_scaler=None, 
    is_gnn_v2=False, 
    show_gnn_risk=False, 
    test_mode=False,
    node_overrides=None
):
    """Render the graph using PyDeck and handle GNN History/Inference."""
    if node_overrides is None:
        node_overrides = {}

    graph = graph_builder.graph
    
    # Initialize History Buffer if needed (in session state)
    if 'gnn_history' not in st.session_state:
         # Deque of tensors [Num_Nodes, 5]
         st.session_state.gnn_history = []

    # --- Prepare Data for Layers ---
    
    # 1. Nodes Data
    nodes_data = []
    # Styles mapping to RGB colors
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
            snapshot = get_current_graph_snapshot(engine, graph_builder, node_overrides=node_overrides, test_mode=test_mode)
            
            # HISTORY MANAGEMENT
            history = st.session_state.gnn_history
            
            # Add current features to history
            history.append(snapshot.x)
            
            # Maintain max size 3
            if len(history) > 3:
                history.pop(0)
            
            # Prepare Stacked Input
            input_stack = []
            if len(history) == 0:
                 input_stack = [snapshot.x] * 3
            elif len(history) == 1:
                 input_stack = [history[0]] * 3
            elif len(history) == 2:
                 input_stack = [history[0], history[0], history[1]]
            else:
                 input_stack = list(history) # [t-2, t-1, t]
                 
            # Concatenate features [Num_Nodes, 18]
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
                out = gnn_model(x_windowed, snapshot.edge_index, edge_attr_inf)
                
                # Logic Fork based on Model Type
                if is_gnn_v2:
                    # V2: Regression (Output = Log-Delay)
                    pred_delay = torch.expm1(out.clamp(min=0.0)) # [N, 1]
                    
                    # Store for visualization
                    current_risks = {}
                    sorted_nodes = sorted(graph.nodes())
                    for i, val_tensor in enumerate(pred_delay):
                        node_id = sorted_nodes[i]
                        val = val_tensor.item()
                        current_risks[node_id] = val
                        
                    # print(f"🔍 GNN V2 Delay: Max={pred_delay.max().item():.1f} min")
                    
                else:
                    # V1: Classification
                    raw_probs = torch.sigmoid(out)
                    
                    # --- Legacy Calibration ---
                    sorted_nodes = sorted(graph.nodes())
                    
                    baseline = raw_probs.median().item()
                    denom = max(0.05, (baseline + 0.15) - baseline)
                    
                    risk_scores = (raw_probs - baseline) / denom
                    risk_scores = torch.clamp(risk_scores, 0.0, 1.0)
                    
                    # Demo Boost
                    for i, node_id in enumerate(sorted_nodes):
                        if node_id in node_overrides:
                            risk_scores[i] = max(risk_scores[i].item(), 0.85)
                            
                    current_risks = {}
                    for i, val_tensor in enumerate(risk_scores):
                         current_risks[sorted_nodes[i]] = val_tensor.item()
                         
                    # print(f"🔍 GNN V1 Risk: Max={risk_scores.max().item():.2f}")

            # --- Update Heatmap Layer ---
            gnn_data = []
            visual_node_map = {n['id']: n for n in nodes_data}
            
            for node_id, risk_val in current_risks.items():
                node = graph.nodes[node_id]['data']
                
                if is_gnn_v2:
                    is_hotspot = risk_val > 15.0
                    display_text = f"{risk_val:.0f} min"
                    weight = min(max(risk_val / 60.0, 0.2), 1.0) 
                    if risk_val < 1.0: weight = 0.0
                else:
                    is_hotspot = risk_val > 0.6
                    display_text = f"{risk_val:.2f}"
                    weight = risk_val if risk_val > 0.2 else 0.0
                
                if is_hotspot and node_id in visual_node_map:
                    intensity_factor = min(1.0, weight)
                    visual_node_map[node_id]['color'] = [255, int(255 * (1-intensity_factor)), int(255 * (1-intensity_factor))]
                    visual_node_map[node_id]['radius'] = 6000
                
                if weight > 0.1:
                     gnn_data.append({
                        "lon": node.lon,
                        "lat": node.lat,
                        "weight": weight,
                        "risk": display_text
                    })

            layers.append(pdk.Layer(
                "HeatmapLayer",
                gnn_data,
                get_position=["lon", "lat"],
                get_weight="weight",
                radius_pixels=80,
                intensity=1.5,
                threshold=0.05,
                opacity=0.6,
                color_range=[
                    [255, 255, 178],
                    [254, 204, 92],
                    [253, 141, 60],
                    [240, 59, 32],
                    [189, 0, 38]
                ]
            ))
        else:
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
        tooltip={"text": "{id}\n{type}\n{status}\nRisk: {risk}"},
        map_style="mapbox://styles/mapbox/dark-v10"
    )
