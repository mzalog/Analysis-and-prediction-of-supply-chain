

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from pathlib import Path

from supply_chain.simulation.schema import Event, EventType



import math

SIMULATION_COLUMNS = [
    "truck_id",
    "order_id", 
    "node_id",
    "node_type",
    "event_type",
    "truck_status",
    "timestamp",
    "vehicle_gps_latitude",
    "vehicle_gps_longitude",
    "fuel_consumption_rate",
    "eta_variation_hours",
    "traffic_congestion_level",
    "warehouse_inventory_level",
    "loading_unloading_time",
    "handling_equipment_availability",
    "order_fulfillment_status",
    "weather_condition_severity",
    "port_congestion_level",
    "shipping_costs",
    "supplier_reliability_score",
    "lead_time_days",
    "historical_demand",
    "iot_temperature",
    "cargo_condition_status",
    "route_risk_level",
    "customs_clearance_time",
    "driver_behavior_score",
    "fatigue_monitoring_score",
    "disruption_likelihood_score",
    "delay_probability",
    "risk_classification",
    "delivery_time_deviation",
    "pending_orders_count",
]

KAGGLE_COLUMNS = SIMULATION_COLUMNS[6:]


class StatsCalibrator:

    

    DEFAULTS = {
        "vehicle_gps_latitude": {"mean": 40.0, "std": 8.0, "min": 25.0, "max": 55.0},
        "vehicle_gps_longitude": {"mean": -90.0, "std": 20.0, "min": -125.0, "max": -65.0},
        "fuel_consumption_rate": {"mean": 7.0, "std": 3.0, "min": 3.0, "max": 25.0},
        "eta_variation_hours": {"mean": 2.5, "std": 2.0, "min": -2.0, "max": 8.0},
        "traffic_congestion_level": {"mean": 5.0, "std": 3.0, "min": 0.0, "max": 10.0},
        "warehouse_inventory_level": {"mean": 400.0, "std": 300.0, "min": 0.0, "max": 1000.0},
        "loading_unloading_time": {"mean": 2.5, "std": 1.5, "min": 0.5, "max": 5.0},
        "weather_condition_severity": {"mean": 0.4, "std": 0.3, "min": 0.0, "max": 1.0},
        "port_congestion_level": {"mean": 5.0, "std": 3.0, "min": 0.0, "max": 10.0},
        "shipping_costs": {"mean": 400.0, "std": 250.0, "min": 100.0, "max": 1000.0},
        "supplier_reliability_score": {"mean": 0.6, "std": 0.25, "min": 0.0, "max": 1.0},
        "lead_time_days": {"mean": 5.0, "std": 4.0, "min": 1.0, "max": 15.0},
        "historical_demand": {"mean": 5000.0, "std": 3000.0, "min": 0.0, "max": 10000.0},
        "iot_temperature": {"mean": 5.0, "std": 15.0, "min": -10.0, "max": 40.0},
        "route_risk_level": {"mean": 5.0, "std": 3.0, "min": 0.0, "max": 10.0},
        "customs_clearance_time": {"mean": 2.0, "std": 1.5, "min": 0.5, "max": 5.0},
        "driver_behavior_score": {"mean": 0.5, "std": 0.3, "min": 0.0, "max": 1.0},
        "fatigue_monitoring_score": {"mean": 0.7, "std": 0.25, "min": 0.0, "max": 1.0},
        "disruption_likelihood_score": {"mean": 0.6, "std": 0.3, "min": 0.0, "max": 1.0},
        "delay_probability": {"mean": 0.5, "std": 0.3, "min": 0.0, "max": 1.0},
        "delivery_time_deviation": {"mean": 15.0, "std": 30.0, "min": -10.0, "max": 180.0},
    }
    
    def __init__(self, csv_path: Optional[Path] = None):
        self.csv_path = csv_path
        self.stats: Dict[str, Dict[str, float]] = dict(self.DEFAULTS)
        
    def load_and_calibrate(self):

        if self.csv_path is None or not self.csv_path.exists():
            print(f"Using default calibration (no CSV at {self.csv_path})")
            return
        
        try:
            df = pd.read_csv(self.csv_path, nrows=5000)
            
            for col in KAGGLE_COLUMNS:
                if col in df.columns and df[col].dtype in [np.float64, np.int64, float, int]:
                    self.stats[col] = {
                        "mean": float(df[col].mean()),
                        "std": float(df[col].std()),
                        "min": float(df[col].min()),
                        "max": float(df[col].max()),
                    }
            print(f"Calibrated from {self.csv_path}")
        except Exception as e:
            print(f"Warning: Could not calibrate from {self.csv_path}: {e}")
    
    def sample(self, col_name: str) -> float:

        if col_name not in self.stats:
            return random.random()
        
        stat = self.stats[col_name]
        val = random.gauss(stat["mean"], stat["std"])
        return max(stat["min"], min(stat["max"], val))
    
    def sample_correlated(self, col_name: str, base_factor: float) -> float:
        """
        Samples a value where the MEAN is shifted by base_factor, 
        instead of scaling the bounds, to avoid saturation.
        base_factor: 1.0 = Mean, >1.0 = Higher, <1.0 = Lower.
        """
        if col_name not in self.stats:
            return base_factor
        
        stat = self.stats[col_name]
        
        # New Logic: Shift Mean, keep Std Dev similar (or slightly scaled)
        # We want base_factor=1.0 -> mean
        # base_factor=2.0 -> mean * 2 (roughly)
        
        target_mean = stat["mean"] * base_factor
        
        # dynamic_std = stat["std"] * (1.0 + abs(base_factor - 1.0) * 0.2) 
        # (Slight instability if factor is wild)
        
        val = random.gauss(target_mean, stat["std"])
        
        # Soft Clamping: Allow exceeding max/min slightly if factor forces it, 
        # but try to stick to realistic bounds if possible.
        # Actually, for "saturation" issues, we should just NOT cap at max 
        # if the logic demands higher values (e.g. extreme delays).
        # But for columns with physical limits (0-1 scores), we MUST cap.
        
        if col_name in ["order_fulfillment_status", "weather_condition_severity", 
                        "supplier_reliability_score", "cargo_condition_status",
                        "driver_behavior_score", "fatigue_monitoring_score",
                        "disruption_likelihood_score", "delay_probability"]:
            return max(0.0, min(1.0, val))
            
        return max(stat["min"], val) # One-sided clamp for physical quantities (no negative delays)

    def _periodic_noise(self, x: float, period: float, amplitude: float) -> float:
        """Simple sine-based noise."""
        return amplitude * math.sin(2 * math.pi * x / period)

    def _get_spatial_noise(self, lat: float, lon: float, time_hours: float) -> float:
        """
        Generates correlated noise based on location and time.
        Returns a value approx between 0 and 1.
        """
        # "Fronts" move over time.
        # Lat/Lon scale: approx 1 degree ~ 100km.
        # Fronts might be 5-10 degrees wide.
        
        # Spatial wave moving East-West
        spatial_val = math.sin(lon / 5.0 + time_hours / 24.0) 
        spatial_val += math.cos(lat / 5.0 + time_hours / 48.0)
        
        # Normalize to 0-1 (approx)
        return (spatial_val + 2.0) / 4.0


class DataConverter:

    
    @staticmethod
    def events_to_dataframe(
        events: List[Event],
        calibrator: StatsCalibrator,
        engine: Any = None,
        graph_builder: Any = None,
        start_date: datetime = datetime(2024, 1, 1),
        include_context: bool = True,
    ) -> pd.DataFrame:

        rows = []
        

        relevant_events = [
            e for e in events 
            if e.event_type in (EventType.ARRIVAL_NODE, EventType.END_SERVICE, EventType.DEPART_NODE)
        ]
        
        for ev in relevant_events:
            row = DataConverter._create_row(
                ev, calibrator, engine, graph_builder, start_date, include_context
            )
            rows.append(row)
        
        columns = SIMULATION_COLUMNS if include_context else KAGGLE_COLUMNS
        df = pd.DataFrame(rows, columns=columns)
        return df
    
    @staticmethod
    def _create_row(
        event: Event,
        calibrator: StatsCalibrator,
        engine: Any,
        graph_builder: Any,
        start_date: datetime,
        include_context: bool = True,
    ) -> Dict[str, Any]:

        

        truck_id = event.truck_id if event.truck_id else ""
        order_id = ""
        truck_status = ""
        
        result = {}

        if engine and truck_id and truck_id in engine.trucks:
            truck = engine.trucks[truck_id]
            order_id = truck.assigned_order_id if truck.assigned_order_id else ""
            truck_status = truck.status.value if truck.status else ""
        
        node_id = event.node_id if event.node_id else ""
        event_type_str = event.event_type.value if event.event_type else ""
        

        timestamp = start_date + timedelta(minutes=event.time)
        hour_of_day = timestamp.hour
        weekday = timestamp.weekday() # 0-4=Mon-Fri, 5-6=Sat-Sun
        month = timestamp.month
        day = timestamp.day
        
        lat, lon = 40.0, -90.0
        node_type = None
        if graph_builder and event.node_id:
            try:
                node = graph_builder.get_node(event.node_id)
                lat, lon = node.lat, node.lon
                node_type = node.type
            except (KeyError, AttributeError):
                pass
        
        # --- ADVANCED TRAFFIC LOGIC (External Vehicles) ---
        
        # 1. Day Type Factor
        is_weekend = weekday >= 5
        # Simple holiday approximation (Fixed dates + Christmas period)
        is_holiday = (month == 12 and day >= 24) or (month == 1 and day == 1) or (month == 5 and day == 1) 
        
        if is_holiday:
            traffic_modifier = 0.4 # Holidays = Empty roads (usually)
        elif is_weekend:
            traffic_modifier = 0.6 # Weekends = Less commercial traffic
        else:
            traffic_modifier = 1.0 # Workdays = Base
            
        # 2. Hourly Profile (Rush Hours)
        # 2. Hourly Profile (Rush Hours)
        if 7 <= hour_of_day <= 9:
            traffic_modifier *= 1.8 # Heavy Rush
        elif 16 <= hour_of_day <= 18:
            traffic_modifier *= 1.8 # Heavy Rush
        elif 10 <= hour_of_day <= 15:
            traffic_modifier *= 1.1 # Standard Traffic
        elif 22 <= hour_of_day or hour_of_day <= 5:
            traffic_modifier *= 0.3 # Empty Night Roads
            
        # --- SPATIAL CORRELATIONS ---
        # Time in hours (continuous) for weather fronts
        time_h = start_date.timestamp() / 3600.0 + (event.time / 60.0)
        
        # 1. Weather Field
        # Base weather from calibration is global "severity" probability
        # We mod it with spatial factors
        spatial_w = calibrator._get_spatial_noise(lat, lon, time_h)
        # Combine: Global Seasonality (already in calibrator defaults via 'iot_temperature' logic maybe?)
        # Let's use spatial_w to drive the "local" severity
        weather = spatial_w + random.gauss(0, 0.1)
        weather = max(0.0, min(1.0, weather))
        
        weather_factor = weather
        
        # 2. Traffic Field
        # Traffic is mostly time-based (Rush Hour) + Spatial Hotspots
        spatial_t = calibrator._get_spatial_noise(lat + 10, lon + 10, time_h) # Different phase
        traffic_base = calibrator.stats["traffic_congestion_level"]["mean"]
        
        # Apply modifiers
        traffic = traffic_base * traffic_modifier * (0.8 + 0.4 * spatial_t)
        
        # Restore duration variables
        travel_duration = event.details.get("travel_duration", 0.0) if event.details else 0.0
        travel_factor = min(1.0, travel_duration / 600.0)

        service_duration = event.details.get("service_duration", 0.0) if event.details else 0.0
        service_factor = min(1.0, service_duration / 300.0)
        
        # Random "Other Vehicles" Noise
        traffic += random.gauss(0, 0.5)
        traffic = max(0.0, min(10.0, traffic))
        
        risk_base = (travel_factor + weather_factor + (traffic / 15.0)) / 2.5
        route_risk = calibrator.sample_correlated("route_risk_level", 1.0 + risk_base) # Use base_factor ~1.0+
        
        delay_prob = calibrator.sample_correlated("delay_probability", 1.0 + risk_base)
        
        # Capacity Utilization (Orders vs Trucks)
        utilization_ratio = 1.0 # Default
        if engine and hasattr(engine, 'trucks') and hasattr(engine, 'orders'):
             num_trucks = len(engine.trucks)
             
             # Global stats for Capacity Penalty
             pending_count_global = len(engine.pending_orders)
             in_transit_count = sum(1 for t in engine.trucks.values() if t.assigned_order_id)
             active_orders_count = pending_count_global + in_transit_count
             
             if num_trucks > 0:
                 utilization_ratio = active_orders_count / num_trucks
        
        # Local Backlog (Feature for GNN)
        node_backlog = 0
        if engine and event.node_id:
            # Count pending orders originating from THIS node
            # Optimization: could maintain a separate map, but filter is okay for N=100-500 orders
            node_backlog = sum(1 for oid in engine.pending_orders if engine.orders[oid].origin_node_id == event.node_id)
                 
        # Delay Calculation Logic (Organic)
        base_delay_dev = calibrator.sample("delivery_time_deviation")
        
        # 1. Capacity Factor (Refined)
        # Previous logic was too harsh for the active truck count (Ratio ~15 -> ~5000 min delay)
        # New logic: Start penalizing only when truly overwhelmed, and scale linearly/logarithmically.
        capacity_penalty = 0.0
        if utilization_ratio > 1.0:
            # Linear penalty: at ratio 10 (4000 orders/400 trucks), delay is ~30 mins + random
            # At ratio 20 (peak), delay is ~60-90 mins.
            # This allows "Low Risk" to exist when ratio is < 5.
            capacity_penalty = (utilization_ratio - 1.0) * 8.0 
            
            # Add quadratic spice only for extreme chaos
            if utilization_ratio > 15.0:
                 capacity_penalty += ((utilization_ratio - 15.0) ** 1.5) * 2.0 
            
        # 2. Environmental Factor
        env_penalty = 0.0
        if traffic > 7.0 and weather > 0.7:
             env_penalty = random.uniform(30, 90)
        elif traffic > 8.0:
             env_penalty = random.uniform(15, 45)
             
        # Total Delay
        total_delay = base_delay_dev + capacity_penalty + env_penalty
        if random.random() < 0.01: total_delay += random.uniform(60, 120)
        
        # REALISM UPDATE: "Order Not Delivered" Logic
        # If delay is extreme (> 7 days / 10,000 mins), the order is considered FAILED/LOST.
        # This prevents "infinity" values (e.g. 9 years delay) which break AI models.
        FAILURE_THRESHOLD = 10000.0 
        
        if total_delay > FAILURE_THRESHOLD:
            total_delay = FAILURE_THRESHOLD # Cap at "Lost"
            fulfillment_status = 0.0 # Failed/Cancelled
        
        # FINAL SAFETY CLAMP (Relaxed to 1 week)
        total_delay = max(-10.0, min(FAILURE_THRESHOLD, total_delay))

        eta_base = (traffic / 10.0 + weather_factor) / 2.0
        eta_variation = calibrator.sample_correlated("eta_variation_hours", eta_base)
        
        fuel = calibrator.sample("fuel_consumption_rate") * (1 + traffic * 0.05)
        
        warehouse_factor = 0.8 if node_type and "WAREHOUSE" in str(node_type) else 0.4
        
        # Feedback Loop: Inventory depends on Busy Count
        # If node is busy, inventory drops faster or fills up faster (context dependent).
        # Let's say high traffic/busy = lower inventory (depletion)
        node_busy_factor = 1.0
        if graph_builder and event.node_id:
             try:
                 node_obj = graph_builder.get_node(event.node_id)
                 if node_obj.busy_count > 0:
                     # More busy = more depletion?
                     node_busy_factor = 0.5
             except: pass
        
        warehouse_inv = calibrator.sample_correlated("warehouse_inventory_level", warehouse_factor * node_busy_factor)
        
        port_factor = 0.9 if node_type and "PORT" in str(node_type) else 0.3
        port_cong = calibrator.sample_correlated("port_congestion_level", port_factor)
        
        loading_time = calibrator.sample_correlated("loading_unloading_time", service_factor)
        
        equip_avail = 1 if random.random() > (0.3 + service_factor * 0.2) else 0
        
        shipping = calibrator.sample("shipping_costs") * (1 + travel_factor * 0.3)
        

        supplier_rel = calibrator.sample("supplier_reliability_score")
        
        lead_time = calibrator.sample_correlated("lead_time_days", delay_prob)
        
        demand = calibrator.sample("historical_demand")
        
        month = timestamp.month
        temp_base = 20 * np.sin((month - 3) * np.pi / 6)
        iot_temp = temp_base + random.gauss(0, 10)
        iot_temp = max(-10, min(40, iot_temp))
        

        cargo_ok = 1 if (abs(iot_temp) < 30 and route_risk < 7 and random.random() > 0.1) else 0
        
        customs_factor = travel_factor * 0.8
        customs_time = calibrator.sample_correlated("customs_clearance_time", customs_factor)
        
        hours_driving = event.time / 60.0
        fatigue_factor = min(1.0, hours_driving / 12.0)
        fatigue = calibrator.sample_correlated("fatigue_monitoring_score", fatigue_factor)
        driver_behavior = calibrator.sample("driver_behavior_score")
        

        disruption = calibrator.sample_correlated(
            "disruption_likelihood_score",
            (delay_prob + weather_factor + route_risk / 10.0) / 3.0
        )
        
        fulfillment_status = DataConverter._determine_fulfillment(
            event, engine, delay_prob, route_risk
        )
        
        risk_class = DataConverter._classify_risk(delay_prob, route_risk, disruption)
        
        deviation = total_delay # Use our calculated organic delay
        if fulfillment_status < 0.5:
            deviation = abs(deviation) + random.uniform(0, 3)
        

        if include_context:
            result["truck_id"] = truck_id
            result["order_id"] = order_id
            result["node_id"] = node_id
            result["node_type"] = str(node_type) if node_type else ""
            result["event_type"] = event_type_str
            result["truck_status"] = truck_status
        

        result.update({
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "vehicle_gps_latitude": round(lat, 6),
            "vehicle_gps_longitude": round(lon, 6),
            "fuel_consumption_rate": round(fuel, 2),
            "eta_variation_hours": round(eta_variation, 2),
            "traffic_congestion_level": round(traffic, 2),
            "warehouse_inventory_level": round(warehouse_inv, 2),
            "loading_unloading_time": round(loading_time, 2),
            "handling_equipment_availability": equip_avail,
            "order_fulfillment_status": round(fulfillment_status, 4),
            "weather_condition_severity": round(weather, 4),
            "port_congestion_level": round(port_cong, 2),
            "shipping_costs": round(shipping, 2),
            "supplier_reliability_score": round(supplier_rel, 4),
            "lead_time_days": round(lead_time, 2),
            "historical_demand": round(demand, 2),
            "iot_temperature": round(iot_temp, 2),
            "cargo_condition_status": cargo_ok,
            "route_risk_level": round(route_risk, 2),
            "customs_clearance_time": round(customs_time, 2),
            "driver_behavior_score": round(driver_behavior, 4),
            "fatigue_monitoring_score": round(fatigue, 4),
            "disruption_likelihood_score": round(disruption, 4),
            "delay_probability": round(delay_prob, 4),
            "risk_classification": risk_class,
            "delivery_time_deviation": round(deviation, 2),
            "pending_orders_count": node_backlog,
        })
        
        return result
    
    @staticmethod
    def _determine_fulfillment(
        event: Event, 
        engine: Any, 
        delay_prob: float, 
        route_risk: float
    ) -> float:

        if engine and event.truck_id and event.truck_id != "SYSTEM":
            try:
                truck = engine.trucks.get(event.truck_id)
                if truck and truck.assigned_order_id:
                    order = engine.orders.get(truck.assigned_order_id)
                    if order:
                        if order.status == "COMPLETED":
                            return random.uniform(0.85, 1.0)
                        elif order.status == "CANCELLED":
                            return random.uniform(0.0, 0.2)
            except (AttributeError, KeyError):
                pass
        

        base_fulfillment = 1.0 - (delay_prob * 0.4 + route_risk / 10.0 * 0.3)
        noise = random.gauss(0, 0.1)
        return max(0.0, min(1.0, base_fulfillment + noise))
    
    @staticmethod
    def _classify_risk(delay_prob: float, route_risk: float, disruption: float) -> str:

        risk_score = (delay_prob + route_risk / 10.0 + disruption) / 3.0
        
        if risk_score > 0.7:
            return "High Risk"
        elif risk_score > 0.4:
            return "Moderate Risk"
        else:
            return "Low Risk"
