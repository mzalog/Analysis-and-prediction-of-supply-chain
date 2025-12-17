

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from pathlib import Path

from supply_chain.simulation.schema import Event, EventType



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
        "delivery_time_deviation": {"mean": 4.0, "std": 4.0, "min": -2.0, "max": 10.0},
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

        if col_name not in self.stats:
            return base_factor
        
        stat = self.stats[col_name]

        base_val = stat["min"] + base_factor * (stat["max"] - stat["min"])
        noise = random.gauss(0, stat["std"] * 0.3)
        val = base_val + noise
        return max(stat["min"], min(stat["max"], val))


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
        

        if engine and truck_id and truck_id in engine.trucks:
            truck = engine.trucks[truck_id]
            order_id = truck.assigned_order_id if truck.assigned_order_id else ""
            truck_status = truck.status.value if truck.status else ""
        
        node_id = event.node_id if event.node_id else ""
        event_type_str = event.event_type.value if event.event_type else ""
        

        timestamp = start_date + timedelta(minutes=event.time)
        hour_of_day = timestamp.hour
        
        lat, lon = 40.0, -90.0
        node_type = None
        if graph_builder and event.node_id:
            try:
                node = graph_builder.get_node(event.node_id)
                lat, lon = node.lat, node.lon
                node_type = node.type
            except (KeyError, AttributeError):
                pass
        

        
        traffic = calibrator.sample_correlated("traffic_congestion_level", rush_hour_factor)
        
        weather = calibrator.sample("weather_condition_severity")
        weather_factor = weather
        
        risk_base = (travel_factor + weather_factor + rush_hour_factor * 0.5) / 2.5
        route_risk = calibrator.sample_correlated("route_risk_level", risk_base)
        
        delay_prob = calibrator.sample_correlated("delay_probability", risk_base)
        
        eta_base = (traffic / 10.0 + weather_factor) / 2.0
        eta_variation = calibrator.sample_correlated("eta_variation_hours", eta_base)
        
        fuel = calibrator.sample("fuel_consumption_rate") * (1 + traffic * 0.05)
        
        warehouse_factor = 0.8 if node_type and "WAREHOUSE" in str(node_type) else 0.4
        warehouse_inv = calibrator.sample_correlated("warehouse_inventory_level", warehouse_factor)
        
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
        
        deviation = calibrator.sample_correlated("delivery_time_deviation", delay_prob)
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
