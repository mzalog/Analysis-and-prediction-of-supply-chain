from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Tuple


@dataclass(frozen=True)
class DataPaths:
    """Central definition of project data and report paths."""

    project_root: Path = field(
        default_factory=lambda: Path(__file__).resolve().parents[2]
    )
    raw_data_dir: Path = field(init=False)
    interim_data_dir: Path = field(init=False)
    processed_data_dir: Path = field(init=False)
    reports_dir: Path = field(init=False)
    figures_dir: Path = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "raw_data_dir",
            self.project_root / "data" / "raw",
        )
        object.__setattr__(
            self,
            "interim_data_dir",
            self.project_root / "data" / "interim",
        )
        object.__setattr__(
            self,
            "processed_data_dir",
            self.project_root / "data" / "processed",
        )
        object.__setattr__(
            self,
            "reports_dir",
            self.project_root / "reports",
        )
        object.__setattr__(
            self,
            "figures_dir",
            self.reports_dir / "figures",
        )

    @property
    def raw_csv_path(self) -> Path:
        """Default path to the Kaggle CSV file."""
        return self.raw_data_dir / "dynamic_supply_chain_logistics_dataset.csv"

    @property
    def interim_parquet_path(self) -> Path:
        """Default path to the cleaned dataset in Parquet format."""
        return self.interim_data_dir / "cleaned_logistics_data.parquet"


@dataclass(frozen=True)
class DatasetSchema:
    """
    Dataset-specific configuration for the Kaggle logistics dataset.

    This defines:
    - canonical column names,
    - datetime / target columns,
    - numeric and binary features.
    """

    datetime_column: str = "timestamp"
    target_column: str = "order_fulfillment_status"

    column_renames: Mapping[str, str] = field(
        default_factory=lambda: {
            "Timestamp": "timestamp",
            "timestamp": "timestamp",

            "Vehicle GPS Latitude": "vehicle_gps_latitude",
            "vehicle_gps_latitude": "vehicle_gps_latitude",

            "Vehicle GPS Longitude": "vehicle_gps_longitude",
            "vehicle_gps_longitude": "vehicle_gps_longitude",

            "Fuel Consumption Rate": "fuel_consumption_rate",
            "fuel_consumption_rate": "fuel_consumption_rate",

            "ETA Variation (hours)": "eta_variation_hours",
            "eta_variation_hours": "eta_variation_hours",

            "Traffic Congestion Level": "traffic_congestion_level",
            "traffic_congestion_level": "traffic_congestion_level",

            "Warehouse Inventory Level": "warehouse_inventory_level",
            "warehouse_inventory_level": "warehouse_inventory_level",

            "Loading/Unloading Time": "loading_unloading_time",
            "loading_unloading_time": "loading_unloading_time",

            "Handling Equipment Availability": "handling_equipment_availability",
            "handling_equipment_availability": "handling_equipment_availability",

            "Order Fulfillment Status": "order_fulfillment_status",
            "order_fulfillment_status": "order_fulfillment_status",

            "Weather Condition Severity": "weather_condition_severity",
            "weather_condition_severity": "weather_condition_severity",

            "Port Congestion Level": "port_congestion_level",
            "port_congestion_level": "port_congestion_level",

            "Shipping Costs": "shipping_costs",
            "shipping_costs": "shipping_costs",

            "Supplier Reliability Score": "supplier_reliability_score",
            "supplier_reliability_score": "supplier_reliability_score",

            "Lead Time (days)": "lead_time_days",
            "lead_time_days": "lead_time_days",

            "Historical Demand": "historical_demand",
            "historical_demand": "historical_demand",

            "IoT Temperature": "iot_temperature",
            "iot_temperature": "iot_temperature",

            "Cargo Condition Status": "cargo_condition_status",
            "cargo_condition_status": "cargo_condition_status",

            "Route Risk Level": "route_risk_level",
            "route_risk_level": "route_risk_level",

            "Customs Clearance Time": "customs_clearance_time",
            "customs_clearance_time": "customs_clearance_time",

            "Driver Behavior Score": "driver_behavior_score",
            "driver_behavior_score": "driver_behavior_score",

            "Fatigue Monitoring Score": "fatigue_monitoring_score",
            "fatigue_monitoring_score": "fatigue_monitoring_score",

            "disruption_likelihood_score": "disruption_likelihood_score",
            "delay_probability": "delay_probability",
            "risk_classification": "risk_classification",
            "delivery_time_deviation": "delivery_time_deviation",
        }
    )

    @property
    def binary_features(self) -> Tuple[str, ...]:
        """Features that are logically binary (0/1)."""
        return (
            "handling_equipment_availability",
            "order_fulfillment_status",
            "cargo_condition_status",
        )

    @property
    def continuous_features(self) -> Tuple[str, ...]:
        """Continuous numeric features used for correlations and EDA."""
        return (
            "vehicle_gps_latitude",
            "vehicle_gps_longitude",
            "fuel_consumption_rate",
            "eta_variation_hours",
            "traffic_congestion_level",
            "warehouse_inventory_level",
            "loading_unloading_time",
            "weather_condition_severity",
            "port_congestion_level",
            "shipping_costs",
            "supplier_reliability_score",
            "lead_time_days",
            "historical_demand",
            "iot_temperature",
            "route_risk_level",
            "customs_clearance_time",
            "driver_behavior_score",
            "fatigue_monitoring_score",
            "disruption_likelihood_score",
            "delay_probability",
            "delivery_time_deviation",
        )

    @property
    def numeric_features(self) -> Tuple[str, ...]:
        return self.continuous_features + self.binary_features

_paths = DataPaths()
DATA_RAW_DIR = _paths.raw_data_dir
REPORTS_DIR = _paths.reports_dir
