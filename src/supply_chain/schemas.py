from __future__ import annotations

from typing import List

import pandera as pa
from pandera.typing import Series

class SupplyChainSchema(pa.DataFrameModel):
    """
    Pandera schema for the logistics supply chain dataset.
    Enforces data types and valid ranges for key features.
    """

    # Target
    order_fulfillment_status: Series[str] = pa.Field(coerce=True, isin=["On Time", "Delayed"])

    # Numeric Features - Continuous
    vehicle_gps_latitude: Series[float] = pa.Field(ge=-90, le=90, coerce=True)
    vehicle_gps_longitude: Series[float] = pa.Field(ge=-180, le=180, coerce=True)
    fuel_consumption_rate: Series[float] = pa.Field(ge=0, coerce=True)
    eta_variation_hours: Series[float] = pa.Field(coerce=True)
    traffic_congestion_level: Series[float] = pa.Field(ge=0, le=10, coerce=True, nullable=True)
    warehouse_inventory_level: Series[float] = pa.Field(ge=0, coerce=True)
    loading_unloading_time: Series[float] = pa.Field(ge=0, coerce=True)
    weather_condition_severity: Series[float] = pa.Field(ge=0, le=10, coerce=True, nullable=True)
    port_congestion_level: Series[float] = pa.Field(ge=0, le=10, coerce=True, nullable=True)
    shipping_costs: Series[float] = pa.Field(ge=0, coerce=True)
    supplier_reliability_score: Series[float] = pa.Field(ge=0, le=10, coerce=True, nullable=True)
    lead_time_days: Series[float] = pa.Field(ge=0, coerce=True)
    historical_demand: Series[float] = pa.Field(ge=0, coerce=True)
    iot_temperature: Series[float] = pa.Field(coerce=True)
    route_risk_level: Series[float] = pa.Field(ge=0, le=10, coerce=True, nullable=True)
    customs_clearance_time: Series[float] = pa.Field(ge=0, coerce=True)
    driver_behavior_score: Series[float] = pa.Field(ge=0, le=10, coerce=True, nullable=True)
    fatigue_monitoring_score: Series[float] = pa.Field(ge=0, le=10, coerce=True, nullable=True)
    disruption_likelihood_score: Series[float] = pa.Field(ge=0, le=1, coerce=True, nullable=True)
    delay_probability: Series[float] = pa.Field(ge=0, le=1, coerce=True, nullable=True)
    delivery_time_deviation: Series[float] = pa.Field(coerce=True)

    handling_equipment_availability: Series[float] = pa.Field(isin=[0, 1], coerce=True, nullable=True)
    cargo_condition_status: Series[str] = pa.Field(nullable=True)

    class Config:
        coerce = True
        strict = False # Allow extra columns

    @classmethod
    def get_continuous_features(cls) -> List[str]:
        """Return names of continuous numeric features."""
        return [
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
        ]

    @classmethod
    def get_binary_features(cls) -> List[str]:
        """Return names of binary features."""
        return [
            "handling_equipment_availability",
            # order_fulfillment_status is target, cargo_condition_status is categorical string
        ]
