import pandas as pd
import pandera as pa
import pytest

from src.supply_chain.schemas import SupplyChainSchema


def get_valid_data():
    return {
        "vehicle_gps_latitude": [45.0, -33.0],
        "vehicle_gps_longitude": [12.0, 150.0],
        "fuel_consumption_rate": [10.5, 8.2],
        "eta_variation_hours": [0.1, -0.2],
        "traffic_congestion_level": [5.0, 2.0],
        "warehouse_inventory_level": [100.0, 50.0],
        "loading_unloading_time": [45.0, 30.0],
        "weather_condition_severity": [2.0, 5.0],
        "port_congestion_level": [3.0, 7.0],
        "shipping_costs": [500.0, 450.0],
        "supplier_reliability_score": [8.0, 9.0],
        "lead_time_days": [10.0, 12.0],
        "historical_demand": [1000.0, 1200.0],
        "iot_temperature": [20.0, 22.0],
        "route_risk_level": [1.0, 2.0],
        "customs_clearance_time": [2.0, 3.0],
        "driver_behavior_score": [9.0, 8.0],
        "fatigue_monitoring_score": [1.0, 2.0],
        "disruption_likelihood_score": [0.1, 0.2],
        "delay_probability": [0.1, 0.2],
        "delivery_time_deviation": [0.0, 1.0],
        "handling_equipment_availability": [1.0, 0.0],
        "order_fulfillment_status": ["On Time", "Delayed"],
        "cargo_condition_status": ["Safe", "Damaged"],
        # "timestamp": pd.to_datetime(["2023-01-01", "2023-01-02"]) # Not in schema validation
    }

def test_valid_schema():
    """Test that valid data passes schema validation."""
    data = get_valid_data()
    df = pd.DataFrame(data)
    validated_df = SupplyChainSchema.validate(df)
    assert isinstance(validated_df, pd.DataFrame)

def test_invalid_range():
    """Test that out-of-range values raise SchemaErrors."""
    data = get_valid_data()
    data["vehicle_gps_latitude"] = [100.0, 100.0] # Invalid > 90
    df = pd.DataFrame(data)
    with pytest.raises(pa.errors.SchemaError):
        SupplyChainSchema.validate(df)

def test_invalid_categorical():
    """Test that invalid categorical values raise SchemaErrors."""
    data = get_valid_data()
    data["handling_equipment_availability"] = [2.0, 2.0] # Invalid
    df = pd.DataFrame(data)
    with pytest.raises(pa.errors.SchemaError):
        SupplyChainSchema.validate(df)

def test_coerce_types():
    """Test that types are coerced if possible."""
    data = get_valid_data()
    data["fuel_consumption_rate"] = ["10.5", "8.2"] # String
    df = pd.DataFrame(data)
    validated_df = SupplyChainSchema.validate(df)
    assert pd.api.types.is_float_dtype(validated_df["fuel_consumption_rate"])
