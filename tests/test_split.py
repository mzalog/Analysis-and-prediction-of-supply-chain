import pandas as pd
import pytest

from src.supply_chain.data.split import TimeBasedSplitter, TimeSplitConfig
from src.supply_chain.schemas import SupplyChainSchema


@pytest.fixture
def time_df():
    dates = pd.date_range(start="2023-01-01", periods=100, freq="D")
    return pd.DataFrame({
        "timestamp": dates,
        "value": range(100)
    })

def test_time_split_sizes(time_df):
    config = TimeSplitConfig(schema=SupplyChainSchema, train_frac=0.6, val_frac=0.2, persist_splits=False)
    splitter = TimeBasedSplitter(config)
    
    train, val, test = splitter.split(time_df)
    
    assert len(train) == 60
    assert len(val) == 20
    assert len(test) == 20

def test_time_split_leakage(time_df):
    config = TimeSplitConfig(schema=SupplyChainSchema, train_frac=0.6, val_frac=0.2, persist_splits=False)
    splitter = TimeBasedSplitter(config)
    
    train, val, test = splitter.split(time_df)
    
    # Ensure temporal ordering
    assert train["timestamp"].max() < val["timestamp"].min()
    assert val["timestamp"].max() < test["timestamp"].min()

def test_split_missing_column():
    df = pd.DataFrame({"other": [1, 2, 3]})
    config = TimeSplitConfig(schema=SupplyChainSchema, persist_splits=False)
    splitter = TimeBasedSplitter(config)
    
    train, val, test = splitter.split(df)
    # Should return original df as train (or handle gracefully as per implementation)
    # Implementation returns (df, empty, empty) if col missing
    assert len(train) == 3
    assert len(val) == 0
    assert len(test) == 0
