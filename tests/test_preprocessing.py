import numpy as np
import pandas as pd
import pytest

from src.supply_chain.data.preprocessing import PreprocessingConfig, TabularPreprocessor
from src.supply_chain.schemas import SupplyChainSchema


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "vehicle_gps_latitude": [10.0, 20.0, np.nan],
        "handling_equipment_availability": [1.0, 0.0, np.nan],
        "extra_col": ["a", "b", "c"]
    })

def test_preprocessor_fit_transform(sample_df):
    config = PreprocessingConfig(schema=SupplyChainSchema)
    preprocessor = TabularPreprocessor(config)
    
    # Fit
    preprocessor.fit(sample_df)
    # latitude + handling = 2 features
    assert len(preprocessor.feature_names_out) == 2 
    
    # Transform
    X = preprocessor.transform(sample_df)
    assert X.shape == (3, 2)
    assert not np.isnan(X).any() # Should be imputed

def test_preprocessor_feature_names(sample_df):
    config = PreprocessingConfig(schema=SupplyChainSchema)
    preprocessor = TabularPreprocessor(config)
    preprocessor.fit(sample_df)
    
    names = preprocessor.feature_names_out
    assert "vehicle_gps_latitude" in names
    assert "handling_equipment_availability" in names
    assert "extra_col" not in names
