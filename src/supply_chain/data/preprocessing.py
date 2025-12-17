from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Tuple

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from supply_chain.logging_config import get_logger
from supply_chain.schemas import SupplyChainSchema

logger = get_logger(__name__)


@dataclass
class PreprocessingConfig:
    schema: SupplyChainSchema
    numeric_imputation_strategy: str = "median"
    binary_imputation_strategy: str = "most_frequent"


class TabularPreprocessor:
    """Imputation and scaling for numeric and binary features.

    This class wraps a scikit-learn pipeline and exposes a small, typed
    interface suitable for batch preprocessing for ML pipelines in later sprints.
    """

    def __init__(self, config: PreprocessingConfig) -> None:
        self._config = config
        self._pipeline: ColumnTransformer | None = None
        self._feature_names_out: list[str] | None = None

    @property
    def feature_names_out(self) -> Tuple[str, ...]:
        if self._feature_names_out is None:
            return tuple()
        return tuple(self._feature_names_out)

    def _build_pipeline(self, df: pd.DataFrame) -> ColumnTransformer:
        numeric_features = [
            col for col in self._config.schema.get_continuous_features() if col in df.columns
        ]
        binary_features = [
            col for col in self._config.schema.get_binary_features() if col in df.columns
        ]

        numeric_pipeline = Pipeline(
            steps=[
                (
                    "imputer",
                    SimpleImputer(strategy=self._config.numeric_imputation_strategy),
                ),
                ("scaler", StandardScaler()),
            ]
        )

        binary_pipeline = Pipeline(
            steps=[
                (
                    "imputer",
                    SimpleImputer(strategy=self._config.binary_imputation_strategy),
                ),
            ]
        )

        transformers: list[tuple[str, Pipeline, Iterable[str]]] = []
        if numeric_features:
            transformers.append(("numeric", numeric_pipeline, numeric_features))
        if binary_features:
            transformers.append(("binary", binary_pipeline, binary_features))

        column_transformer = ColumnTransformer(transformers=transformers, remainder="drop")
        return column_transformer

    def fit(self, df: pd.DataFrame) -> TabularPreprocessor:
        logger.info("Fitting preprocessing pipeline on data with shape %s", df.shape)
        self._pipeline = self._build_pipeline(df)
        self._pipeline.fit(df)

        feature_names: list[str] = []
        for name, _, columns in self._pipeline.transformers_:
            if name == "remainder":
                continue
            feature_names.extend(list(columns))
        self._feature_names_out = feature_names
        logger.info("Preprocessing pipeline fitted; %d features", len(feature_names))
        return self

    def transform(self, df: pd.DataFrame) -> np.ndarray:
        if self._pipeline is None:
            raise RuntimeError("Preprocessing pipeline is not fitted. Call 'fit' first.")

        logger.info("Applying preprocessing pipeline to data with shape %s", df.shape)
        transformed = self._pipeline.transform(df)
        return transformed

    def fit_transform(self, df: pd.DataFrame) -> np.ndarray:
        self.fit(df)
        return self.transform(df)

