from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from supply_chain.config import DatasetSchema
from supply_chain.logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class TimeFeatureConfig:
    schema: DatasetSchema
    max_lag_hours: int = 24


class TimeFeatureEngineer:


    def __init__(self, config: TimeFeatureConfig) -> None:
        self._config = config

    def add_calendar_features(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        ts_col = self._config.schema.datetime_column

        if ts_col not in df.columns:
            logger.warning(
                "Cannot create calendar features – datetime column '%s' missing",
                ts_col,
            )
            return df

        if not pd.api.types.is_datetime64_any_dtype(df[ts_col]):
            logger.info("Converting '%s' to datetime for calendar features", ts_col)
            df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce", utc=True)

        df["hour_of_day"] = df[ts_col].dt.hour
        df["day_of_week"] = df[ts_col].dt.dayofweek
        df["day_of_month"] = df[ts_col].dt.day
        df["week_of_year"] = df[ts_col].dt.isocalendar().week.astype("Int64")

        logger.info("Added basic calendar time features")
        return df

    def add_lag_features(
        self,
        df: pd.DataFrame,
        target_column: str,
        *,
        groupby_column: str | None = None,
        lags: tuple[int, ...] = (1, 2, 3),
    ) -> pd.DataFrame:
        df = df.copy()
        ts_col = self._config.schema.datetime_column

        if ts_col not in df.columns or target_column not in df.columns:
            logger.warning(
                "Cannot create lag features – required columns missing: %s, %s",
                ts_col,
                target_column,
            )
            return df

        df = df.sort_values(by=[c for c in ([groupby_column, ts_col]) if c is not None])

        for lag in lags:
            col_name = f"{target_column}_lag_{lag}"
            if groupby_column is None:
                df[col_name] = df[target_column].shift(lag)
            else:
                df[col_name] = df.groupby(groupby_column)[target_column].shift(lag)

        logger.info("Added %d lag features for target '%s'", len(lags), target_column)
        return df

    def add_rolling_features(
        self,
        df: pd.DataFrame,
        target_column: str,
        *,
        groupby_column: str | None = None,
        window: int = 3,
    ) -> pd.DataFrame:
        df = df.copy()
        ts_col = self._config.schema.datetime_column

        if ts_col not in df.columns or target_column not in df.columns:
            logger.warning(
                "Cannot create rolling features – required columns missing: %s, %s",
                ts_col,
                target_column,
            )
            return df

        df = df.sort_values(by=[c for c in ([groupby_column, ts_col]) if c is not None])

        if groupby_column is None:
            rolling = df[target_column].rolling(window=window, min_periods=1)
        else:
            rolling = (
                df.groupby(groupby_column)[target_column]
                .rolling(window=window, min_periods=1)
                .reset_index(level=0, drop=True)
            )

        df[f"{target_column}_roll_mean_{window}"] = rolling.mean()
        df[f"{target_column}_roll_std_{window}"] = rolling.std()

        logger.info(
            "Added rolling-window features (window=%d) for target '%s'",
            window,
            target_column,
        )
        return df

