from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from supply_chain.config import DatasetSchema
from supply_chain.logging_config import get_logger

logger = get_logger(__name__)


class DataCleaner:


    def __init__(
        self,
        dataset_schema: DatasetSchema | None = None,
        *,
        id_columns: Iterable[str] | None = None,
    ) -> None:
        self._schema = dataset_schema
        self._id_columns = set(id_columns or [])

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:

        logger.info("Starting basic data cleaning")
        working_df = df.copy()

        working_df = self._standardize_column_names(working_df)
        working_df = self._apply_schema_types(working_df)
        working_df = self._strip_string_columns(working_df)
        working_df = self._drop_obvious_duplicates(working_df)

        logger.info("Finished cleaning | final shape: %s", working_df.shape)
        return working_df



    def _standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:

        df = df.copy()


        if self._schema is not None:
            rename_map = {
                col: self._schema.column_renames[col]
                for col in df.columns
                if col in self._schema.column_renames
            }
            if rename_map:
                logger.info("Applying explicit column renames: %s", rename_map)
                df = df.rename(columns=rename_map)

        def _normalize(name: str) -> str:

            name = name.strip()
            name = name.replace("%", "pct")
            for ch in [" ", "-", "/", ".", "(", ")", "[", "]"]:
                name = name.replace(ch, "_")
            while "__" in name:
                name = name.replace("__", "_")
            return name.lower()

        df.columns = [_normalize(col) for col in df.columns]
        return df

    def _apply_schema_types(self, df: pd.DataFrame) -> pd.DataFrame:

        if self._schema is None:
            logger.info("No DatasetSchema provided; skipping schema-based typing.")
            return df

        df = df.copy()


        dt_col = self._schema.datetime_column
        if dt_col in df.columns:
            logger.info("Converting '%s' to datetime (UTC)...", dt_col)
            df[dt_col] = pd.to_datetime(df[dt_col], errors="coerce", utc=True)
        else:
            logger.warning(
                "Expected datetime column '%s' not found in DataFrame.", dt_col
            )


        for col in self._schema.numeric_features:
            if col not in df.columns:
                logger.debug("Numeric column '%s' not found; skipping.", col)
                continue
            df[col] = pd.to_numeric(df[col], errors="coerce")


        for col in self._schema.binary_features:
            if col not in df.columns:
                continue
            series = pd.to_numeric(df[col], errors="coerce")
            series = series.round().clip(lower=0, upper=1)
            df[col] = series.astype("Int8")

        return df

    def _strip_string_columns(self, df: pd.DataFrame) -> pd.DataFrame:

        df = df.copy()
        object_cols = df.select_dtypes(include=["object", "string"]).columns
        for col in object_cols:
            df[col] = df[col].astype("string").str.strip()
        return df

    def _drop_obvious_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:

        df = df.copy()
        before = len(df)

        if self._id_columns and self._id_columns.issubset(df.columns):
            df = df.drop_duplicates(subset=list(self._id_columns))
        else:
            df = df.drop_duplicates()

        after = len(df)
        logger.info("Dropped %d duplicate rows", before - after)
        return df
