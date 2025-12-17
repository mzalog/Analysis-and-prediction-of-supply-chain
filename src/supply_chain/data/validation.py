from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import pandas as pd
import pandera as pa

from supply_chain.logging_config import get_logger
from supply_chain.schemas import SupplyChainSchema

logger = get_logger(__name__)


@dataclass
class DataValidationConfig:
    schema: type[pa.DataFrameModel] | None = None
    max_missing_ratio: float = 0.3


class DataValidator:


    def __init__(self, config: DataValidationConfig | None = None) -> None:
        self._config = config or DataValidationConfig(schema=SupplyChainSchema)

    def validate(self, df: pd.DataFrame) -> Mapping[str, pd.DataFrame]:

        logger.info("Running data validation checks")

        reports: dict[str, pd.DataFrame] = {}


        reports["missing_values"] = self._check_missing_values(df)

        if self._config.schema is not None:
            schema_report = self._check_schema_pandera(df)
            if schema_report is not None:
                reports["schema_validation"] = schema_report

        logger.info("Data validation finished")
        return reports

    def _check_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        total = len(df)
        rows = []
        for col in df.columns:
            missing_count = int(df[col].isna().sum())
            missing_ratio = float(missing_count) / total if total > 0 else 0.0
            above_threshold = missing_ratio > self._config.max_missing_ratio
            rows.append({
                "column": col,
                "missing_count": missing_count,
                "missing_ratio": missing_ratio,
                "above_threshold": above_threshold
            })
        
        report = pd.DataFrame(rows).sort_values(by="missing_ratio", ascending=False)
        
        n_flagged = int(report["above_threshold"].sum())
        if n_flagged:
            logger.warning(
                "Missing-values check: %d columns exceed threshold %.2f",
                n_flagged,
                self._config.max_missing_ratio,
            )
        return report

    def _check_schema_pandera(self, df: pd.DataFrame) -> pd.DataFrame | None:
        if self._config.schema is None:
            return None
            
        try:
            self._config.schema.validate(df, lazy=True)
            return None
        except pa.errors.SchemaErrors as err:
            logger.warning("Pandera schema validation failed with %d errors", len(err.failure_cases))
            return err.failure_cases

    def _check_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        n_duplicates = df.duplicated().sum()
        if n_duplicates > 0:
            logger.warning("Duplicate check: found %d duplicate rows", n_duplicates)
        else:
            logger.info("Duplicate check: no duplicate rows found")
        
        return pd.DataFrame([{"n_duplicates": n_duplicates, "status": "failed" if n_duplicates > 0 else "passed"}])
