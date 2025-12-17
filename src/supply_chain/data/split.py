from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

import pandas as pd

from supply_chain.config import DataPaths
from supply_chain.logging_config import get_logger
from supply_chain.schemas import SupplyChainSchema

logger = get_logger(__name__)


@dataclass
class TimeSplitConfig:
    schema: SupplyChainSchema
    train_frac: float = 0.6
    val_frac: float = 0.2
    persist_splits: bool = True

    def __post_init__(self) -> None:
        if not (0.0 < self.train_frac < 1.0 and 0.0 < self.val_frac < 1.0):
            raise ValueError("Fractions must be in (0, 1)")
        if self.train_frac + self.val_frac >= 1.0:
            raise ValueError("train_frac + val_frac must be < 1.0")


class TimeBasedSplitter:
    """Perform simple time-ordered train/val/test split.

    The split is based solely on the configured datetime column and preserves
    temporal ordering to reduce the risk of data leakage.
    """

    def __init__(self, config: TimeSplitConfig) -> None:
        self._config = config

    def split(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        ts_col = "timestamp" # Hardcoded as per schema definition

        if ts_col not in df.columns:
            logger.warning(
                "Time-based split skipped: datetime column '%s' not present.", ts_col
            )
            return df, df.iloc[0:0].copy(), df.iloc[0:0].copy()

        if not pd.api.types.is_datetime64_any_dtype(df[ts_col]):
            logger.info("Converting '%s' to datetime for time split", ts_col)
            df = df.copy()
            df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce", utc=True)

        df_sorted = df.sort_values(by=ts_col).reset_index(drop=True)
        n = len(df_sorted)
        n_train = int(n * self._config.train_frac)
        n_val = int(n * self._config.val_frac)

        train = df_sorted.iloc[:n_train].copy()
        val = df_sorted.iloc[n_train : n_train + n_val].copy()
        test = df_sorted.iloc[n_train + n_val :].copy()

        logger.info(
            "Time-based split sizes | train=%d, val=%d, test=%d (total=%d)",
            len(train),
            len(val),
            len(test),
            n,
        )

        # Simple leakage sanity check: ensure max(train) <= min(val) <= min(test)
        max_train = train[ts_col].max() if not train.empty else None
        min_val = val[ts_col].min() if not val.empty else None
        min_test = test[ts_col].min() if not test.empty else None

        logger.info(
            "Time boundaries | max_train=%s, min_val=%s, min_test=%s",
            max_train,
            min_val,
            min_test,
        )

        if self._config.persist_splits:
            self._persist_splits(train, val, test)

        return train, val, test

    def _persist_splits(
        self,
        train: pd.DataFrame,
        val: pd.DataFrame,
        test: pd.DataFrame,
    ) -> None:


        paths = DataPaths()
        out_dir = paths.processed_data_dir
        out_dir.mkdir(parents=True, exist_ok=True)

        train_path = out_dir / "train.parquet"
        val_path = out_dir / "val.parquet"
        test_path = out_dir / "test.parquet"

        train.to_parquet(train_path, index=False)
        val.to_parquet(val_path, index=False)
        test.to_parquet(test_path, index=False)

        logger.info("Saved train split to %s", train_path)
        logger.info("Saved validation split to %s", val_path)
        logger.info("Saved test split to %s", test_path)
