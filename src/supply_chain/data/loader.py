from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pandas as pd

from supply_chain.logging_config import get_logger

logger = get_logger(__name__)


class CSVDataLoader:
    """Load tabular logistics data from a CSV file."""

    def __init__(
        self,
        path: Path,
        *,
        sep: str = ",",
        encoding: str = "utf-8",
        dtype_overrides: Mapping[str, Any] | None = None,
    ) -> None:
        self._path = Path(path)
        self._sep = sep
        self._encoding = encoding
        self._dtype_overrides = dict(dtype_overrides) if dtype_overrides else {}

    @property
    def path(self) -> Path:

        return self._path

    def load(self) -> pd.DataFrame:

        if not self._path.is_file():
            raise FileNotFoundError(f"CSV file does not exist: {self._path}")

        logger.info("Loading CSV from %s", self._path)

        df = pd.read_csv(
            self._path,
            sep=self._sep,
            encoding=self._encoding,
            dtype=self._dtype_overrides or None,
            low_memory=False,
        )

        logger.info("Loaded data with shape: %s", df.shape)
        return df
