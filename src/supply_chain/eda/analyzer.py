from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from supply_chain.config import DatasetSchema
from supply_chain.logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class EDAConfig:


    figures_dir: Path
    max_univariate_plots: int = 10
    correlation_method: str = "pearson"

    def ensure_directories(self) -> None:

        self.figures_dir.mkdir(parents=True, exist_ok=True)


class ExploratoryDataAnalyzer:


    def __init__(
        self,
        df: pd.DataFrame,
        config: EDAConfig,
        dataset_schema: DatasetSchema | None = None,
    ) -> None:
        self.df = df
        self.config = config
        self.schema = dataset_schema
        self.config.ensure_directories()



    def summarize_schema(self) -> pd.DataFrame:

        series_total = len(self.df)
        summary_rows: list[dict[str, object]] = []

        for col in self.df.columns:
            col_series = self.df[col]
            missing_count = col_series.isna().sum()
            missing_ratio = (
                float(missing_count) / series_total if series_total > 0 else np.nan
            )
            unique_count = col_series.nunique(dropna=True)

            summary_rows.append(
                {
                    "column": col,
                    "dtype": str(col_series.dtype),
                    "missing_count": int(missing_count),
                    "missing_ratio": missing_ratio,
                    "unique_count": int(unique_count),
                }
            )

        summary_df = pd.DataFrame(summary_rows).sort_values(
            by="missing_ratio", ascending=False
        )

        logger.info("Generated schema summary for %d columns", len(summary_df))
        return summary_df

    def numeric_correlations(
        self,
        target_cols: Iterable[str] | None = None,
    ) -> pd.DataFrame:

        if self.schema is not None:
            numeric_cols = [
                col
                for col in self.schema.numeric_features
                if col in self.df.columns
            ]
            numeric_df = self.df[numeric_cols]
        else:
            numeric_df = self.df.select_dtypes(include=[np.number])

        if numeric_df.empty:
            logger.warning("No numeric columns available for correlation computation.")
            return pd.DataFrame()

        corr_matrix = numeric_df.corr(method=self.config.correlation_method)

        if target_cols:
            existing_targets = [c for c in target_cols if c in corr_matrix.columns]
            if not existing_targets:
                logger.warning(
                    "None of the requested target columns exist in the correlation matrix."
                )
                return corr_matrix
            corr_matrix = corr_matrix[existing_targets]

        logger.info("Computed correlation matrix with shape %s", corr_matrix.shape)
        return corr_matrix


    def plot_missing_values(self) -> Path | None:
        summary = self.summarize_schema()
        summary = summary[summary["missing_count"] > 0]
        if summary.empty:
            logger.info("No missing values to plot.")
            return None

        top = summary.head(self.config.max_univariate_plots)
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.barh(top["column"], top["missing_ratio"] * 100.0)
        ax.set_xlabel("Missing values [%]")
        ax.set_ylabel("Column")
        ax.set_title("Missing values per column")

        fig.tight_layout()
        output_path = self.config.figures_dir / "missing_values.png"
        fig.savefig(output_path)
        plt.close(fig)

        logger.info("Saved missing values plot to %s", output_path)
        return output_path

    def _numeric_columns_for_plots(self) -> Sequence[str]:

        if self.schema is not None:
            return [
                col
                for col in self.schema.numeric_features
                if col in self.df.columns
            ]
        numeric_df = self.df.select_dtypes(include=[np.number])
        return list(numeric_df.columns)

    def plot_numeric_distributions(self) -> list[Path]:
        numeric_cols = self._numeric_columns_for_plots()
        if not numeric_cols:
            logger.info("No numeric columns available for distribution plots.")
            return []

        paths: list[Path] = []
        columns_to_plot = numeric_cols[: self.config.max_univariate_plots]

        for col in columns_to_plot:
            fig, ax = plt.subplots(figsize=(8, 4))
            sns.histplot(self.df[col].dropna(), kde=True, ax=ax)
            ax.set_title(f"Distribution of {col}")
            ax.set_xlabel(col)

            fig.tight_layout()
            output_path = self.config.figures_dir / f"dist_{col}.png"
            fig.savefig(output_path)
            plt.close(fig)

            paths.append(output_path)
            logger.info("Saved distribution plot for %s to %s", col, output_path)

        return paths

    def plot_correlation_heatmap(self) -> Path | None:

        if self.schema is not None:
            numeric_cols = [
                col
                for col in self.schema.numeric_features
                if col in self.df.columns
            ]
            numeric_df = self.df[numeric_cols]
        else:
            numeric_df = self.df.select_dtypes(include=[np.number])

        if numeric_df.shape[1] < 2:
            logger.info("Not enough numeric columns for correlation heatmap.")
            return None

        corr = numeric_df.corr(method=self.config.correlation_method)

        fig, ax = plt.subplots(figsize=(10, 8))
        sns.heatmap(
            corr,
            ax=ax,
            cmap="viridis",
            annot=False,
            square=True,
        )
        ax.set_title("Correlation heatmap (numeric features)")

        fig.tight_layout()
        output_path = self.config.figures_dir / "correlation_heatmap.png"
        fig.savefig(output_path)
        plt.close(fig)

        logger.info("Saved correlation heatmap to %s", output_path)
        return output_path



    def run_basic_eda(
        self,
        target_cols: Iterable[str] | None = None,
    ) -> dict[str, object]:

        logger.info("Running basic EDA pipeline")

        schema_summary = self.summarize_schema()
        corr_matrix = self.numeric_correlations(target_cols=target_cols)

        missing_plot = self.plot_missing_values()
        dist_plots = self.plot_numeric_distributions()
        corr_plot = self.plot_correlation_heatmap()

        return {
            "schema_summary": schema_summary,
            "corr_matrix": corr_matrix,
            "missing_plot": missing_plot,
            "dist_plots": dist_plots,
            "corr_plot": corr_plot,
        }

    def export_eda_tables(
        self,
        outputs: dict[str, object],
        *,
        reports_dir: Path,
    ) -> None:


        reports_dir.mkdir(parents=True, exist_ok=True)

        schema_summary = outputs.get("schema_summary")
        if isinstance(schema_summary, pd.DataFrame):
            path = reports_dir / "schema_summary.csv"
            schema_summary.to_csv(path, index=False)
            logger.info("Exported schema summary to %s", path)

        corr_matrix = outputs.get("corr_matrix")
        if isinstance(corr_matrix, pd.DataFrame) and not corr_matrix.empty:
            path = reports_dir / "correlation_matrix.csv"
            corr_matrix.to_csv(path)
            logger.info("Exported correlation matrix to %s", path)

