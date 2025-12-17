from __future__ import annotations

import argparse
import random
from pathlib import Path

import pandas as pd

from supply_chain.config import DataPaths, DatasetSchema, DATA_RAW_DIR, REPORTS_DIR
from supply_chain.data.cleaner import DataCleaner
from supply_chain.data.loader import CSVDataLoader
from supply_chain.data.preprocessing import PreprocessingConfig, TabularPreprocessor
from supply_chain.data.split import TimeBasedSplitter, TimeSplitConfig
from supply_chain.data.time_features import TimeFeatureConfig, TimeFeatureEngineer
from supply_chain.data.validation import DataValidationConfig, DataValidator
from supply_chain.schemas import SupplyChainSchema
from supply_chain.eda.analyzer import EDAConfig, ExploratoryDataAnalyzer
from supply_chain.logging_config import get_logger, setup_logging


from supply_chain.simulation.graph import GraphBuilder
from supply_chain.simulation.engine import SimulationEngine
from supply_chain.simulation.schema import Event, EventType, NodeType
from supply_chain.simulation.visualization import SimulationVisualizer

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Sprint-2 data loading, cleaning and basic EDA."
    )

    parser.add_argument(
        "--simulate",
        action="store_true",
        help="Run the supply chain simulation instead of the ETL pipeline.",
    )
    
    parser.add_argument(
        "--live",
        action="store_true",
        help="Run the simulation with live visualization.",
    )
    
    parser.add_argument(
        "--csv-path",
        type=Path,
        default=None,
        help="Optional explicit path to the raw CSV file.",
    )
    parser.add_argument(
        "--sample-frac",
        type=float,
        default=1.0,
        help=(
            "Optional fraction of data to sample for EDA (0 < frac <= 1). "
            "Useful when experimenting with large datasets."
        ),
    )
    parser.add_argument(
        "--no-validation",
        action="store_true",
        help="Skip data-quality validation checks (Sprint 3).",
    )
    parser.add_argument(
        "--no-time-features",
        action="store_true",
        help="Skip creation of time-based features (Sprint 3).",
    )
    parser.add_argument(
        "--no-preprocessing",
        action="store_true",
        help=(
            "Skip fitting preprocessing pipeline (imputation + scaling); "
            "useful if you only want raw cleaned data and EDA."
        ),
    )
    parser.add_argument(
        "--no-time-split",
        action="store_true",
        help="Skip time-based train/val/test split (Sprint 3).",
    )
    

    parser.add_argument(
        "--tsplib",
        type=Path,
        default=None,
        help="Path to a TSPLIB file (e.g., kroA100.txt) to build the graph from.",
    )
    parser.add_argument(
        "--num-trucks",
        type=int,
        default=15,
        help="Number of trucks to spawn in the simulation (default: 15).",
    )
    parser.add_argument(
        "--num-orders",
        type=int,
        default=50,
        help="Number of orders to generate in the simulation (default: 50).",
    )
    parser.add_argument(
        "--k-neighbors",
        type=int,
        default=4,
        help="Number of nearest neighbors for graph edges (default: 4).",
    )
    return parser.parse_args()


def run_simulation(args):
    logger.info("Starting Supply Chain Simulation...")
    

    gb = GraphBuilder()
    
    if args.tsplib:
        logger.info(f"Loading graph from TSPLIB file: {args.tsplib}")
        gb.create_from_tsplib(args.tsplib, k_neighbors=args.k_neighbors)
    else:
        gb.create_random_graph(num_nodes=15, k_neighbors=3)
    
    logger.info(f"Graph created: {len(gb.nodes)} nodes, {len(gb.edges)} edges")
    

    engine = SimulationEngine(gb)
    

    valid_spawn_nodes = [
        n.id for n in gb.nodes.values() 
        if n.type not in [NodeType.CUSTOMER, NodeType.INSPECTION]
    ]
    
    if not valid_spawn_nodes:
        logger.warning("No valid spawn nodes found! Falling back to all nodes.")
        valid_spawn_nodes = list(gb.nodes.keys())
    

    all_node_ids = list(gb.nodes.keys())
    num_nodes = len(all_node_ids)
    

    num_trucks = args.num_trucks
    logger.info(f"Spawning {num_trucks} trucks...")
    for i in range(num_trucks):
        start_node = random.choice(valid_spawn_nodes)
        engine.schedule_event(Event(
            time=0.0,
            truck_id=f"T{i+1}",
            node_id=start_node,
            event_type=EventType.TRUCK_SPAWN
        ))
        

    num_orders = args.num_orders
    logger.info(f"Generating {num_orders} orders...")
    for i in range(num_orders):
        creation_time = random.uniform(0, 1200.0)
        origin = random.choice(all_node_ids)
        destination = random.choice(all_node_ids)
        while destination == origin:
            destination = random.choice(all_node_ids)
            
        engine.schedule_event(Event(
            time=creation_time,
            truck_id="SYSTEM",
            node_id=origin,
            event_type=EventType.ORDER_CREATED,
            details={
                "order_id": f"ORD{i+1}",
                "origin": origin,
                "destination": destination
            }
        ))
    
    if args.live:
        logger.info("Starting Live Visualization...")
        SimulationVisualizer.animate_simulation(engine, gb)
    else:
        duration = 24 * 60 * 7 # 1 week in minutes
        engine.run(duration)
        logger.info(f"Simulation completed. Processed {len(engine.processed_events)} events.")
        
        vis = SimulationVisualizer(engine.processed_events, gb.graph)
        

        figures_dir = Path(REPORTS_DIR) / "figures"
        figures_dir.mkdir(parents=True, exist_ok=True)
        
        vis.plot_graph(str(figures_dir / "simulation_graph.png"))
        vis.plot_event_timeline(str(figures_dir / "simulation_timeline.png"))
        
        output_csv = Path(DATA_RAW_DIR) / "simulation_events.csv"
        vis.export_events_to_csv(str(output_csv))
        logger.info(f"Results saved to {output_csv} and {figures_dir}")
        

        from supply_chain.simulation.integration import StatsCalibrator, DataConverter
        

        calibrator = StatsCalibrator(Path(DATA_RAW_DIR) / "dynamic_supply_chain_logistics_dataset.csv")
        calibrator.load_and_calibrate()
        

        df_simulated = DataConverter.events_to_dataframe(
            engine.processed_events, 
            calibrator,
            engine=engine,
            graph_builder=gb
        )
        

        simulated_data_path = Path(DATA_RAW_DIR) / "simulated_supply_chain_data.csv"
        df_simulated.to_csv(simulated_data_path, index=False)
        logger.info(f"Simulated dataset saved to {simulated_data_path} ({len(df_simulated)} rows, {len(df_simulated.columns)} columns)")
        

        logger.info(f"Columns: {list(df_simulated.columns)}")
        logger.info(f"Risk classification distribution:\n{df_simulated['risk_classification'].value_counts()}")


def main() -> None:
    setup_logging()
    
    args = parse_args()

    if args.simulate:
        run_simulation(args)
        return

    paths = DataPaths()
    schema = DatasetSchema()

    csv_path = args.csv_path or paths.raw_csv_path
    logger.info("Using CSV path: %s", csv_path)


    loader = CSVDataLoader(csv_path)
    df_raw = loader.load()

    if 0 < args.sample_frac < 1.0:
        logger.info(
            "Subsampling data to fraction=%s for EDA purposes...",
            args.sample_frac,
        )
        df_raw = df_raw.sample(
            frac=args.sample_frac,
            random_state=42,
        ).reset_index(drop=True)


    cleaner = DataCleaner(dataset_schema=schema)
    df_clean = cleaner.clean(df_raw)


    paths.interim_data_dir.mkdir(parents=True, exist_ok=True)
    interim_path = paths.interim_parquet_path
    df_clean.to_parquet(interim_path, index=False)
    logger.info("Saved cleaned data to %s", interim_path)

    if not args.no_validation:
        validation_config = DataValidationConfig(
            schema=SupplyChainSchema,
            max_missing_ratio=0.3,
        )
        validator = DataValidator(validation_config)
        validation_reports = validator.validate(df_clean)
        paths.reports_dir.mkdir(parents=True, exist_ok=True)
        missing_report = validation_reports.get("missing_values")
        if isinstance(missing_report, pd.DataFrame):
            missing_path = paths.reports_dir / "missing_values_summary.csv"
            missing_report.to_csv(missing_path, index=False)
            logger.info("Saved missing-values validation report to %s", missing_path)

    if not args.no_time_features:
        tf_config = TimeFeatureConfig(schema=schema)
        tf_engineer = TimeFeatureEngineer(tf_config)
        df_clean = tf_engineer.add_calendar_features(df_clean)
        df_clean = tf_engineer.add_lag_features(
            df_clean,
            target_column=schema.target_column,
            lags=(1, 2, 3),
        )
        df_clean = tf_engineer.add_rolling_features(
            df_clean,
            target_column=schema.target_column,
            window=3,
        )

    if not args.no_preprocessing:
        pp_config = PreprocessingConfig(schema=SupplyChainSchema)
        preprocessor = TabularPreprocessor(pp_config)
        features_array = preprocessor.fit_transform(df_clean)
        logger.info(
            "Preprocessing completed; %d numeric/binary features prepared",
            len(preprocessor.feature_names_out),
        )

        if not args.no_time_split:
            splitter = TimeBasedSplitter(TimeSplitConfig(schema=schema))

            train_df, val_df, test_df = splitter.split(df_clean)
            logger.info(
                "Time-based split completed | train=%d, val=%d, test=%d",
                len(train_df),
                len(val_df),
                len(test_df),
            )


    eda_config = EDAConfig(figures_dir=paths.figures_dir)
    analyzer = ExploratoryDataAnalyzer(df_clean, eda_config, dataset_schema=schema)

    eda_outputs = analyzer.run_basic_eda(
        target_cols=[schema.target_column],
    )


    analyzer.export_eda_tables(eda_outputs, reports_dir=paths.reports_dir)


    print("\n=== SCHEMA SUMMARY (top 20 columns by missing ratio) ===")
    schema_summary = eda_outputs["schema_summary"]
    if isinstance(schema_summary, pd.DataFrame):
        print(schema_summary.head(20))
    else:
        print("No schema summary available.")

    print("\n=== CORRELATION MATRIX (head) ===")
    corr = eda_outputs["corr_matrix"]
    if isinstance(corr, pd.DataFrame) and not corr.empty:
        print(corr.head())
    else:
        print("No numeric correlations available.")


if __name__ == "__main__":
    main()
