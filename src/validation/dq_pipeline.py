"""
Data Quality Pipeline Runner Module for DEPI Data Quality ETL Pipeline Platform.

Orchestrates batch data quality validation for all Bronze data sources:
- postgres.customers
- postgres.products
- postgres.orders
- csv_files.shipments
- rest_api.products

Writes:
- Valid records to Silver (Parquet format)
- Rejected records to Quarantine (Parquet format with run_id, rejection_reason, rejection_timestamp)
- Quality Reports in both JSON and Parquet formats
"""

import glob
import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.validation.contracts import CONTRACTS, get_contract
from src.validation.quality_engine import DataQualityEngine

# Setup module logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("DataQualityPipeline")


# Mapping of dataset names to their Bronze source subdirectories and file patterns
DATASET_SOURCE_MAP = {
    "postgres.customers": {
        "source_subdir": os.path.join("postgres", "customers"),
        "file_prefix": "customers_",
        "extension": "csv",
        "clean_name": "customers",
    },
    "postgres.products": {
        "source_subdir": os.path.join("postgres", "products"),
        "file_prefix": "products_",
        "extension": "csv",
        "clean_name": "products",
    },
    "postgres.orders": {
        "source_subdir": os.path.join("postgres", "orders"),
        "file_prefix": "orders_",
        "extension": "csv",
        "clean_name": "orders",
    },
    "csv_files.shipments": {
        "source_subdir": os.path.join("csv_files", "shipments"),
        "file_prefix": "shipments_",
        "extension": "csv",
        "clean_name": "shipments",
    },
    "rest_api.products": {
        "source_subdir": os.path.join("rest_api", "products"),
        "file_prefix": "products_",
        "extension": "json",
        "clean_name": "api_products",
    },
}

# Execution dependency order: Parents before dependents
PROCESSING_ORDER = [
    "postgres.customers",
    "postgres.products",
    "postgres.orders",
    "csv_files.shipments",
    "rest_api.products",
]


class DataQualityPipeline:
    """Production-oriented Data Quality Pipeline orchestrator."""

    def __init__(
        self,
        spark: SparkSession,
        bronze_dir: str = "bronze",
        silver_dir: str = "silver",
        rejected_dir: str = "rejected",
        reports_dir: str = "quality_reports",
        pipeline_name: str = "data_quality_etl_pipeline",
    ):
        self.spark = spark
        self.bronze_dir = bronze_dir
        self.silver_dir = silver_dir
        self.rejected_dir = rejected_dir
        self.reports_dir = reports_dir
        self.pipeline_name = pipeline_name

    @staticmethod
    def extract_run_id_from_filename(filepath: str) -> Optional[str]:
        """
        Extracts run_id from filename pattern <name>_<run_id>.<ext>.
        Example: customers_a1b2c3d4.csv -> a1b2c3d4
        """
        basename = os.path.basename(filepath)
        name_part, _ = os.path.splitext(basename)
        if "_" in name_part:
            return name_part.rsplit("_", 1)[-1]
        return None

    def discover_bronze_runs(self) -> List[str]:
        """Discovers all distinct run_ids present in the bronze directory."""
        run_ids = set()
        pattern = os.path.join(self.bronze_dir, "**", "*.*")
        for filepath in glob.glob(pattern, recursive=True):
            if os.path.isfile(filepath):
                rid = self.extract_run_id_from_filename(filepath)
                if rid:
                    run_ids.add(rid)
        return sorted(list(run_ids))

    def find_bronze_file(
        self, dataset_name: str, run_id: Optional[str] = None
    ) -> Optional[str]:
        """Finds the bronze file for a dataset, optionally filtered by run_id."""
        if dataset_name not in DATASET_SOURCE_MAP:
            raise KeyError(f"Unknown dataset: {dataset_name}")

        info = DATASET_SOURCE_MAP[dataset_name]
        folder = os.path.join(self.bronze_dir, info["source_subdir"])
        if not os.path.exists(folder):
            return None

        ext = info["extension"]
        prefix = info["file_prefix"]
        if run_id:
            pattern = os.path.join(folder, "**", f"{prefix}{run_id}.{ext}")
        else:
            pattern = os.path.join(folder, "**", f"{prefix}*.{ext}")

        matches = glob.glob(pattern, recursive=True)
        if not matches:
            return None

        # Return the newest file if multiple matches exist
        matches.sort(key=os.path.getmtime, reverse=True)
        return matches[0]

    def read_bronze_dataset(
        self, dataset_name: str, file_path: str
    ) -> DataFrame:
        """
        Reads a Bronze dataset from file_path into a PySpark DataFrame.
        Handles both CSV and JSON (flattening nested API pages if needed).
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Bronze file not found: {file_path}")

        info = DATASET_SOURCE_MAP[dataset_name]
        ext = info["extension"]

        if ext == "csv":
            # Read CSV with header as strings to allow safe validation
            df = (
                self.spark.read.option("header", "true")
                .option("inferSchema", "false")
                .csv(file_path)
            )
            return df

        elif ext == "json":
            # REST API products are stored as JSON array of page objects:
            # [{"products": [...], "total": ..., ...}, ...]
            raw_json_df = (
                self.spark.read.option("multiline", "true").json(file_path)
            )

            # Check if JSON contains a 'products' array
            if "products" in raw_json_df.columns:
                # Explode the products array to get individual product rows
                flattened_df = raw_json_df.select(
                    F.explode("products").alias("p")
                ).select("p.*")
                return flattened_df
            else:
                return raw_json_df
        else:
            raise ValueError(f"Unsupported file format: {ext}")

    def prepare_and_cast_dataset(
        self, dataset_name: str, df: DataFrame
    ) -> Tuple[DataFrame, List[Tuple[Any, str]]]:
        """
        Applies safe casting based on the dataset's DataContract.
        Returns: (casted_df, custom_schema_error_rules)
        """
        contract = get_contract(dataset_name)
        target_schema = contract.expected_schema

        parsed_df, parse_error_expressions = DataQualityEngine.safe_cast(
            df, target_schema
        )

        custom_rules = []
        for col_name, target_type in target_schema.items():
            err_col = f"_dq_cast_err_{col_name}"
            if err_col in parsed_df.columns:
                cond = F.col(err_col).isNotNull()
                msg = f"Cannot parse '{col_name}' as {target_type}"
                custom_rules.append((cond, msg))

        return parsed_df, custom_rules

    def save_silver(
        self, dataset_name: str, df: DataFrame, run_id: str
    ) -> str:
        """Writes valid records to Silver directory in Parquet format."""
        clean_name = DATASET_SOURCE_MAP[dataset_name]["clean_name"]
        output_dir = os.path.join(self.silver_dir, clean_name, f"run_id={run_id}")
        os.makedirs(output_dir, exist_ok=True)

        # Write Parquet idempotently
        df.write.mode("overwrite").parquet(output_dir)
        logger.info(
            f"Silver: Saved {df.count()} valid records to {output_dir}"
        )
        return output_dir

    def save_quarantine(
        self, dataset_name: str, df: DataFrame, run_id: str
    ) -> str:
        """Writes rejected records to Quarantine directory in Parquet format."""
        clean_name = DATASET_SOURCE_MAP[dataset_name]["clean_name"]
        output_dir = os.path.join(
            self.rejected_dir, clean_name, f"run_id={run_id}"
        )
        os.makedirs(output_dir, exist_ok=True)

        # Write Parquet idempotently
        df.write.mode("overwrite").parquet(output_dir)
        logger.info(
            f"Quarantine: Saved {df.count()} rejected records to {output_dir}"
        )
        return output_dir

    def save_reports(
        self, reports_df: DataFrame, run_id: str
    ) -> Tuple[str, str]:
        """Saves Quality Reports in both JSON and Parquet formats."""
        run_report_dir = os.path.join(self.reports_dir, f"run_id={run_id}")
        os.makedirs(run_report_dir, exist_ok=True)

        parquet_path = os.path.join(run_report_dir, "quality_report.parquet")
        json_path = os.path.join(run_report_dir, "quality_report.json")

        # 1. Save as Parquet
        reports_df.write.mode("overwrite").parquet(parquet_path)

        # 2. Save as JSON
        records = [row.asDict() for row in reports_df.collect()]
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(records, f, indent=2)

        logger.info(f"Reports saved to: {parquet_path} and {json_path}")
        return parquet_path, json_path

    def process_dataset(
        self,
        dataset_name: str,
        df: DataFrame,
        run_id: str,
        reference_dfs: Optional[Dict[str, DataFrame]] = None,
    ) -> Tuple[DataFrame, DataFrame, DataFrame]:
        """
        Validates a single dataset using its contract and DataQualityEngine.
        Returns: (valid_records, rejected_records, quality_report)
        """
        contract = get_contract(dataset_name)
        engine = DataQualityEngine(
            run_id=run_id,
            pipeline_name=self.pipeline_name,
            task_name=f"validate_{DATASET_SOURCE_MAP[dataset_name]['clean_name']}",
        )

        # Apply safe type casting first
        casted_df, custom_rules = self.prepare_and_cast_dataset(
            dataset_name, df
        )

        validation_kwargs = contract.get_validation_kwargs(reference_dfs)
        validation_kwargs["custom_rules"] = custom_rules

        valid_df, rejected_df, report_df = engine.validate(
            casted_df, **validation_kwargs
        )

        # Clean up any internal cast error columns from valid DataFrame
        drop_cols = [c for c in valid_df.columns if c.startswith("_dq_cast_err_")]
        if drop_cols:
            valid_df = valid_df.drop(*drop_cols)
        if drop_cols:
            rejected_df = rejected_df.drop(*drop_cols)

        # Persist Silver and Quarantine
        self.save_silver(dataset_name, valid_df, run_id)
        self.save_quarantine(dataset_name, rejected_df, run_id)

        return valid_df, rejected_df, report_df

    def run_pipeline(
        self,
        run_id: Optional[str] = None,
        datasets_override: Optional[Dict[str, DataFrame]] = None,
    ) -> Dict[str, Any]:
        """
        Runs the complete Data Quality Pipeline across all datasets in dependency order.
        Supports passing an explicit run_id or processing provided in-memory DataFrames.
        """
        start_time = datetime.now(timezone.utc)
        resolved_run_id = run_id or (
            self.discover_bronze_runs()[-1]
            if self.discover_bronze_runs()
            else datetime.now().strftime("%Y%m%d%H%M%S")
        )

        logger.info(
            f"Starting Data Quality Pipeline run: {resolved_run_id}"
        )

        all_reports: List[DataFrame] = []
        silver_references: Dict[str, DataFrame] = {}
        dataset_summaries: Dict[str, Dict[str, Any]] = {}

        for dataset_name in PROCESSING_ORDER:
            logger.info(f"--- Processing {dataset_name} ---")

            if datasets_override and dataset_name in datasets_override:
                df = datasets_override[dataset_name]
            else:
                file_path = self.find_bronze_file(
                    dataset_name, run_id=resolved_run_id
                )
                if not file_path:
                    logger.warning(
                        f"No Bronze file found for {dataset_name} (run_id={resolved_run_id}). Skipping."
                    )
                    continue
                logger.info(f"Reading {dataset_name} from {file_path}")
                df = self.read_bronze_dataset(dataset_name, file_path)

            # Validate dataset
            valid_df, rejected_df, report_df = self.process_dataset(
                dataset_name=dataset_name,
                df=df,
                run_id=resolved_run_id,
                reference_dfs=silver_references,
            )

            # Store valid records in silver_references for downstream referential checks
            silver_references[dataset_name] = valid_df
            all_reports.append(report_df)

            dataset_summaries[dataset_name] = {
                "records_checked": df.count(),
                "records_valid": valid_df.count(),
                "records_rejected": rejected_df.count(),
            }

        # Combine all dataset quality reports
        if all_reports:
            combined_report_df = all_reports[0]
            for r_df in all_reports[1:]:
                combined_report_df = combined_report_df.unionByName(r_df)

            parquet_path, json_path = self.save_reports(
                combined_report_df, resolved_run_id
            )
        else:
            combined_report_df = None
            parquet_path, json_path = None, None

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        logger.info(
            f"Data Quality Pipeline run {resolved_run_id} completed in {duration:.2f}s"
        )

        return {
            "run_id": resolved_run_id,
            "status": "SUCCESS",
            "duration_seconds": duration,
            "datasets": dataset_summaries,
            "report_parquet": parquet_path,
            "report_json": json_path,
            "reports_df": combined_report_df,
        }


def main():
    """CLI entrypoint for running the Data Quality Pipeline."""
    import sys

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("DataQualityPipelineCLI")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    target_run_id = sys.argv[1] if len(sys.argv) > 1 else None
    pipeline = DataQualityPipeline(spark=spark)
    results = pipeline.run_pipeline(run_id=target_run_id)

    print("\n=== PIPELINE EXECUTION SUMMARY ===")
    print(f"Run ID: {results['run_id']}")
    print(f"Status: {results['status']}")
    print(f"Duration: {results['duration_seconds']:.2f}s")
    for ds, stats in results["datasets"].items():
        print(
            f"- {ds}: checked={stats['records_checked']}, valid={stats['records_valid']}, rejected={stats['records_rejected']}"
        )
    print(f"Report JSON: {results['report_json']}")
    print(f"Report Parquet: {results['report_parquet']}")

    spark.stop()


if __name__ == "__main__":
    main()
