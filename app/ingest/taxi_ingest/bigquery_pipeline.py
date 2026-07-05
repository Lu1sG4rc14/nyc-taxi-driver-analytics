"""BigQuery and Cloud Storage operations for the taxi analytics pipeline.

Created: 2026-07-05
Author: Luis G (https://github.com/Lu1sG4rc14)
"""

from __future__ import annotations

import csv
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import requests
from google.cloud import bigquery, storage

from taxi_ingest.config import PipelineConfig


class BigQueryPipeline:
    """Wrapper around GCP clients and SQL execution for one pipeline run."""

    def __init__(self, config: PipelineConfig) -> None:
        """Initializes BigQuery, Cloud Storage, and SQL directory settings.

        Args:
            config: Runtime pipeline configuration.
        """
        self.config = config
        self.bq = bigquery.Client(project=config.project_id, location=config.bigquery_location)
        self.storage = storage.Client(project=config.project_id)
        self.sql_dir = Path(__import__("os").getenv("SQL_DIR", "sql"))

    def run_setup(self) -> None:
        """Creates required BigQuery tables when they do not exist."""
        self.run_sql_file("setup/create_tables.sql")

    def refresh_zone_lookup(self) -> None:
        """Downloads and reloads the taxi zone lookup reference table.

        The lookup is small and public, so the pipeline reloads it with
        `WRITE_TRUNCATE` before transformations to keep reference data current.

        Raises:
            requests.HTTPError: If the lookup CSV cannot be downloaded.
            google.api_core.GoogleAPIError: If the GCS upload or BigQuery load
                job fails.
        """
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "taxi_zone_lookup.csv"
            response = requests.get(self.config.zone_lookup_url, timeout=60)
            response.raise_for_status()
            csv_path.write_bytes(response.content)

            normalized_path = Path(tmp) / "taxi_zone_lookup_normalized.csv"
            with csv_path.open("r", encoding="utf-8-sig", newline="") as source, normalized_path.open(
                "w", encoding="utf-8", newline=""
            ) as target:
                reader = csv.DictReader(source)
                writer = csv.DictWriter(
                    target,
                    fieldnames=["location_id", "borough", "zone", "service_zone", "meta_loaded_at"],
                )
                writer.writeheader()
                loaded_at = datetime.now(timezone.utc).isoformat()
                for row in reader:
                    writer.writerow(
                        {
                            "location_id": row["LocationID"],
                            "borough": row["Borough"],
                            "zone": row["Zone"],
                            "service_zone": row["service_zone"],
                            "meta_loaded_at": loaded_at,
                        }
                    )

            uri = self.upload_file(
                normalized_path,
                self.config.raw_bucket,
                "reference/taxi_zone_lookup/taxi_zone_lookup_normalized.csv",
            )

        table_id = f"{self.config.project_id}.{self.config.bronze_dataset}.taxi_zone_lookup"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=[
                bigquery.SchemaField("location_id", "INT64"),
                bigquery.SchemaField("borough", "STRING"),
                bigquery.SchemaField("zone", "STRING"),
                bigquery.SchemaField("service_zone", "STRING"),
                bigquery.SchemaField("meta_loaded_at", "TIMESTAMP"),
            ],
        )
        self.bq.load_table_from_uri(uri, table_id, job_config=job_config).result()

    def latest_success(self, source_month: str) -> dict[str, Any] | None:
        """Fetches the latest successful manifest row for a source month.

        Args:
            source_month: Month in `YYYY-MM` format.

        Returns:
            Manifest row as a dictionary, or `None` when the month has no
            successful load yet.
        """
        query = f"""
        SELECT *
        FROM `{self.config.project_id}.{self.config.ops_dataset}.ingestion_manifest`
        WHERE source_name = 'yellow_tripdata'
          AND source_month = @source_month
          AND status = 'SUCCESS'
        ORDER BY completed_at DESC
        LIMIT 1
        """
        job = self.bq.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "source_month",
                        "DATE",
                        date.fromisoformat(f"{source_month}-01"),
                    )
                ]
            ),
        )
        rows = list(job.result())
        return dict(rows[0]) if rows else None

    def insert_manifest(self, row: dict[str, Any]) -> None:
        """Appends one run manifest row to the ops dataset.

        Args:
            row: Manifest row shaped for `L100_ops.ingestion_manifest`.

        Raises:
            RuntimeError: If BigQuery returns insert errors.
        """
        table_id = f"{self.config.project_id}.{self.config.ops_dataset}.ingestion_manifest"
        errors = self.bq.insert_rows_json(table_id, [row])
        if errors:
            raise RuntimeError(f"Failed to insert manifest row: {errors}")

    def upload_file(self, path: Path, bucket_name: str, object_name: str) -> str:
        """Uploads a local file to Cloud Storage.

        Args:
            path: Local file path to upload.
            bucket_name: Destination bucket.
            object_name: Destination object name.

        Returns:
            `gs://` URI for the uploaded object.
        """
        bucket = self.storage.bucket(bucket_name)
        blob = bucket.blob(object_name)
        blob.upload_from_filename(str(path))
        return f"gs://{bucket_name}/{object_name}"

    def load_parquet_to_staging(self, uri: str, staging_table: str) -> None:
        """Loads a generated Parquet file into a transient staging table.

        Args:
            uri: Source `gs://` URI.
            staging_table: Temporary table name in the staging dataset.
        """
        table_id = f"{self.config.project_id}.{self.config.staging_dataset}.{staging_table}"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        self.bq.load_table_from_uri(uri, table_id, job_config=job_config).result()

    def apply_append_delta(self, staging_table: str, source_month: str) -> None:
        """Appends staged delta rows to bronze and refreshes analytics tables.

        Args:
            staging_table: Transient staging table that contains new rows.
            source_month: Month being processed in `YYYY-MM` format.
        """
        self.run_sql_file(
            "bronze/append_bronze_from_staging.sql",
            staging_table=staging_table,
            source_month=source_month,
        )
        self.rebuild_analytics_month(source_month)

    def apply_rebuild_month(self, staging_table: str, source_month: str) -> None:
        """Replaces one month in bronze and refreshes analytics tables.

        Args:
            staging_table: Transient staging table that contains the full month.
            source_month: Month being rebuilt in `YYYY-MM` format.
        """
        self.run_sql_file(
            "bronze/rebuild_bronze_month_from_staging.sql",
            staging_table=staging_table,
            source_month=source_month,
        )
        self.rebuild_analytics_month(source_month)

    def rebuild_analytics_month(self, source_month: str) -> None:
        """Rebuilds silver and gold tables for one affected month.

        Args:
            source_month: Month to rebuild in `YYYY-MM` format.
        """
        for sql_file in [
            "silver/rebuild_fact_trips_month.sql",
            "gold/rebuild_zone_hour_earnings_month.sql",
            "gold/rebuild_airport_strategy_month.sql",
            "gold/rebuild_payment_tip_patterns_month.sql",
            "gold/rebuild_driver_recommendations_month.sql",
        ]:
            self.run_sql_file(sql_file, source_month=source_month)

    def drop_staging_table(self, staging_table: str) -> None:
        """Drops a transient staging table after a successful transformation.

        Args:
            staging_table: Table name in the staging dataset.
        """
        table_id = f"{self.config.project_id}.{self.config.staging_dataset}.{staging_table}"
        self.bq.delete_table(table_id, not_found_ok=True)

    def run_sql_file(self, relative_path: str, **extra_context: str) -> None:
        """Renders and executes one SQL file with pipeline context.

        Args:
            relative_path: SQL file path relative to `SQL_DIR`.
            **extra_context: Additional placeholders required by the SQL file.
        """
        sql = (self.sql_dir / relative_path).read_text(encoding="utf-8")
        context = {
            "project_id": self.config.project_id,
            "ops_dataset": self.config.ops_dataset,
            "staging_dataset": self.config.staging_dataset,
            "bronze_dataset": self.config.bronze_dataset,
            "silver_dataset": self.config.silver_dataset,
            "gold_dataset": self.config.gold_dataset,
            "bigquery_location": self.config.bigquery_location,
        }
        context.update(extra_context)
        rendered = sql.format(**context)
        self.bq.query(rendered).result()


def manifest_row(
    *,
    run_id: str,
    source_month: str,
    source_url: str,
    source_head: dict[str, Any],
    status: str,
    load_mode: str,
    started_at: datetime,
    completed_at: datetime,
    source_row_count: int | None = None,
    previous_row_count: int | None = None,
    delta_row_count: int | None = None,
    snapshot_hash: str | None = None,
    previous_snapshot_hash: str | None = None,
    raw_gcs_uri: str | None = None,
    delta_gcs_uri: str | None = None,
    staging_table: str | None = None,
    error_message: str | None = None,
) -> dict[str, Any]:
    """Builds a manifest row for `L100_ops.ingestion_manifest`.

    Args:
        run_id: Unique run identifier.
        source_month: Source month in `YYYY-MM` format.
        source_url: Source file URL.
        source_head: Source metadata collected from HTTP headers.
        status: Final run status such as `SUCCESS`, `FAILED`, or `SKIPPED`.
        load_mode: Load strategy used for the run.
        started_at: Run start timestamp.
        completed_at: Run completion timestamp.
        source_row_count: Current source file row count.
        previous_row_count: Previous successful source row count.
        delta_row_count: Number of rows loaded in this run.
        snapshot_hash: Current canonical snapshot hash.
        previous_snapshot_hash: Previous successful snapshot hash.
        raw_gcs_uri: GCS URI of the downloaded raw source snapshot.
        delta_gcs_uri: GCS URI of the generated delta or rebuild Parquet.
        staging_table: Transient BigQuery staging table name.
        error_message: Optional truncated error text for failed/skipped runs.

    Returns:
        Dictionary ready for BigQuery JSON insertion.
    """
    return {
        "run_id": run_id,
        "source_name": "yellow_tripdata",
        "source_month": date.fromisoformat(f"{source_month}-01").isoformat(),
        "source_url": source_url,
        "source_etag": source_head.get("etag"),
        "source_last_modified": source_head.get("last_modified"),
        "source_content_length": source_head.get("content_length"),
        "source_row_count": source_row_count,
        "previous_row_count": previous_row_count,
        "delta_row_count": delta_row_count,
        "snapshot_hash": snapshot_hash,
        "previous_snapshot_hash": previous_snapshot_hash,
        "raw_gcs_uri": raw_gcs_uri,
        "delta_gcs_uri": delta_gcs_uri,
        "staging_table": staging_table,
        "load_mode": load_mode,
        "status": status,
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "error_message": error_message,
    }
