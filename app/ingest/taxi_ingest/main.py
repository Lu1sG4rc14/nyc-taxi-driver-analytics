from __future__ import annotations

import logging
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

from taxi_ingest.bigquery_pipeline import BigQueryPipeline, manifest_row
from taxi_ingest.config import PipelineConfig
from taxi_ingest.parquet_delta import (
    canonical_raw_table,
    prepare_load_table,
    read_source_table,
    table_hash,
    write_parquet,
)
from taxi_ingest.source import SourceNotFound, download, head_source, source_url


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    config = PipelineConfig.from_env()
    pipeline = BigQueryPipeline(config)

    logger.info("Running BigQuery setup")
    pipeline.run_setup()
    logger.info(
        "Selected source_months=%s force_reload=%s skip_missing_sources=%s",
        config.source_months,
        config.force_reload,
        config.skip_missing_sources,
    )

    for source_month in config.source_months:
        process_month(config, pipeline, source_month)


def process_month(config: PipelineConfig, pipeline: BigQueryPipeline, source_month: str) -> None:
    run_id = uuid.uuid4().hex
    started_at = datetime.now(timezone.utc)
    url = source_url(config.source_base_url, source_month)
    logger.info("Processing source_month=%s run_id=%s", source_month, run_id)

    try:
        source_head = head_source(url)
        previous = pipeline.latest_success(source_month)

        if previous and _same_fingerprint(previous, source_head) and not config.force_reload:
            completed_at = datetime.now(timezone.utc)
            logger.info("No source metadata change for %s", source_month)
            pipeline.insert_manifest(
                manifest_row(
                    run_id=run_id,
                    source_month=source_month,
                    source_url=url,
                    source_head=source_head,
                    status="SUCCESS",
                    load_mode="NOOP",
                    started_at=started_at,
                    completed_at=completed_at,
                    source_row_count=previous.get("source_row_count"),
                    previous_row_count=previous.get("source_row_count"),
                    delta_row_count=0,
                    snapshot_hash=previous.get("snapshot_hash"),
                    previous_snapshot_hash=previous.get("snapshot_hash"),
                )
            )
            return

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            source_path = tmp / f"yellow_tripdata_{source_month}.parquet"
            download(url, source_path)

            raw_gcs_uri = pipeline.upload_file(
                source_path,
                config.raw_bucket,
                f"raw/yellow_tripdata/source_month={source_month}/run_id={run_id}/source.parquet",
            )

            source_table = read_source_table(source_path)
            canonical = canonical_raw_table(source_table)
            current_hash = table_hash(canonical)
            current_rows = canonical.num_rows

            previous_rows = int(previous["source_row_count"]) if previous else 0
            previous_hash = previous.get("snapshot_hash") if previous else None
            load_mode, start_offset = _decide_load_mode(
                canonical,
                current_hash=current_hash,
                current_rows=current_rows,
                previous_hash=previous_hash,
                previous_rows=previous_rows,
                force_reload=config.force_reload,
            )

            if load_mode == "METADATA_ONLY":
                completed_at = datetime.now(timezone.utc)
                pipeline.insert_manifest(
                    manifest_row(
                        run_id=run_id,
                        source_month=source_month,
                        source_url=url,
                        source_head=source_head,
                        status="SUCCESS",
                        load_mode=load_mode,
                        started_at=started_at,
                        completed_at=completed_at,
                        source_row_count=current_rows,
                        previous_row_count=previous_rows,
                        delta_row_count=0,
                        snapshot_hash=current_hash,
                        previous_snapshot_hash=previous_hash,
                        raw_gcs_uri=raw_gcs_uri,
                    )
                )
                logger.info("Source data unchanged for %s; metadata changed only", source_month)
                return

            load_slice = canonical.slice(start_offset)
            load_table = prepare_load_table(
                load_slice,
                source_month=source_month,
                source_url=url,
                source_etag=source_head.get("etag"),
                ingestion_batch_id=run_id,
                start_offset=start_offset,
                ingested_at=started_at,
            )
            delta_path = tmp / f"yellow_tripdata_{source_month}_{load_mode.lower()}.parquet"
            write_parquet(load_table, delta_path)

            delta_gcs_uri = pipeline.upload_file(
                delta_path,
                config.staging_bucket,
                f"generated/yellow_tripdata/source_month={source_month}/run_id={run_id}/{delta_path.name}",
            )
            staging_table = f"yellow_trips_delta_{source_month.replace('-', '')}_{run_id[:12]}"
            pipeline.load_parquet_to_staging(delta_gcs_uri, staging_table)

            logger.info("Refreshing taxi zone lookup before transformations")
            pipeline.refresh_zone_lookup()

            if load_mode in {"INITIAL_LOAD", "APPEND_DELTA"}:
                pipeline.apply_append_delta(staging_table, source_month)
            elif load_mode in {"REBUILD_MONTH", "FORCE_RELOAD"}:
                pipeline.apply_rebuild_month(staging_table, source_month)
            else:
                raise ValueError(f"Unsupported load_mode: {load_mode}")

            pipeline.drop_staging_table(staging_table)

            completed_at = datetime.now(timezone.utc)
            pipeline.insert_manifest(
                manifest_row(
                    run_id=run_id,
                    source_month=source_month,
                    source_url=url,
                    source_head=source_head,
                    status="SUCCESS",
                    load_mode=load_mode,
                    started_at=started_at,
                    completed_at=completed_at,
                    source_row_count=current_rows,
                    previous_row_count=previous_rows,
                    delta_row_count=load_slice.num_rows,
                    snapshot_hash=current_hash,
                    previous_snapshot_hash=previous_hash,
                    raw_gcs_uri=raw_gcs_uri,
                    delta_gcs_uri=delta_gcs_uri,
                    staging_table=staging_table,
                )
            )
            logger.info(
                "Completed %s for %s: source_rows=%s delta_rows=%s",
                load_mode,
                source_month,
                current_rows,
                load_slice.num_rows,
            )

    except SourceNotFound as exc:
        if not config.skip_missing_sources:
            logger.exception("Failed processing source_month=%s", source_month)
            pipeline.insert_manifest(
                manifest_row(
                    run_id=run_id,
                    source_month=source_month,
                    source_url=url,
                    source_head={},
                    status="FAILED",
                    load_mode="FAILED",
                    started_at=started_at,
                    completed_at=datetime.now(timezone.utc),
                    error_message=str(exc)[:1000],
                )
            )
            raise

        logger.warning("Skipping missing source_month=%s: %s", source_month, exc)
        pipeline.insert_manifest(
            manifest_row(
                run_id=run_id,
                source_month=source_month,
                source_url=url,
                source_head={},
                status="SKIPPED",
                load_mode="SOURCE_NOT_FOUND",
                started_at=started_at,
                completed_at=datetime.now(timezone.utc),
                error_message=str(exc)[:1000],
            )
        )

    except Exception as exc:
        logger.exception("Failed processing source_month=%s", source_month)
        try:
            pipeline.insert_manifest(
                manifest_row(
                    run_id=run_id,
                    source_month=source_month,
                    source_url=url,
                    source_head=source_head if "source_head" in locals() else {},
                    status="FAILED",
                    load_mode="FAILED",
                    started_at=started_at,
                    completed_at=datetime.now(timezone.utc),
                    error_message=str(exc)[:1000],
                )
            )
        finally:
            raise


def _same_fingerprint(previous: dict, source_head: dict) -> bool:
    return (
        previous.get("source_etag") == source_head.get("etag")
        and previous.get("source_last_modified") == source_head.get("last_modified")
        and previous.get("source_content_length") == source_head.get("content_length")
    )


def _decide_load_mode(
    canonical,
    *,
    current_hash: str,
    current_rows: int,
    previous_hash: str | None,
    previous_rows: int,
    force_reload: bool = False,
) -> tuple[str, int]:
    if force_reload:
        return "FORCE_RELOAD", 0

    if previous_hash is None or previous_rows == 0:
        return "INITIAL_LOAD", 0

    if current_hash == previous_hash:
        return "METADATA_ONLY", current_rows

    if current_rows > previous_rows:
        prefix_hash = table_hash(canonical.slice(0, previous_rows))
        if prefix_hash == previous_hash:
            return "APPEND_DELTA", previous_rows

    return "REBUILD_MONTH", 0


if __name__ == "__main__":
    main()
