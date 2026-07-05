"""Tests for Parquet normalization and append-delta detection.

Created: 2026-07-05
Author: Luis G (https://github.com/Lu1sG4rc14)
"""

from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from taxi_ingest.main import _decide_load_mode
from taxi_ingest.parquet_delta import canonical_raw_table, prepare_load_table, table_hash


class ParquetDeltaTests(unittest.TestCase):
    """Validates incremental load-mode decisions and generated metadata."""

    def test_append_only_change_loads_only_new_rows(self) -> None:
        """Ensures append-only source changes load only newly appended rows."""
        first_snapshot = canonical_raw_table(_source_table([1, 2]))
        second_snapshot = canonical_raw_table(_source_table([1, 2, 3]))

        mode, start_offset = _decide_load_mode(
            second_snapshot,
            current_hash=table_hash(second_snapshot),
            current_rows=second_snapshot.num_rows,
            previous_hash=table_hash(first_snapshot),
            previous_rows=first_snapshot.num_rows,
        )

        self.assertEqual(mode, "APPEND_DELTA")
        self.assertEqual(start_offset, 2)

        delta = prepare_load_table(
            second_snapshot.slice(start_offset),
            source_month="2023-01",
            source_url="https://example.test/yellow_tripdata_2023-01.parquet",
            source_etag='"abc"',
            ingestion_batch_id="run123",
            start_offset=start_offset,
            ingested_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(delta.num_rows, 1)
        self.assertEqual(delta.column_names[-6:], [
            "meta_source_row_number",
            "meta_source_month",
            "meta_source_file_url",
            "meta_source_file_etag",
            "meta_ingestion_batch_id",
            "meta_ingested_at",
        ])
        self.assertEqual(delta["meta_source_row_number"].to_pylist(), [3])
        self.assertEqual(delta["vendor_id"].to_pylist(), [3])

    def test_prefix_change_rebuilds_month(self) -> None:
        """Ensures changed historical rows trigger a month rebuild."""
        first_snapshot = canonical_raw_table(_source_table([1, 2]))
        changed_snapshot = canonical_raw_table(_source_table([9, 2, 3]))

        mode, start_offset = _decide_load_mode(
            changed_snapshot,
            current_hash=table_hash(changed_snapshot),
            current_rows=changed_snapshot.num_rows,
            previous_hash=table_hash(first_snapshot),
            previous_rows=first_snapshot.num_rows,
        )

        self.assertEqual(mode, "REBUILD_MONTH")
        self.assertEqual(start_offset, 0)

    def test_force_reload_rebuilds_from_first_row(self) -> None:
        """Ensures explicit force reload bypasses append-delta detection."""
        snapshot = canonical_raw_table(_source_table([1, 2, 3]))

        mode, start_offset = _decide_load_mode(
            snapshot,
            current_hash=table_hash(snapshot),
            current_rows=snapshot.num_rows,
            previous_hash=table_hash(canonical_raw_table(_source_table([1, 2]))),
            previous_rows=2,
            force_reload=True,
        )

        self.assertEqual(mode, "FORCE_RELOAD")
        self.assertEqual(start_offset, 0)


def _source_table(vendor_ids: list[int]) -> pa.Table:
    """Builds a minimal TLC-like Arrow table for unit tests.

    Args:
        vendor_ids: Vendor IDs used to distinguish row order and content.

    Returns:
        Arrow table with the source columns required by canonicalization.
    """
    row_count = len(vendor_ids)
    return pa.table(
        {
            "VendorID": vendor_ids,
            "tpep_pickup_datetime": [
                datetime(2023, 1, 1, 8, i, 0) for i in range(row_count)
            ],
            "tpep_dropoff_datetime": [
                datetime(2023, 1, 1, 8, i + 5, 0) for i in range(row_count)
            ],
            "passenger_count": [1] * row_count,
            "trip_distance": [1.2] * row_count,
            "RatecodeID": [1] * row_count,
            "store_and_fwd_flag": ["N"] * row_count,
            "PULocationID": [161] * row_count,
            "DOLocationID": [236] * row_count,
            "payment_type": [1] * row_count,
            "fare_amount": [10.0] * row_count,
            "extra": [0.0] * row_count,
            "mta_tax": [0.5] * row_count,
            "tip_amount": [2.0] * row_count,
            "tolls_amount": [0.0] * row_count,
            "improvement_surcharge": [1.0] * row_count,
            "total_amount": [15.0] * row_count,
            "congestion_surcharge": [2.5] * row_count,
            "Airport_fee": [0.0] * row_count,
        }
    )


if __name__ == "__main__":
    unittest.main()
