"""Parquet normalization and append-delta utilities.

Created: 2026-07-05
Author: Luis G (https://github.com/Lu1sG4rc14)
"""

from __future__ import annotations

import hashlib
import math
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


CANONICAL_TYPES: dict[str, pa.DataType] = {
    "vendor_id": pa.int64(),
    "pickup_datetime": pa.timestamp("us"),
    "dropoff_datetime": pa.timestamp("us"),
    "passenger_count": pa.float64(),
    "trip_distance": pa.float64(),
    "ratecode_id": pa.float64(),
    "store_and_fwd_flag": pa.string(),
    "pickup_location_id": pa.int64(),
    "dropoff_location_id": pa.int64(),
    "payment_type": pa.int64(),
    "fare_amount": pa.float64(),
    "extra": pa.float64(),
    "mta_tax": pa.float64(),
    "tip_amount": pa.float64(),
    "tolls_amount": pa.float64(),
    "improvement_surcharge": pa.float64(),
    "total_amount": pa.float64(),
    "congestion_surcharge": pa.float64(),
    "airport_fee": pa.float64(),
}

SOURCE_COLUMN_CANDIDATES: dict[str, tuple[str, ...]] = {
    "vendor_id": ("VendorID", "vendor_id"),
    "pickup_datetime": ("tpep_pickup_datetime", "pickup_datetime"),
    "dropoff_datetime": ("tpep_dropoff_datetime", "dropoff_datetime"),
    "passenger_count": ("passenger_count",),
    "trip_distance": ("trip_distance",),
    "ratecode_id": ("RatecodeID", "ratecode_id"),
    "store_and_fwd_flag": ("store_and_fwd_flag",),
    "pickup_location_id": ("PULocationID", "pickup_location_id"),
    "dropoff_location_id": ("DOLocationID", "dropoff_location_id"),
    "payment_type": ("payment_type",),
    "fare_amount": ("fare_amount",),
    "extra": ("extra",),
    "mta_tax": ("mta_tax",),
    "tip_amount": ("tip_amount",),
    "tolls_amount": ("tolls_amount",),
    "improvement_surcharge": ("improvement_surcharge",),
    "total_amount": ("total_amount",),
    "congestion_surcharge": ("congestion_surcharge",),
    "airport_fee": ("airport_fee", "Airport_fee"),
}


def month_start(source_month: str) -> date:
    """Converts a `YYYY-MM` source month into its first calendar day.

    Args:
        source_month: Month string in `YYYY-MM` format.

    Returns:
        Date representing the first day of the month.
    """
    year, month = source_month.split("-")
    return date(int(year), int(month), 1)


def read_source_table(path: Path) -> pa.Table:
    """Reads a source Parquet file into an Arrow table.

    Args:
        path: Local Parquet file path.

    Returns:
        Arrow table with the source file schema.
    """
    return pq.read_table(path)


def canonical_raw_table(source_table: pa.Table) -> pa.Table:
    """Projects a TLC source table into the canonical raw schema.

    Monthly TLC files can vary in column names and physical types. This
    function picks the first known source column for each canonical field,
    casts it to the expected Arrow type, and fills missing optional fields
    with nulls.

    Args:
        source_table: Arrow table read from a TLC source Parquet file.

    Returns:
        Arrow table with canonical column names and types.
    """
    arrays: list[pa.Array | pa.ChunkedArray] = []
    names: list[str] = []

    for canonical_name, data_type in CANONICAL_TYPES.items():
        source_name = _first_existing(source_table, SOURCE_COLUMN_CANDIDATES[canonical_name])
        if source_name is None:
            array: pa.Array | pa.ChunkedArray = pa.nulls(source_table.num_rows, type=data_type)
        else:
            array = source_table[source_name]
            if not array.type.equals(data_type):
                array = pc.cast(array, data_type, safe=False)

        arrays.append(array)
        names.append(canonical_name)

    return pa.table(arrays, names=names)


def table_hash(table: pa.Table) -> str:
    """Computes a deterministic content hash for an Arrow table.

    Args:
        table: Canonical Arrow table or slice to hash.

    Returns:
        SHA-256 hexadecimal digest that is stable across runs for equivalent
        table content.
    """
    digest = hashlib.sha256()
    for column_name in table.column_names:
        digest.update(column_name.encode("utf-8"))
        digest.update(b"\x1e")
        column = table[column_name]
        for chunk in column.chunks:
            for value in chunk.to_pylist():
                digest.update(_stable_value(value))
                digest.update(b"\x1f")
    return digest.hexdigest()


def prepare_load_table(
    canonical_table: pa.Table,
    *,
    source_month: str,
    source_url: str,
    source_etag: str | None,
    ingestion_batch_id: str,
    start_offset: int,
    ingested_at: datetime | None = None,
) -> pa.Table:
    """Adds ingestion metadata columns to a delta or rebuild slice.

    The source does not provide a stable trip identifier. The pipeline keeps
    source row number and month as lineage, then bronze SQL derives a stable
    `meta_trip_key` from those values.

    Args:
        canonical_table: Canonical rows that should be loaded to BigQuery.
        source_month: Source month in `YYYY-MM` format.
        source_url: Original source file URL.
        source_etag: Source ETag captured during metadata inspection.
        ingestion_batch_id: Unique run identifier.
        start_offset: Zero-based number of rows skipped from the source file.
        ingested_at: Optional ingestion timestamp. Defaults to current UTC.

    Returns:
        Arrow table with source columns first and `meta_` lineage columns at
        the end.
    """
    ingested_at = ingested_at or datetime.now(timezone.utc)
    row_count = canonical_table.num_rows

    metadata_arrays: list[pa.Array] = [
        pa.array(range(start_offset + 1, start_offset + row_count + 1), type=pa.int64()),
        pa.array([month_start(source_month)] * row_count, type=pa.date32()),
        pa.array([source_url] * row_count, type=pa.string()),
        pa.array([source_etag] * row_count, type=pa.string()),
        pa.array([ingestion_batch_id] * row_count, type=pa.string()),
        pa.array([ingested_at] * row_count, type=pa.timestamp("us", tz="UTC")),
    ]
    metadata_names = [
        "meta_source_row_number",
        "meta_source_month",
        "meta_source_file_url",
        "meta_source_file_etag",
        "meta_ingestion_batch_id",
        "meta_ingested_at",
    ]

    return pa.table(
        [canonical_table[name] for name in canonical_table.column_names] + metadata_arrays,
        names=canonical_table.column_names + metadata_names,
    )


def write_parquet(table: pa.Table, path: Path) -> None:
    """Writes an Arrow table as Snappy-compressed Parquet.

    Args:
        table: Arrow table to write.
        path: Destination file path.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path, compression="snappy")


def _first_existing(table: pa.Table, candidates: tuple[str, ...]) -> str | None:
    """Finds the first candidate column present in a table.

    Args:
        table: Arrow table to inspect.
        candidates: Candidate source column names in priority order.

    Returns:
        First matching column name, or `None` when no candidate exists.
    """
    existing = set(table.column_names)
    for candidate in candidates:
        if candidate in existing:
            return candidate
    return None


def _stable_value(value: Any) -> bytes:
    """Serializes a Python value into stable bytes for hashing.

    Args:
        value: Scalar value from an Arrow column.

    Returns:
        Byte representation that treats nulls, NaN values, and date/time
        values consistently.
    """
    if value is None:
        return b"<NULL>"
    if isinstance(value, float) and math.isnan(value):
        return b"<NaN>"
    if isinstance(value, (datetime, date)):
        return value.isoformat().encode("utf-8")
    return repr(value).encode("utf-8")
