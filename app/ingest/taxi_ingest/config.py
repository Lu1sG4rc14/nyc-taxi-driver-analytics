"""Runtime configuration parsing for the NYC taxi ingestion job.

Created: 2026-07-05
Author: Luis G (https://github.com/Lu1sG4rc14)
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Sequence


@dataclass(frozen=True)
class SourceSelection:
    """Resolved list of source months and missing-file behavior."""

    months: tuple[str, ...]
    skip_missing_sources: bool


@dataclass(frozen=True)
class PipelineConfig:
    """Immutable runtime configuration for one Cloud Run Job execution."""

    project_id: str
    raw_bucket: str
    staging_bucket: str
    bigquery_location: str
    source_months: tuple[str, ...]
    force_reload: bool = False
    skip_missing_sources: bool = False
    source_base_url: str = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    zone_lookup_url: str = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    ops_dataset: str = "L100_ops"
    staging_dataset: str = "L00_staging"
    bronze_dataset: str = "L10_bronze"
    silver_dataset: str = "L20_silver"
    gold_dataset: str = "L30_gold"

    @classmethod
    def from_env(cls, argv: Sequence[str] | None = None) -> "PipelineConfig":
        """Builds pipeline configuration from environment variables and CLI args.

        CLI source-selection arguments take precedence over environment
        variables, allowing Cloud Run execution overrides to drive manual
        backfills without changing the deployed job definition.

        Args:
            argv: Optional command-line arguments. When `None`, `argparse`
                reads from the process command line.

        Returns:
            Fully resolved pipeline configuration.

        Raises:
            ValueError: If a required environment variable is missing or a date
                range is invalid.
        """
        args = _parse_args(argv)
        source_selection = _resolve_source_selection(args)

        return cls(
            project_id=_required("PROJECT_ID"),
            raw_bucket=_required("RAW_BUCKET"),
            staging_bucket=_required("STAGING_BUCKET"),
            bigquery_location=os.getenv("BIGQUERY_LOCATION", "US"),
            source_months=source_selection.months,
            force_reload=args.force_reload or _truthy(os.getenv("FORCE_RELOAD")),
            skip_missing_sources=source_selection.skip_missing_sources,
            source_base_url=os.getenv(
                "SOURCE_BASE_URL",
                "https://d37ci6vzurychx.cloudfront.net/trip-data",
            ).rstrip("/"),
            zone_lookup_url=os.getenv(
                "ZONE_LOOKUP_URL",
                "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
            ),
        )


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    """Parses supported execution-time arguments.

    Args:
        argv: Optional argument sequence passed by tests or local execution.

    Returns:
        Parsed argparse namespace.
    """
    parser = argparse.ArgumentParser(description="Run the NYC taxi incremental ingest pipeline.")
    parser.add_argument(
        "--source-month",
        dest="source_month",
        action="append",
        default=[],
        help="Month to process in YYYY-MM format. Can be passed more than once.",
    )
    parser.add_argument(
        "--source-months",
        default=None,
        help="Comma-separated list of months to process in YYYY-MM format.",
    )
    parser.add_argument(
        "--source-date",
        default=None,
        help="Date in YYYY-MM-DD format; the containing month is processed.",
    )
    parser.add_argument(
        "--source-start-date",
        default=None,
        help="Inclusive start date for range backfills; processes every month from this date's month.",
    )
    parser.add_argument(
        "--source-end-date",
        default=None,
        help="Inclusive end date for range backfills. Defaults to yesterday when source-start-date is set.",
    )
    parser.add_argument(
        "--force-reload",
        action="store_true",
        help="Delete and fully reload the selected month(s), bypassing no-op and append-delta logic.",
    )
    return parser.parse_args(argv)


def _resolve_source_selection(args: argparse.Namespace) -> SourceSelection:
    """Resolves the month or months that the current run should process.

    Precedence is explicit CLI months, environment months, date range,
    single date, and finally yesterday's month for scheduled daily runs.

    Args:
        args: Parsed command-line arguments.

    Returns:
        Source selection with normalized month strings.

    Raises:
        ValueError: If any date or month value is invalid.
    """
    cli_months = [*args.source_month, *_split_csv(args.source_months)]
    if cli_months:
        return SourceSelection(months=_normalize_months(cli_months), skip_missing_sources=False)

    env_months = _split_csv(os.getenv("SOURCE_MONTHS"))
    if env_months:
        return SourceSelection(months=_normalize_months(env_months), skip_missing_sources=False)

    source_start_date = args.source_start_date or os.getenv("SOURCE_START_DATE")
    if source_start_date:
        start = date.fromisoformat(source_start_date)
        end = date.fromisoformat(args.source_end_date or os.getenv("SOURCE_END_DATE") or (date.today() - timedelta(days=1)).isoformat())
        return SourceSelection(months=_month_range(start, end), skip_missing_sources=True)

    source_date = args.source_date or os.getenv("SOURCE_DATE")
    if source_date:
        return SourceSelection(months=(_month_from_date(date.fromisoformat(source_date)),), skip_missing_sources=False)

    return SourceSelection(months=(_month_from_date(date.today() - timedelta(days=1)),), skip_missing_sources=False)


def _split_csv(value: str | None) -> list[str]:
    """Splits a comma-separated string into non-empty trimmed values.

    Args:
        value: Raw comma-separated string.

    Returns:
        List of non-empty values.
    """
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _normalize_months(months: Sequence[str]) -> tuple[str, ...]:
    """Validates, normalizes, and de-duplicates month strings.

    Args:
        months: Month values expected in `YYYY-MM` format.

    Returns:
        Tuple of unique month strings in first-seen order.

    Raises:
        ValueError: If a month cannot be parsed as `YYYY-MM`.
    """
    normalized: list[str] = []
    seen: set[str] = set()
    for month in months:
        normalized_month = date.fromisoformat(f"{month}-01").strftime("%Y-%m")
        if normalized_month not in seen:
            normalized.append(normalized_month)
            seen.add(normalized_month)
    return tuple(normalized)


def _month_from_date(value: date) -> str:
    """Formats a date as its containing source month.

    Args:
        value: Date to convert.

    Returns:
        Month string in `YYYY-MM` format.
    """
    return value.strftime("%Y-%m")


def _month_range(start: date, end: date) -> tuple[str, ...]:
    """Builds an inclusive range of source months.

    Args:
        start: Any date in the first month to process.
        end: Any date in the last month to process.

    Returns:
        Inclusive tuple of month strings in `YYYY-MM` format.

    Raises:
        ValueError: If the start month is after the end month.
    """
    start_month = date(start.year, start.month, 1)
    end_month = date(end.year, end.month, 1)
    if start_month > end_month:
        raise ValueError("source-start-date must be earlier than or equal to source-end-date")

    months: list[str] = []
    current = start_month
    while current <= end_month:
        months.append(_month_from_date(current))
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)
    return tuple(months)


def _truthy(value: str | None) -> bool:
    """Interprets common truthy strings used in environment variables.

    Args:
        value: Raw environment variable value.

    Returns:
        `True` for common truthy strings, otherwise `False`.
    """
    return value is not None and value.strip().lower() in {"1", "true", "t", "yes", "y"}


def _required(name: str) -> str:
    """Reads a required environment variable.

    Args:
        name: Environment variable name.

    Returns:
        Environment variable value.

    Raises:
        ValueError: If the value is missing or empty.
    """
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value
