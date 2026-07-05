from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Sequence


@dataclass(frozen=True)
class SourceSelection:
    months: tuple[str, ...]
    skip_missing_sources: bool


@dataclass(frozen=True)
class PipelineConfig:
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
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _normalize_months(months: Sequence[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    seen: set[str] = set()
    for month in months:
        normalized_month = date.fromisoformat(f"{month}-01").strftime("%Y-%m")
        if normalized_month not in seen:
            normalized.append(normalized_month)
            seen.add(normalized_month)
    return tuple(normalized)


def _month_from_date(value: date) -> str:
    return value.strftime("%Y-%m")


def _month_range(start: date, end: date) -> tuple[str, ...]:
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
    return value is not None and value.strip().lower() in {"1", "true", "t", "yes", "y"}


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value
