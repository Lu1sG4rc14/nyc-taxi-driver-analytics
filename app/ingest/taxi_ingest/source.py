"""HTTP source access helpers for the NYC taxi ingestion pipeline.

Created: 2026-07-05
Author: Luis G (https://github.com/Lu1sG4rc14)
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import requests


class SourceNotFound(FileNotFoundError):
    """Raised when an expected monthly TLC source file is not available."""


def source_url(base_url: str, source_month: str) -> str:
    """Builds the canonical TLC yellow taxi Parquet URL for a month.

    Args:
        base_url: Base URL that hosts monthly TLC trip-data files.
        source_month: Month to load in `YYYY-MM` format.

    Returns:
        Fully qualified URL for the selected monthly Parquet file.
    """
    return f"{base_url}/yellow_tripdata_{source_month}.parquet"


def head_source(url: str) -> dict[str, Any]:
    """Reads source metadata used to decide whether a file changed.

    Some object hosts do not allow HTTP HEAD. When that happens, the
    function falls back to a streamed GET and reads only response headers.

    Args:
        url: Source object URL.

    Returns:
        Dictionary containing `etag`, `last_modified`, and `content_length`.

    Raises:
        SourceNotFound: If the object returns HTTP 404.
        requests.HTTPError: If the source returns another non-success status.
    """
    response = requests.head(url, allow_redirects=True, timeout=30)
    if response.status_code == 405:
        response.close()
        with requests.get(url, stream=True, timeout=30) as fallback:
            _raise_for_status(fallback, url)
            return {
                "etag": fallback.headers.get("ETag"),
                "last_modified": fallback.headers.get("Last-Modified"),
                "content_length": _to_int(fallback.headers.get("Content-Length")),
            }
    _raise_for_status(response, url)
    try:
        return {
            "etag": response.headers.get("ETag"),
            "last_modified": response.headers.get("Last-Modified"),
            "content_length": _to_int(response.headers.get("Content-Length")),
        }
    finally:
        response.close()


def download(url: str, destination: Path) -> None:
    """Downloads a source object to local disk in streaming chunks.

    Args:
        url: Source object URL.
        destination: Local path where the downloaded file will be written.

    Raises:
        SourceNotFound: If the object returns HTTP 404.
        requests.HTTPError: If the source returns another non-success status.
    """
    destination.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=120) as response:
        _raise_for_status(response, url)
        with destination.open("wb") as fh:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    fh.write(chunk)


def _raise_for_status(response: requests.Response, url: str) -> None:
    """Normalizes HTTP error handling for source requests.

    Args:
        response: Completed `requests` response.
        url: URL being requested, used in the custom 404 message.

    Raises:
        SourceNotFound: If the object returns HTTP 404.
        requests.HTTPError: If the source returns another non-success status.
    """
    if response.status_code == 404:
        raise SourceNotFound(f"Source file not found: {url}")
    response.raise_for_status()


def _to_int(value: str | None) -> int | None:
    """Converts an optional header value to an integer when possible.

    Args:
        value: Raw HTTP header string.

    Returns:
        Parsed integer, or `None` when the value is missing or not numeric.
    """
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None
