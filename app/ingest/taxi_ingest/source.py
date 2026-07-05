from __future__ import annotations

from pathlib import Path
from typing import Any

import requests


class SourceNotFound(FileNotFoundError):
    pass


def source_url(base_url: str, source_month: str) -> str:
    return f"{base_url}/yellow_tripdata_{source_month}.parquet"


def head_source(url: str) -> dict[str, Any]:
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
    destination.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=120) as response:
        _raise_for_status(response, url)
        with destination.open("wb") as fh:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    fh.write(chunk)


def _raise_for_status(response: requests.Response, url: str) -> None:
    if response.status_code == 404:
        raise SourceNotFound(f"Source file not found: {url}")
    response.raise_for_status()


def _to_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None
