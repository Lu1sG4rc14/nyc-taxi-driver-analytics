"""Tests for source-month selection and backfill configuration.

Created: 2026-07-05
Author: Luis G (https://github.com/Lu1sG4rc14)
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from taxi_ingest.config import _parse_args, _resolve_source_selection


class ConfigSourceSelectionTests(unittest.TestCase):
    """Validates CLI and environment source selection behavior."""

    def test_source_date_selects_only_containing_month(self) -> None:
        """Ensures a specific source date resolves to its containing month."""
        args = _parse_args(["--source-date", "2023-02-15"])

        with patch.dict("os.environ", {}, clear=True):
            selection = _resolve_source_selection(args)

        self.assertEqual(selection.months, ("2023-02",))
        self.assertFalse(selection.skip_missing_sources)

    def test_source_start_date_selects_inclusive_month_range(self) -> None:
        """Ensures a backfill date range includes both boundary months."""
        args = _parse_args(
            [
                "--source-start-date",
                "2023-01-15",
                "--source-end-date",
                "2023-03-01",
            ]
        )

        with patch.dict("os.environ", {}, clear=True):
            selection = _resolve_source_selection(args)

        self.assertEqual(selection.months, ("2023-01", "2023-02", "2023-03"))
        self.assertTrue(selection.skip_missing_sources)

    def test_explicit_months_take_precedence_over_range_backfill(self) -> None:
        """Ensures explicit months override broader range-backfill arguments."""
        args = _parse_args(
            [
                "--source-month",
                "2023-02",
                "--source-start-date",
                "2023-01-01",
                "--source-end-date",
                "2023-03-01",
            ]
        )

        with patch.dict("os.environ", {}, clear=True):
            selection = _resolve_source_selection(args)

        self.assertEqual(selection.months, ("2023-02",))
        self.assertFalse(selection.skip_missing_sources)


if __name__ == "__main__":
    unittest.main()
