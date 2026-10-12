"""Shared, side-effect-free parsing helpers for the OPSD grid dataset.

The CSV lands exactly as published; every interpretation of a column name,
timestamp or value happens here (tested) or in SQL views (also reviewed).
"""
from __future__ import annotations

METRIC_SUFFIXES = {
    "load_actual_entsoe_transparency": "load_actual",
    "load_forecast_entsoe_transparency": "load_forecast",
    "wind_generation_actual": "wind_generation",
    "solar_generation_actual": "solar_generation",
}

# Hourly rows published in the 2020-10-06 singleindex release:
# 2015-01-01T00:00Z .. 2020-09-30T23:00Z inclusive.
EXPECTED_HOURS = 50400


def classify_column(header: str) -> str | None:
    """Return the metric tag for a country-level series column, else None.

    Country-level means the prefix is exactly a two-letter ISO-style code:
    control-area splits (DE_50hertz_...), bidding zones (DK1_..._pow style) and
    capacity/profile variants are deliberately not selected.
    """
    for suffix, metric in METRIC_SUFFIXES.items():
        if header.endswith(suffix):
            code = header[: -len(suffix) - 1]
            if len(code) == 2 and code.isalpha() and code.isupper():
                return metric
    return None


def country_of(header: str) -> str:
    for suffix in METRIC_SUFFIXES:
        if header.endswith(suffix):
            return header[: -len(suffix) - 1]
    raise ValueError(f"not a selected series column: {header!r}")


def select_series_columns(header_row: list[str]) -> list[str]:
    return [h for h in header_row if classify_column(h) is not None]


def split_unit(unit: str) -> tuple[str, str]:
    """('DE_load_actual_entsoe_transparency') -> ('DE', 'load_actual')."""
    for suffix, metric in METRIC_SUFFIXES.items():
        if unit.endswith(suffix):
            return unit[: -len(suffix) - 1], metric
    raise ValueError(f"unknown unit code: {unit!r}")
