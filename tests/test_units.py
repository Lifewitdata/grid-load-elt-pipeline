"""Unit tests for the parsing contract between the CSV bytes and the warehouse.

The DB-dependent behaviour is exercised by etl/dq.py against the real
warehouse; these tests cover the pure functions so a regression in column
selection or parsing is caught without a database.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "etl"))
from units import (classify_column, country_of,  # noqa: E402
                   select_series_columns, split_unit)

LOAD_A = "DE_load_actual_entsoe_transparency"
LOAD_F = "FR_load_forecast_entsoe_transparency"
WIND = "DK_wind_generation_actual"
SOLAR = "PT_solar_generation_actual"


@pytest.mark.parametrize("col,metric", [
    (LOAD_A, "load_actual"), (LOAD_F, "load_forecast"),
    (WIND, "wind_generation"), (SOLAR, "solar_generation"),
])
def test_selected_columns(col, metric):
    assert classify_column(col) == metric


@pytest.mark.parametrize("col", [
    "DE_50hertz_load_actual_entsoe_transparency",   # control area, not country
    "DE_LU_load_actual_entsoe_transparency",        # country pair
    "DE_wind_capacity",                             # capacity, not generation
    "DE_wind_profile",
    "wind_DE_estate_2019_10_pow",
    "utc_timestamp", "cet_cest_timestamp",
    "X1_load_actual_entsoe_transparency",           # not 2-letter
    "DE_load_actual_entsoe_transparency_extra",
])
def test_rejected_columns(col):
    assert classify_column(col) is None


def test_select_over_real_header():
    header = ["utc_timestamp", "cet_cest_timestamp", LOAD_A,
              "DE_wind_capacity", "DE_50hertz_load_actual_entsoe_transparency", WIND]
    assert select_series_columns(header) == [LOAD_A, WIND]


def test_split_unit_roundtrip():
    assert split_unit(LOAD_A) == ("DE", "load_actual")
    assert split_unit(WIND) == ("DK", "wind_generation")
    with pytest.raises(ValueError):
        split_unit("bogus")
    assert country_of(LOAD_F) == "FR"
