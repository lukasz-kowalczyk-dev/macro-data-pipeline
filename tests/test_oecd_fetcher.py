"""
Tests for OECDFetcher.

Verifies parsing of the SDMX-JSON format (more complex than IMF).
"""

import json
from datetime import date
from pathlib import Path

import pytest

from pipeline.config import DatasetConfig
from pipeline.fetchers.oecd import OECDFetcher

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def oecd_config() -> DatasetConfig:
    return DatasetConfig(
        source="OECD",
        dataset_code="QNA",
        indicator_codes=["B1GQ"],
        country_codes=["POL", "USA"],
        start_year=2000,
        frequency="Q",
    )


@pytest.fixture
def oecd_response() -> dict:
    with open(FIXTURES_DIR / "oecd_response.json") as f:
        return json.load(f)


def test_parse_response_count(oecd_config, oecd_response):
    """Should get 2 series × 3 observations = 6 records."""
    fetcher = OECDFetcher()
    records = fetcher.parse_response(oecd_response, oecd_config)
    assert len(records) == 6


def test_parse_response_structure(oecd_config, oecd_response):
    """Every record must have the required fields."""
    fetcher = OECDFetcher()
    records = fetcher.parse_response(oecd_response, oecd_config)

    for rec in records:
        assert "indicator_code" in rec
        assert "country_code" in rec
        assert "obs_date" in rec
        assert isinstance(rec["obs_date"], date)
        assert "value" in rec


def test_build_url_contains_key_parts(oecd_config):
    """URL should contain the dataflow, country, indicator code, and parameters."""
    fetcher = OECDFetcher()
    url = fetcher.build_url(oecd_config, country="POL")

    assert "sdmx.oecd.org" in url
    assert "DSD_NAMAIN1" in url
    assert "DF_QNA_EXPENDITURE_GROWTH_OECD" in url
    assert "POL" in url
    assert "B1GQ" in url
    assert "startPeriod=2000-Q1" in url
    assert "format=jsondata" in url


def test_build_url_multi_country(oecd_config):
    """Without the country parameter — URL contains all countries with '+' separator."""
    fetcher = OECDFetcher()
    url = fetcher.build_url(oecd_config)

    assert "POL+USA" in url


class TestParsePeriod:
    """Tests for SDMX date parsing."""

    def test_annual(self):
        assert OECDFetcher._parse_period("2020", "A") == date(2020, 1, 1)

    def test_quarterly_q1(self):
        assert OECDFetcher._parse_period("2020-Q1", "Q") == date(2020, 1, 1)

    def test_quarterly_q2(self):
        assert OECDFetcher._parse_period("2020-Q2", "Q") == date(2020, 4, 1)

    def test_quarterly_q3(self):
        assert OECDFetcher._parse_period("2020-Q3", "Q") == date(2020, 7, 1)

    def test_quarterly_q4(self):
        assert OECDFetcher._parse_period("2020-Q4", "Q") == date(2020, 10, 1)

    def test_monthly(self):
        assert OECDFetcher._parse_period("2020-06", "M") == date(2020, 6, 1)

    def test_invalid_returns_none(self):
        assert OECDFetcher._parse_period("invalid", "A") is None
