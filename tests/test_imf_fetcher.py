"""
Tests for IMFFetcher.

Uses fixtures (saved JSON) instead of the real API.
This keeps tests fast and runnable offline.
"""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline.config import DatasetConfig
from pipeline.fetchers.imf import IMFFetcher

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def imf_config() -> DatasetConfig:
    """IMF dataset configuration for tests."""
    return DatasetConfig(
        source="IMF",
        dataset_code="WEO",
        indicator_codes=["NGDP_RPCH", "PCPIPCH"],
        country_codes=["USA", "POL"],
        start_year=2019,
        frequency="A",
    )


@pytest.fixture
def imf_response() -> dict:
    """Load fixture JSON from disk."""
    with open(FIXTURES_DIR / "imf_response.json") as f:
        return json.load(f)


def test_build_url(imf_config):
    """URL should contain the first indicator and the countries."""
    fetcher = IMFFetcher()
    url = fetcher.build_url(imf_config)

    # build_url builds a URL for the first indicator (fetch() iterates over all)
    assert "NGDP_RPCH" in url
    assert "USA,POL" in url
    assert url.startswith("https://www.imf.org/external/datamapper/api/v1")


def test_parse_response_count(imf_config, imf_response):
    """Should get 2 indicators × 2 countries × 5 years = 20 records."""
    fetcher = IMFFetcher()
    records = fetcher.parse_response(imf_response, imf_config)

    assert len(records) == 20  # 2 indicators × 2 countries × 5 years


def test_parse_response_structure(imf_config, imf_response):
    """Every record must have the required fields."""
    fetcher = IMFFetcher()
    records = fetcher.parse_response(imf_response, imf_config)

    for rec in records:
        assert "indicator_code" in rec
        assert "country_code" in rec
        assert "year" in rec
        assert "value" in rec
        assert "dataset_code" in rec
        assert "frequency" in rec


def test_parse_response_filters_start_year():
    """Records before start_year should be filtered out."""
    config = DatasetConfig(
        source="IMF",
        dataset_code="WEO",
        indicator_codes=["NGDP_RPCH"],
        country_codes=["USA"],
        start_year=2022,  # from 2022 onwards only
        frequency="A",
    )
    response = {
        "values": {"NGDP_RPCH": {"USA": {"2020": 1.0, "2021": 2.0, "2022": 3.0, "2023": 4.0}}}
    }
    fetcher = IMFFetcher()
    records = fetcher.parse_response(response, config)

    years = [r["year"] for r in records]
    assert 2020 not in years
    assert 2021 not in years
    assert 2022 in years
    assert 2023 in years


def test_parse_response_handles_null_values(imf_config):
    """Null values from the API should be converted to None."""
    response = {"values": {"NGDP_RPCH": {"USA": {"2023": None}}}}
    fetcher = IMFFetcher()
    records = fetcher.parse_response(response, imf_config)

    assert len(records) == 1
    assert records[0]["value"] is None


def test_fetch_calls_api_per_indicator(imf_config):
    """fetch() should call HTTP GET separately for each indicator."""
    fetcher = IMFFetcher()

    # Response for each indicator separately (one indicator per request)
    resp_ngdp = {
        "values": {
            "NGDP_RPCH": {
                "USA": {"2019": 2.29, "2020": -2.77, "2021": 5.95, "2022": 1.99, "2023": 2.54},
                "POL": {"2019": 4.52, "2020": -2.00, "2021": 6.91, "2022": 5.14, "2023": 0.15},
            }
        }
    }
    resp_pcpi = {
        "values": {
            "PCPIPCH": {
                "USA": {"2019": 2.28, "2020": 1.23, "2021": 4.70, "2022": 8.00, "2023": 4.12},
                "POL": {"2019": 2.30, "2020": 3.40, "2021": 5.10, "2022": 14.40, "2023": 11.40},
            }
        }
    }
    # side_effect returns successive values on successive calls
    with patch.object(fetcher, "_get_with_retry", side_effect=[resp_ngdp, resp_pcpi]):
        records = fetcher.fetch(imf_config)

    # 2 indicators × 2 countries × 5 years (from 2019) = 20 records
    assert len(records) == 20
