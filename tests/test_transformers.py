"""
Tests for IMF and OECD transformers.

Verifies that raw records from fetchers are correctly converted
into Observation objects (our common data format).
"""

from datetime import date

import pytest

from pipeline.models import Observation
from pipeline.transformers import imf as imf_transformer
from pipeline.transformers import oecd as oecd_transformer

# =============================================================================
# IMF transformer tests
# =============================================================================


class TestIMFTransformer:
    def test_basic_transformation(self):
        """A raw IMF record should be converted into an Observation."""
        raw = [
            {
                "indicator_code": "NGDP_RPCH",
                "country_code": "USA",
                "year": 2023,
                "value": 2.54,
                "dataset_code": "WEO",
                "frequency": "A",
            }
        ]
        observations = imf_transformer.transform(raw)

        assert len(observations) == 1
        obs = observations[0]

        assert isinstance(obs, Observation)
        assert obs.source == "IMF"
        assert obs.dataset_code == "WEO"
        assert obs.indicator_code == "NGDP_RPCH"
        assert obs.country_code == "USA"
        assert obs.frequency == "A"
        assert obs.obs_date == date(2023, 1, 1)  # annual data = January 1st
        assert obs.obs_value == pytest.approx(2.54)

    def test_series_id_format(self):
        """series_id should follow the format: SOURCE.DATASET.INDICATOR.COUNTRY"""
        raw = [
            {
                "indicator_code": "PCPIPCH",
                "country_code": "POL",
                "year": 2022,
                "value": 14.4,
                "dataset_code": "WEO",
                "frequency": "A",
            }
        ]
        observations = imf_transformer.transform(raw)
        assert observations[0].series_id == "IMF.WEO.PCPIPCH.POL"

    def test_null_value_is_preserved(self):
        """A None value should be preserved (missing data)."""
        raw = [
            {
                "indicator_code": "NGDP_RPCH",
                "country_code": "USA",
                "year": 2023,
                "value": None,
                "dataset_code": "WEO",
                "frequency": "A",
            }
        ]
        observations = imf_transformer.transform(raw)
        assert observations[0].obs_value is None

    def test_country_code_uppercased(self):
        """Country code should always be uppercase."""
        raw = [
            {
                "indicator_code": "NGDP_RPCH",
                "country_code": "usa",  # lowercase
                "year": 2023,
                "value": 2.54,
                "dataset_code": "WEO",
                "frequency": "A",
            }
        ]
        observations = imf_transformer.transform(raw)
        assert observations[0].country_code == "USA"

    def test_empty_input_returns_empty_list(self):
        """No input records → no observations."""
        assert imf_transformer.transform([]) == []

    def test_invalid_records_are_skipped(self):
        """Records with missing fields should be skipped (no crash)."""
        raw = [
            {"country_code": "USA", "year": 2023, "value": 1.0},  # missing indicator_code
            {
                "indicator_code": "NGDP_RPCH",
                "country_code": "USA",
                "year": 2023,
                "value": 2.54,
                "dataset_code": "WEO",
                "frequency": "A",
            },
        ]
        observations = imf_transformer.transform(raw)
        # Only the second (valid) record should remain
        assert len(observations) == 1


# =============================================================================
# OECD transformer tests
# =============================================================================


class TestOECDTransformer:
    def test_basic_transformation(self):
        """A raw OECD record should be converted into an Observation."""
        raw = [
            {
                "indicator_code": "GDP",
                "country_code": "POL",
                "obs_date": date(2020, 1, 1),
                "value": 592340.5,
                "dataset_code": "QNA",
                "frequency": "Q",
            }
        ]
        observations = oecd_transformer.transform(raw)

        assert len(observations) == 1
        obs = observations[0]

        assert isinstance(obs, Observation)
        assert obs.source == "OECD"
        assert obs.dataset_code == "QNA"
        assert obs.indicator_code == "GDP"
        assert obs.country_code == "POL"
        assert obs.frequency == "Q"
        assert obs.obs_date == date(2020, 1, 1)
        assert obs.obs_value == pytest.approx(592340.5)

    def test_series_id_format(self):
        """series_id should follow the format: SOURCE.DATASET.INDICATOR.COUNTRY"""
        raw = [
            {
                "indicator_code": "GDP",
                "country_code": "USA",
                "obs_date": date(2023, 7, 1),
                "value": 100.0,
                "dataset_code": "QNA",
                "frequency": "Q",
            }
        ]
        observations = oecd_transformer.transform(raw)
        assert observations[0].series_id == "OECD.QNA.GDP.USA"

    def test_empty_input_returns_empty_list(self):
        """No input records → no observations."""
        assert oecd_transformer.transform([]) == []


# =============================================================================
# Observation model tests — to_bq_row()
# =============================================================================


class TestObservationToBQRow:
    def test_to_bq_row_contains_all_fields(self):
        """to_bq_row() should return a dict with all BigQuery columns."""
        raw = [
            {
                "indicator_code": "NGDP_RPCH",
                "country_code": "USA",
                "year": 2023,
                "value": 2.54,
                "dataset_code": "WEO",
                "frequency": "A",
            }
        ]
        obs = imf_transformer.transform(raw)[0]
        row = obs.to_bq_row()

        expected_keys = {
            "source",
            "dataset_code",
            "series_id",
            "indicator_code",
            "country_code",
            "frequency",
            "obs_date",
            "obs_value",
            "unit",
            "ingested_at",
        }
        assert set(row.keys()) == expected_keys

    def test_to_bq_row_date_is_string(self):
        """obs_date in the BQ row should be an ISO string (required by BigQuery)."""
        raw = [
            {
                "indicator_code": "NGDP_RPCH",
                "country_code": "USA",
                "year": 2023,
                "value": 2.54,
                "dataset_code": "WEO",
                "frequency": "A",
            }
        ]
        obs = imf_transformer.transform(raw)[0]
        row = obs.to_bq_row()

        assert isinstance(row["obs_date"], str)
        assert row["obs_date"] == "2023-01-01"
