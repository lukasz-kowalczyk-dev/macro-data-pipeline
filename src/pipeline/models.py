"""
Data models for the pipeline.

Observation = one row in the BigQuery table.
Every source (OECD, IMF) must map its data to this format.
"""

from datetime import date, datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field, computed_field, field_validator


class Observation(BaseModel):
    """
    One macroeconomic observation.

    Example: Poland's GDP for 2023 according to OECD = 679.4 billion USD.
    """

    # --- Source identification ---
    source: str
    """Where the data comes from. Values: 'OECD' or 'IMF'."""

    dataset_code: str
    """Dataset code in the source API. E.g. 'QNA' (OECD), 'WEO' (IMF)."""

    indicator_code: str
    """Indicator code. E.g. 'GDP', 'PCPIPCH' (CPI inflation)."""

    country_code: str
    """Country code in ISO 3166 alpha-3 format. E.g. 'POL', 'USA', 'DEU'."""

    frequency: str
    """Observation frequency: 'A' = annual, 'Q' = quarterly, 'M' = monthly."""

    # --- Observation value ---
    obs_date: date
    """
    Observation date — always the first day of the period.
    E.g. Q1 2023 → 2023-01-01, December 2023 → 2023-12-01.
    """

    obs_value: Optional[float]
    """Numeric value. None means missing data (NaN in the API)."""

    unit: Optional[str] = None
    """Unit of measure. E.g. 'USD_CAP', '%', 'INDEX'."""

    # --- Metadata ---
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(tz=timezone.utc))
    """Time of loading into BigQuery. Set automatically when the object is created."""

    @computed_field
    @property
    def series_id(self) -> str:
        """
        Unique time series key.
        Format: '{source}.{dataset_code}.{indicator_code}.{country_code}'
        E.g. 'OECD.QNA.GDP.POL'

        Used in the MERGE (upsert) clause in BigQuery.
        """
        return f"{self.source}.{self.dataset_code}.{self.indicator_code}.{self.country_code}"

    @field_validator("source")
    @classmethod
    def source_must_be_known(cls, v: str) -> str:
        allowed = {"OECD", "IMF"}
        if v not in allowed:
            raise ValueError(f"source must be one of {allowed}, got: '{v}'")
        return v

    @field_validator("frequency")
    @classmethod
    def frequency_must_be_known(cls, v: str) -> str:
        allowed = {"A", "Q", "M"}
        if v not in allowed:
            raise ValueError(f"frequency must be one of {allowed}, got: '{v}'")
        return v

    @field_validator("country_code")
    @classmethod
    def country_code_uppercase(cls, v: str) -> str:
        return v.upper()

    def to_bq_row(self) -> dict:
        """
        Converts an Observation to a dictionary ready for insertion into BigQuery.
        BigQuery requires dates as ISO-format strings or date/datetime objects.
        """
        return {
            "source": self.source,
            "dataset_code": self.dataset_code,
            "series_id": self.series_id,
            "indicator_code": self.indicator_code,
            "country_code": self.country_code,
            "frequency": self.frequency,
            "obs_date": self.obs_date.isoformat(),
            "obs_value": self.obs_value,
            "unit": self.unit,
            "ingested_at": self.ingested_at.isoformat(),
        }
