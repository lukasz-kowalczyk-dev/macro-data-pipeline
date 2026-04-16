"""
Pipeline configuration — list of datasets to fetch.

To add a new dataset, just append a DatasetConfig object to the DATASETS list.
No changes needed in fetcher or transformer logic.
"""

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()  # load variables from .env file (local dev only)


# ---------------------------------------------------------------------------
# GCP Settings
# ---------------------------------------------------------------------------

GCP_PROJECT_ID: str = os.environ.get("GCP_PROJECT_ID", "")
BQ_DATASET: str = os.environ.get("BQ_DATASET", "macro_data")
BQ_TABLE: str = "observations"

# Full BigQuery table name in format: project.dataset.table
BQ_TABLE_FULL: str = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"


# ---------------------------------------------------------------------------
# Dataset configuration definition
# ---------------------------------------------------------------------------


@dataclass
class DatasetConfig:
    """
    Describes one dataset to fetch from an API.

    Example (IMF):
        DatasetConfig(
            source="IMF",
            dataset_code="WEO",
            indicator_codes=["NGDP_RPCH", "PCPIPCH"],
            country_codes=["USA", "DEU", "POL"],
            start_year=2000,
        )
    """

    source: str
    """'OECD' or 'IMF'"""

    dataset_code: str
    """Dataset code in the API. E.g. 'WEO', 'QNA'."""

    indicator_codes: list[str]
    """List of indicator codes to fetch."""

    country_codes: list[str]
    """List of country codes (ISO alpha-3) to fetch."""

    start_year: int = 2000
    """Earliest year to include in historical data."""

    frequency: str = "A"
    """Frequency: 'A'=annual, 'Q'=quarterly, 'M'=monthly."""

    extra_params: dict = field(default_factory=dict)
    """Additional API-specific parameters (optional)."""


# ---------------------------------------------------------------------------
# List of datasets to fetch
# ---------------------------------------------------------------------------

DATASETS: list[DatasetConfig] = [
    # ------------------------------------------------------------------
    # IMF — World Economic Outlook (WEO)
    # Annual data: GDP growth, CPI inflation, CA balance, public debt
    # API: https://www.imf.org/external/datamapper/api/v1/
    # ------------------------------------------------------------------
    DatasetConfig(
        source="IMF",
        dataset_code="WEO",
        indicator_codes=[
            "NGDP_RPCH",  # Real GDP growth (%)
            "PCPIPCH",  # CPI inflation (%)
            "BCA_NGDPD",  # Current account balance (% of GDP)
            "GGXWDG_NGDP",  # Gross public debt (% of GDP)
            "LUR",  # Unemployment rate (%)
        ],
        country_codes=[
            "USA",
            "DEU",
            "GBR",
            "FRA",
            "JPN",
            "CHN",
            "POL",
            "CHE",
            "SWE",
            "NOR",
        ],
        start_year=2000,
        frequency="A",
    ),
    # ------------------------------------------------------------------
    # OECD — Quarterly National Accounts (QNA)
    # Quarterly data: GDP in quarterly terms
    # API: https://sdmx.oecd.org/public/rest/data/
    # ------------------------------------------------------------------
    DatasetConfig(
        source="OECD",
        dataset_code="QNA",
        indicator_codes=[
            "B1GQ",  # GDP year-on-year growth (%) — quarterly growth, year-on-year
        ],
        country_codes=[
            "USA",
            "DEU",
            "GBR",
            "FRA",
            "JPN",
            "POL",
            "CHE",
            "SWE",
            "NOR",
        ],
        start_year=2000,
        frequency="Q",
    ),
    # Note: OECD PRICES_CPI omitted — dataset contains hundreds of CPI sub-categories
    # (food, transport, clothing, etc.) and returns >11M records.
    # Inflation data is available from IMF (indicator PCPIPCH).
]
