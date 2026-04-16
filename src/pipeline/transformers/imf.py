"""
Transformer for IMF data.

Converts raw records from IMFFetcher (list of dicts)
into a list of Observation objects ready for writing to BigQuery.

Raw record from fetcher:
  {
    "indicator_code": "NGDP_RPCH",
    "country_code": "USA",
    "year": 2023,
    "value": 2.54,
    "dataset_code": "WEO",
    "frequency": "A",
  }

Result — Observation object:
  source="IMF", dataset_code="WEO", indicator_code="NGDP_RPCH",
  country_code="USA", frequency="A",
  obs_date=date(2023, 1, 1), obs_value=2.54, unit="%"
"""

import logging
from datetime import date

from pipeline.models import Observation

logger = logging.getLogger(__name__)

# Mapping: IMF indicator → unit
# Extend as new indicators are added
IMF_UNITS: dict[str, str] = {
    "NGDP_RPCH": "%",  # GDP growth
    "PCPIPCH": "%",  # CPI inflation
    "BCA_NGDPD": "% of GDP",  # current account balance
    "GGXWDG_NGDP": "% of GDP",  # public debt
    "LUR": "%",  # unemployment rate
}


def transform(raw_records: list[dict]) -> list[Observation]:
    """
    Transforms raw IMF records into Observation objects.

    Args:
        raw_records: List of dicts returned by IMFFetcher.parse_response()

    Returns:
        List of Observation objects (may be shorter if records are invalid).
    """
    observations = []
    skipped = 0

    for rec in raw_records:
        try:
            indicator_code = rec["indicator_code"]
            country_code = rec["country_code"]
            year = rec["year"]
            value = rec.get("value")

            obs = Observation(
                source="IMF",
                dataset_code=rec["dataset_code"],
                indicator_code=indicator_code,
                country_code=country_code,
                frequency=rec["frequency"],
                # IMF annual data — always January 1st of the given year
                obs_date=date(year, 1, 1),
                obs_value=value,
                unit=IMF_UNITS.get(indicator_code),
            )
            observations.append(obs)

        except (KeyError, ValueError) as e:
            logger.warning(f"IMF transformer: skipping record {rec} — error: {e}")
            skipped += 1

    if skipped > 0:
        logger.warning(f"IMF transformer: skipped {skipped} records with errors")

    logger.info(f"IMF transformer: transformed {len(observations)} observations")
    return observations
