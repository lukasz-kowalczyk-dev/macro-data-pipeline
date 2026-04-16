"""
Transformer for OECD data.

Converts raw records from OECDFetcher (list of dicts)
into a list of Observation objects ready for writing to BigQuery.

Raw record from fetcher:
  {
    "indicator_code": "GDP",
    "country_code": "POL",
    "obs_date": date(2020, 1, 1),
    "value": 592.0,
    "dataset_code": "QNA",
    "frequency": "Q",
  }

Result — Observation object:
  source="OECD", dataset_code="QNA", indicator_code="GDP",
  country_code="POL", frequency="Q",
  obs_date=date(2020, 1, 1), obs_value=592.0, unit="USD_CAP"
"""

import logging

from pipeline.models import Observation

logger = logging.getLogger(__name__)

# Mapping: (dataset_code, indicator_code) → unit
# indicator_code comes from the SDMX dimension returned by the API
OECD_UNITS: dict[tuple[str, str], str] = {
    ("QNA", "B1GQ"): "PC",  # GDP year-on-year growth, percent
    ("PRICES_CPI", "CP00"): "IX",  # CPI all items, index
}


def transform(raw_records: list[dict]) -> list[Observation]:
    """
    Transforms raw OECD records into Observation objects.

    Args:
        raw_records: List of dicts returned by OECDFetcher.parse_response()

    Returns:
        List of Observation objects.
    """
    observations = []
    skipped = 0

    for rec in raw_records:
        try:
            dataset_code = rec["dataset_code"]
            indicator_code = rec["indicator_code"]
            unit = OECD_UNITS.get((dataset_code, indicator_code))

            obs = Observation(
                source="OECD",
                dataset_code=dataset_code,
                indicator_code=indicator_code,
                country_code=rec["country_code"],
                frequency=rec["frequency"],
                obs_date=rec["obs_date"],
                obs_value=rec.get("value"),
                unit=unit,
            )
            observations.append(obs)

        except (KeyError, ValueError) as e:
            logger.warning(f"OECD transformer: skipping record {rec} — error: {e}")
            skipped += 1

    if skipped > 0:
        logger.warning(f"OECD transformer: skipped {skipped} records with errors")

    logger.info(f"OECD transformer: transformed {len(observations)} observations")
    return observations
