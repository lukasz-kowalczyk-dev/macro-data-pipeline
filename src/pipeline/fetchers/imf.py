"""
Fetcher for the IMF DataMapper API.

API: https://www.imf.org/external/datamapper/api/v1/
Docs: https://www.imf.org/external/datamapper/api/help

Example URL (one indicator at a time!):
  https://www.imf.org/external/datamapper/api/v1/NGDP_RPCH/USA,DEU,POL

IMPORTANT: The API ignores multiple comma-separated indicators — it returns empty values.
Therefore fetch() queries the API separately for each indicator and aggregates the results.

JSON response structure:
  {
    "values": {
      "NGDP_RPCH": {
        "USA": { "2020": -2.77, "2021": 5.95, "2022": 1.99, ... },
        "DEU": { "2020": -3.83, "2021": 3.16, ... },
        ...
      }
    }
  }
"""

import logging

from pipeline.config import DatasetConfig
from pipeline.fetchers.base import BaseFetcher, FetchError

logger = logging.getLogger(__name__)

IMF_BASE_URL = "https://www.imf.org/external/datamapper/api/v1"


class IMFFetcher(BaseFetcher):
    """
    Fetcher for the IMF DataMapper API.

    Queries the API separately for each indicator (the API does not support multiple at once).
    """

    def build_url(self, config: DatasetConfig) -> str:
        """
        Builds a URL for the first indicator in the configuration.

        Note: fetch() overrides this behaviour and iterates over all indicators.
        This method is kept for compatibility with BaseFetcher.

        Format: /v1/{indicator}/{country1},{country2}
        """
        indicator = config.indicator_codes[0]
        countries = ",".join(config.country_codes)
        return f"{IMF_BASE_URL}/{indicator}/{countries}"

    def fetch(self, config: DatasetConfig) -> list[dict]:
        """
        Fetches data from the IMF API — one request per indicator.

        The IMF API does not support multiple indicators in a single URL (returns empty values).
        Therefore we iterate over indicators and aggregate the results.
        """
        all_records = []
        countries = ",".join(config.country_codes)

        for indicator in config.indicator_codes:
            url = f"{IMF_BASE_URL}/{indicator}/{countries}"
            logger.info(f"[IMF] Fetching {indicator}: {url}")

            try:
                response_json = self._get_with_retry(url)
            except Exception as e:
                raise FetchError(
                    f"Failed to fetch {indicator} from IMF for {config.dataset_code}: {e}"
                ) from e

            records = self.parse_response(response_json, config)
            logger.info(f"[IMF] {indicator}: {len(records)} records")
            all_records.extend(records)

        logger.info(f"[IMF] Total: {len(all_records)} records for {config.dataset_code}")
        return all_records

    def parse_response(self, response_json: dict, config: DatasetConfig) -> list[dict]:
        """
        Parses the IMF API response into a list of raw records.

        Each record is a dictionary with keys:
          indicator_code, country_code, year, value
        """
        records = []

        # Data is at: response["values"][indicator][country][year] = value
        values = response_json.get("values", {})

        for indicator_code, countries_data in values.items():
            if not countries_data:
                logger.warning(f"IMF: no data for indicator {indicator_code} — skipping")
                continue
            for country_code, yearly_data in countries_data.items():
                if not yearly_data:
                    continue
                for year_str, value in yearly_data.items():
                    try:
                        year = int(year_str)
                    except ValueError:
                        logger.warning(
                            f"IMF: invalid year '{year_str}' for "
                            f"{indicator_code}/{country_code} — skipping"
                        )
                        continue

                    # Filter years before start_year
                    if year < config.start_year:
                        continue

                    records.append(
                        {
                            "indicator_code": indicator_code,
                            "country_code": country_code,
                            "year": year,
                            # IMF may return "n/a" or null — convert to None
                            "value": float(value) if value is not None and value != "n/a" else None,
                            "dataset_code": config.dataset_code,
                            "frequency": config.frequency,
                        }
                    )

        return records
