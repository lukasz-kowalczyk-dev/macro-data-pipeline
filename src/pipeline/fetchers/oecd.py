"""
Fetcher for the new OECD SDMX REST API (sdmx.oecd.org).

API: https://sdmx.oecd.org/public/rest/data/
Docs: https://www.oecd.org/en/data/insights/data-explainers/2024/09/api.html

The new API (since 2024) replaced the old stats.oecd.org/SDMX-JSON/data/.
Supports multiple countries in a single request (+ separator).

URL format: /data/{agency},{dataflow}/{key}?startPeriod={period}&format=jsondata

Dataflow QNA (quarterly GDP growth, year-on-year):
  Agency+Dataflow: OECD.SDD.NAD,DSD_NAMAIN1@DF_QNA_EXPENDITURE_GROWTH_OECD
  Dimensions (13): FREQ.ADJUSTMENT.REF_AREA.SECTOR.COUNTERPART_SECTOR.TRANSACTION.
                   INSTR_ASSET.ACTIVITY.EXPENDITURE.UNIT_MEASURE.PRICE_BASE.TRANSFORMATION.TABLE_IDENTIFIER
  Key: Q.Y.POL+USA.S1.S1.B1GQ._Z._Z._Z.PC.L.G1.T0102
    - FREQ=Q (quarterly), ADJUSTMENT=Y (seasonally adjusted)
    - REF_AREA=POL+USA (countries, + separator)
    - TRANSACTION=B1GQ (GDP), UNIT_MEASURE=PC (percent), PRICE_BASE=L (chain-linked)
    - TRANSFORMATION=G1 (year-on-year growth), TABLE_IDENTIFIER=T0102

SDMX-JSON response format:
  {
    "data": {
      "dataSets": [{"series": {"0:0:0:...": {"observations": {"0": [val, 0]}}}}],
      "structures": [{"dimensions": {"series": [...], "observation": [...]}}]
    }
  }

Series key "0:0:0:..." = dimension value indices for each series dimension.
"""

import logging
from datetime import date

from pipeline.config import DatasetConfig
from pipeline.fetchers.base import BaseFetcher, FetchError

logger = logging.getLogger(__name__)

OECD_BASE_URL = "https://sdmx.oecd.org/public/rest/data"

# Dataflow configuration for each dataset_code
# key_template: {freq} → config.frequency, {countries} → '+'.join(country_codes)
OECD_DATAFLOWS: dict[str, dict] = {
    "QNA": {
        "agency": "OECD.SDD.NAD",
        "dataflow": "DSD_NAMAIN1@DF_QNA_EXPENDITURE_GROWTH_OECD",
        # FREQ.ADJUSTMENT.REF_AREA.SECTOR.COUNTERPART_SECTOR.TRANSACTION.
        # INSTR_ASSET.ACTIVITY.EXPENDITURE.UNIT_MEASURE.PRICE_BASE.TRANSFORMATION.TABLE_IDENTIFIER
        # PC=percent, L=chain-linked, G1=year-on-year, T0102=Table 1.2 quarterly growth
        "key_template": "{freq}.Y.{countries}.S1.S1.B1GQ._Z._Z._Z.PC.L.G1.T0102",
        "start_period": "{year}-Q1",
        "indicator_dim": "TRANSACTION",  # SDMX dimension containing the indicator code
    },
    "PRICES_CPI": {
        "agency": "OECD.SDD.TPS",
        "dataflow": "DSD_PRICES@DF_PRICES_ALL",
        # REF_AREA.FREQ.METHODOLOGY.MEASURE.UNIT_MEASURE.EXPENDITURE.ADJUSTMENT.TRANSFORMATION
        "key_template": "{countries}.M.N.CPI.IX.CP00.N._Z",
        "start_period": "{year}-01",
        "indicator_dim": "EXPENDITURE",  # CP00 = all CPI items
    },
}


class OECDFetcher(BaseFetcher):
    """
    Fetcher for the OECD SDMX REST API (sdmx.oecd.org).

    The new API supports multiple countries in a single request (+ separator),
    so fetch() sends one request for all countries from the config.
    """

    def build_url(self, config: DatasetConfig, country: str | None = None) -> str:
        """
        Builds a URL for the sdmx.oecd.org API.

        By default builds a URL for all countries in the config (+ separator).
        The optional country parameter restricts to a single country.

        Example (QNA, GDP, POL+USA, from 2000):
          /data/OECD.SDD.NAD,DSD_NAMAIN1@DF_QNA_EXPENDITURE_GROWTH_OECD/
            Q.Y.POL+USA.S1.S1.B1GQ._Z._Z._Z.PC.L.G1.T0102?startPeriod=2000-Q1&format=jsondata
        """
        df_cfg = OECD_DATAFLOWS.get(config.dataset_code)
        if not df_cfg:
            raise FetchError(f"Unknown OECD dataset: {config.dataset_code}")

        countries = country if country else "+".join(config.country_codes)
        freq = config.frequency

        key = df_cfg["key_template"].format(freq=freq, countries=countries)
        dataflow = f"{df_cfg['agency']},{df_cfg['dataflow']}"
        start = df_cfg["start_period"].format(year=config.start_year)

        return f"{OECD_BASE_URL}/{dataflow}/{key}?startPeriod={start}&format=jsondata"

    def fetch(self, config: DatasetConfig) -> list[dict]:
        """
        Fetches data from the OECD API — one request for all countries.

        The new sdmx.oecd.org supports multiple countries in a single URL (+ separator),
        so we don't need to iterate over countries as in the old API.
        """
        url = self.build_url(config)
        logger.info(f"[OECD] Fetching {config.dataset_code}: {url}")

        try:
            response_json = self._get_with_retry(url)
        except Exception as e:
            raise FetchError(f"Failed to fetch OECD data for {config.dataset_code}: {e}") from e

        records = self.parse_response(response_json, config)
        logger.info(f"[OECD] {config.dataset_code}: {len(records)} records")
        return records

    def parse_response(self, response_json: dict, config: DatasetConfig) -> list[dict]:
        """
        Parses an SDMX-JSON response from sdmx.oecd.org.

        Response format:
        data.structures[0].dimensions contains series and observation dimension descriptors.
        Series key "0:1:0:0" = value indices for each dimension.
        """
        records = []

        data = response_json.get("data", {})
        data_sets = data.get("dataSets", [])
        if not data_sets:
            logger.warning(f"OECD [{config.dataset_code}]: empty response (no dataSets)")
            return records

        structures = data.get("structures", [])
        structure = structures[0] if structures else data.get("structure", {})
        dimensions = structure.get("dimensions", {})

        series_dims = dimensions.get("series", [])
        obs_dims = dimensions.get("observation", [])

        # Build value lists for each series dimension
        series_dim_values = [
            [v.get("id", "") for v in dim.get("values", [])] for dim in series_dims
        ]

        # Find observation dates (index → period string e.g. "2020-Q1")
        time_values = []
        for dim in obs_dims:
            if dim.get("id") == "TIME_PERIOD":
                time_values = [v.get("id", "") for v in dim.get("values", [])]
                break

        # Which dimension contains the indicator code (depends on dataflow)
        df_cfg = OECD_DATAFLOWS.get(config.dataset_code, {})
        indicator_dim = df_cfg.get("indicator_dim", "TRANSACTION")

        dataset = data_sets[0]
        series_data = dataset.get("series", {})

        for series_key, series_obj in series_data.items():
            # series_key e.g. "0:1:0:0" — value indices for each dimension
            key_indices = [int(i) for i in series_key.split(":")]

            # Decode series dimensions to a dict: {dim_id: value}
            decoded = {}
            for dim_idx, val_idx in enumerate(key_indices):
                if dim_idx < len(series_dims):
                    dim_id = series_dims[dim_idx].get("id", "")
                    dim_vals = series_dim_values[dim_idx]
                    decoded[dim_id] = dim_vals[val_idx] if val_idx < len(dim_vals) else ""

            country_code = decoded.get("REF_AREA", "")
            indicator_code = decoded.get(indicator_dim, "")

            # Skip series where key dimensions could not be decoded
            if not indicator_code or not country_code:
                continue

            # Parse observations: {obs_index: [value, ...]}
            observations = series_obj.get("observations", {})
            for obs_idx_str, obs_vals in observations.items():
                obs_idx = int(obs_idx_str)
                value = obs_vals[0] if obs_vals else None

                if obs_idx >= len(time_values):
                    continue

                time_str = time_values[obs_idx]
                obs_date = self._parse_period(time_str, config.frequency)
                if obs_date is None:
                    continue

                if obs_date.year < config.start_year:
                    continue

                records.append(
                    {
                        "indicator_code": indicator_code,
                        "country_code": country_code,
                        "obs_date": obs_date,
                        "value": float(value) if value is not None else None,
                        "dataset_code": config.dataset_code,
                        "frequency": config.frequency,
                    }
                )

        return records

    @staticmethod
    def _parse_period(period_str: str, frequency: str) -> date | None:
        """
        Converts an SDMX period string to a date (first day of the period).

        Examples:
          "2020"     → date(2020, 1, 1)     (annual)
          "2020-Q1"  → date(2020, 1, 1)     (quarterly)
          "2020-Q3"  → date(2020, 7, 1)
          "2020-01"  → date(2020, 1, 1)     (monthly)
          "2020-12"  → date(2020, 12, 1)
        """
        try:
            if frequency == "A" or (len(period_str) == 4 and period_str.isdigit()):
                return date(int(period_str[:4]), 1, 1)

            if "Q" in period_str:
                # Format: YYYY-Q1, YYYY-Q2, YYYY-Q3, YYYY-Q4
                year, quarter_str = period_str.split("-")
                quarter = int(quarter_str[1])  # Q1→1, Q2→2, etc.
                month = (quarter - 1) * 3 + 1  # Q1→1, Q2→4, Q3→7, Q4→10
                return date(int(year), month, 1)

            if "-" in period_str and len(period_str) == 7:
                # Format: YYYY-MM
                year, month = period_str.split("-")
                return date(int(year), int(month), 1)

        except (ValueError, IndexError) as e:
            logger.warning(f"OECD: cannot parse period '{period_str}': {e}")

        return None
