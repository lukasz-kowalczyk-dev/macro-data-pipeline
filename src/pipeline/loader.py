"""
Loader — writes data to Google BigQuery.

Uses MERGE (upsert) operation:
- if (series_id, obs_date) already exists → updates the value
- if it does not exist → inserts a new row

Thanks to MERGE the pipeline is idempotent — it can be run multiple times
on the same data without creating duplicates.

The BigQuery table schema is managed by infra/bq_schema/observations.sql.
The loader assumes the table already exists (created on first deployment).
"""

import logging
import time

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from pipeline.config import BQ_DATASET, BQ_TABLE, GCP_PROJECT_ID
from pipeline.models import Observation

logger = logging.getLogger(__name__)


class BigQueryLoader:
    """
    Writes observations to the BigQuery table using MERGE (upsert).

    Usage:
        loader = BigQueryLoader()
        loader.ensure_table_exists()
        loaded = loader.load(observations)
    """

    def __init__(self):
        # Creates a BigQuery client — uses Application Default Credentials
        # Locally: GOOGLE_APPLICATION_CREDENTIALS or `gcloud auth application-default login`
        # On Cloud Run: automatically uses the service account attached to the instance
        self.client = bigquery.Client(project=GCP_PROJECT_ID)
        self.table_ref = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

    def ensure_dataset_exists(self) -> None:
        """Creates the BigQuery dataset if it does not exist."""
        dataset_ref = bigquery.Dataset(f"{GCP_PROJECT_ID}.{BQ_DATASET}")
        dataset_ref.location = "EU"
        try:
            self.client.get_dataset(dataset_ref)
            logger.debug(f"Dataset {BQ_DATASET} already exists")
        except NotFound:
            self.client.create_dataset(dataset_ref, exists_ok=True)
            logger.info(f"Created dataset: {BQ_DATASET}")

    def ensure_table_exists(self) -> None:
        """
        Creates the observations table if it does not exist.
        The schema defined here must match infra/bq_schema/observations.sql.
        """
        self.ensure_dataset_exists()

        schema = [
            bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("dataset_code", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("series_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("indicator_code", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("country_code", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("frequency", "STRING"),
            bigquery.SchemaField("obs_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("obs_value", "FLOAT64"),
            bigquery.SchemaField("unit", "STRING"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        ]

        table = bigquery.Table(self.table_ref, schema=schema)

        # Partition by obs_date — BigQuery scans only selected dates (cost saving)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="obs_date",
        )

        # Clustering — physical grouping of data for fast filtering
        table.clustering_fields = ["source", "country_code", "indicator_code"]

        self.client.create_table(table, exists_ok=True)
        logger.info(f"Table ready: {self.table_ref}")

    def load(self, observations: list[Observation]) -> int:
        """
        Writes observations to BigQuery using MERGE (upsert).

        Strategy:
        1. Load all records into a staging table (batch load — no cost)
        2. Run MERGE: update if exists, insert if new
        3. Drop the staging table (always, even on error)

        Unique key: (series_id, obs_date)

        Args:
            observations: List of Observation objects to write.

        Returns:
            Number of records inserted or updated.
        """
        if not observations:
            logger.info("No observations to write")
            return 0

        rows = [obs.to_bq_row() for obs in observations]
        staging_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}._staging_{int(time.time())}"

        try:
            self._load_to_staging(rows, staging_id)
            merged = self._run_merge(staging_id)
            logger.info(f"Write complete. Total loaded: {merged} records")
            return merged
        finally:
            # Always drop the staging table — even on error
            self.client.delete_table(staging_id, not_found_ok=True)
            logger.debug(f"Dropped staging table: {staging_id}")

    def _load_to_staging(self, rows: list[dict], staging_id: str) -> None:
        """Loads records into a temporary staging table (free batch load)."""
        schema = [
            bigquery.SchemaField("source", "STRING"),
            bigquery.SchemaField("dataset_code", "STRING"),
            bigquery.SchemaField("series_id", "STRING"),
            bigquery.SchemaField("indicator_code", "STRING"),
            bigquery.SchemaField("country_code", "STRING"),
            bigquery.SchemaField("frequency", "STRING"),
            bigquery.SchemaField("obs_date", "DATE"),
            bigquery.SchemaField("obs_value", "FLOAT64"),
            bigquery.SchemaField("unit", "STRING"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
        ]
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        job = self.client.load_table_from_json(rows, staging_id, job_config=job_config)
        job.result()
        logger.info(f"Staging table loaded: {len(rows)} records")

    def _run_merge(self, staging_id: str) -> int:
        """
        Runs MERGE between the staging table and the main table.

        WHEN MATCHED → updates obs_value and ingested_at
        WHEN NOT MATCHED → inserts a new row
        """
        merge_sql = f"""
        MERGE `{self.table_ref}` AS target
        USING (
          SELECT
            ANY_VALUE(source) AS source,
            ANY_VALUE(dataset_code) AS dataset_code,
            series_id,
            ANY_VALUE(indicator_code) AS indicator_code,
            ANY_VALUE(country_code) AS country_code,
            ANY_VALUE(frequency) AS frequency,
            obs_date,
            ANY_VALUE(obs_value) AS obs_value,
            ANY_VALUE(unit) AS unit,
            MAX(ingested_at) AS ingested_at
          FROM `{staging_id}`
          GROUP BY series_id, obs_date
        ) AS source
        ON target.series_id = source.series_id
           AND target.obs_date = source.obs_date
        WHEN MATCHED THEN
          UPDATE SET
            obs_value = source.obs_value,
            ingested_at = source.ingested_at
        WHEN NOT MATCHED THEN
          INSERT (source, dataset_code, series_id, indicator_code, country_code,
                  frequency, obs_date, obs_value, unit, ingested_at)
          VALUES (source.source, source.dataset_code, source.series_id,
                  source.indicator_code, source.country_code, source.frequency,
                  source.obs_date, source.obs_value, source.unit, source.ingested_at)
        """
        job = self.client.query(merge_sql)
        job.result()
        return job.num_dml_affected_rows or 0
