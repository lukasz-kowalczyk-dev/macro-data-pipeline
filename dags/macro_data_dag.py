"""
Apache Airflow DAG — macro-data-pipeline.

Fetches macroeconomic data from IMF and OECD, transforms it,
and writes it to Google BigQuery. Runs weekly.

DAG structure:
  imf_weo ──┐
             ├──► (parallel, independent)
  oecd_qna ─┘

Local run (requires Airflow installed):
  export AIRFLOW_HOME=~/airflow
  airflow db migrate
  airflow dags test macro_data_pipeline 2024-01-01

Run with Docker Compose (recommended):
  docker compose -f docker-compose.airflow.yml up
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline.config import DATASETS
from pipeline.fetchers.base import FetchError
from pipeline.fetchers.imf import IMFFetcher
from pipeline.fetchers.oecd import OECDFetcher
from pipeline.loader import BigQueryLoader
from pipeline.transformers import imf as imf_transformer
from pipeline.transformers import oecd as oecd_transformer

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Fetcher and transformer singletons (created once, shared)
# ---------------------------------------------------------------------------

_FETCHERS = {
    "IMF": IMFFetcher(),
    "OECD": OECDFetcher(),
}

_TRANSFORMERS = {
    "IMF": imf_transformer.transform,
    "OECD": oecd_transformer.transform,
}


# ---------------------------------------------------------------------------
# Function called by each Airflow task
# ---------------------------------------------------------------------------


def run_dataset(source: str, dataset_code: str) -> dict:
    """
    Runs ETL (fetch → transform → load) for one dataset.

    Airflow calls this function as a PythonOperator.
    On error it raises an exception → Airflow marks the task as FAILED
    and retries according to default_args.

    Args:
        source:       'IMF' or 'OECD'
        dataset_code: e.g. 'WEO', 'QNA'

    Returns:
        Dictionary with statistics (logged by Airflow).
    """
    # Find dataset configuration
    config = next(
        (d for d in DATASETS if d.source == source and d.dataset_code == dataset_code),
        None,
    )
    if config is None:
        raise ValueError(f"Unknown dataset: {source}/{dataset_code}")

    logger.info(f"[{source}/{dataset_code}] START")

    # EXTRACT
    fetcher = _FETCHERS[source]
    try:
        raw_records = fetcher.fetch(config)
    except FetchError as e:
        raise RuntimeError(f"[{source}/{dataset_code}] Fetch error: {e}") from e

    logger.info(f"[{source}/{dataset_code}] Fetched {len(raw_records)} records")

    # TRANSFORM
    transformer_fn = _TRANSFORMERS[source]
    observations = transformer_fn(raw_records)
    logger.info(f"[{source}/{dataset_code}] Transformed to {len(observations)} observations")

    # LOAD
    loader = BigQueryLoader()
    loader.ensure_table_exists()
    loaded = loader.load(observations)
    logger.info(f"[{source}/{dataset_code}] Wrote {loaded} records to BigQuery")

    return {
        "source": source,
        "dataset": dataset_code,
        "fetched": len(raw_records),
        "transformed": len(observations),
        "loaded": loaded,
    }


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    # Retry failed tasks up to 3 times
    "retries": 3,
    # Wait 5 minutes between attempts (APIs may have transient issues)
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="macro_data_pipeline",
    default_args=default_args,
    description="Fetches macroeconomic data from IMF and OECD → BigQuery",
    # Run every Monday at 06:00 UTC
    schedule="0 6 * * 1",
    # Earliest date from which the DAG can be triggered
    start_date=datetime(2024, 1, 1),
    # Do not run missed runs if the DAG was paused
    catchup=False,
    tags=["macro", "etl", "imf", "oecd", "bigquery"],
) as dag:
    # Task 1 — IMF World Economic Outlook
    # Annual data: GDP growth, inflation, debt, unemployment
    imf_weo = PythonOperator(
        task_id="imf_weo",
        python_callable=run_dataset,
        op_kwargs={"source": "IMF", "dataset_code": "WEO"},
    )

    # Task 2 — OECD Quarterly National Accounts
    # Quarterly data: GDP (volume, seasonally adjusted)
    oecd_qna = PythonOperator(
        task_id="oecd_qna",
        python_callable=run_dataset,
        op_kwargs={"source": "OECD", "dataset_code": "QNA"},
    )

    # Both tasks are independent — Airflow will run them in parallel
    # In the UI: imf_weo and oecd_qna side by side (no arrow between them)
    [imf_weo, oecd_qna]
