"""
Pipeline entry point — HTTP server for Google Cloud Run.

Cloud Scheduler sends POST to /run → Flask calls run_pipeline().
Pipeline: fetcher → transformer → loader for each dataset from config.

Local run (without BigQuery):
  python -m pipeline.main --dry-run

Docker run:
  docker run -p 8080:8080 macro-data-pipeline
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timezone

from flask import Flask, Response, jsonify, request

from pipeline.config import DATASETS, DatasetConfig
from pipeline.fetchers.base import FetchError
from pipeline.fetchers.imf import IMFFetcher
from pipeline.fetchers.oecd import OECDFetcher
from pipeline.loader import BigQueryLoader
from pipeline.models import Observation
from pipeline.transformers import imf as imf_transformer
from pipeline.transformers import oecd as oecd_transformer

# Logging configuration — logs visible in Cloud Run Console
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Fetcher per source — singletons, created once
_FETCHERS = {
    "IMF": IMFFetcher(),
    "OECD": OECDFetcher(),
}

# Transformer per source — functions from transformers/ modules
_TRANSFORMERS = {
    "IMF": imf_transformer.transform,
    "OECD": oecd_transformer.transform,
}


def run_pipeline(dry_run: bool = False) -> dict:
    """
    Runs the full ETL pipeline for all datasets from configuration.

    Args:
        dry_run: If True — logs records without writing to BigQuery.

    Returns:
        Dictionary with statistics: records fetched/loaded per dataset.
    """
    logger.info("=" * 60)
    logger.info(f"Pipeline start | dry_run={dry_run} | {datetime.now(tz=timezone.utc).isoformat()}")
    logger.info("=" * 60)

    # In dry_run mode we skip BigQuery entirely
    loader = None if dry_run else BigQueryLoader()
    if loader is not None:
        loader.ensure_table_exists()

    results = []
    total_loaded = 0
    pipeline_start = time.time()

    for config in DATASETS:
        dataset_start = time.time()
        result = _process_dataset(config, loader, dry_run)
        result["duration_s"] = round(time.time() - dataset_start, 1)
        results.append(result)
        total_loaded += result.get("loaded", 0)

    elapsed = round(time.time() - pipeline_start, 1)
    summary = {
        "status": "ok",
        "total_loaded": total_loaded,
        "duration_s": elapsed,
        "dry_run": dry_run,
        "datasets": results,
        "finished_at": datetime.now(tz=timezone.utc).isoformat(),
    }

    logger.info("=" * 60)
    logger.info(f"Pipeline finished | total loaded: {total_loaded} | duration: {elapsed}s")
    logger.info("=" * 60)
    return summary


def _process_dataset(config: DatasetConfig, loader: BigQueryLoader | None, dry_run: bool) -> dict:
    """
    Processes one dataset: fetch → transform → load.

    Returns:
        Dictionary with status and statistics for this dataset.
    """
    tag = f"[{config.source}/{config.dataset_code}]"
    logger.info(f"{tag} START")

    try:
        # EXTRACT — fetch data from API
        fetcher = _FETCHERS[config.source]
        raw_records = fetcher.fetch(config)
        logger.info(f"{tag} Fetched {len(raw_records)} raw records")

        # TRANSFORM — convert to Observation objects
        transformer_fn = _TRANSFORMERS[config.source]
        observations: list[Observation] = transformer_fn(raw_records)
        logger.info(f"{tag} Transformed to {len(observations)} observations")

        # LOAD — write to BigQuery (loader=None when dry_run)
        loaded = loader.load(observations) if loader is not None else 0
        if dry_run:
            logger.info(f"{tag} [DRY RUN] {len(observations)} observations (no BigQuery write)")

        return {
            "source": config.source,
            "dataset": config.dataset_code,
            "fetched": len(raw_records),
            "transformed": len(observations),
            "loaded": loaded,
            "status": "ok",
        }

    except FetchError as e:
        logger.error(f"{tag} Fetch error: {e}")
        return {
            "source": config.source,
            "dataset": config.dataset_code,
            "status": "fetch_error",
            "error": str(e),
            "loaded": 0,
        }
    except Exception as e:
        logger.exception(f"{tag} Unexpected error: {e}")
        return {
            "source": config.source,
            "dataset": config.dataset_code,
            "status": "error",
            "error": str(e),
            "loaded": 0,
        }


# ---------------------------------------------------------------------------
# Flask routes
# ---------------------------------------------------------------------------


@app.route("/run", methods=["POST"])
def handle_run() -> tuple[Response, int]:
    """
    Endpoint called by Cloud Scheduler.
    Cloud Scheduler sends POST /run with an empty body or JSON.
    """
    dry_run = request.args.get("dry_run", "false").lower() == "true"
    summary = run_pipeline(dry_run=dry_run)
    status_code = 200 if summary["status"] == "ok" else 500
    return jsonify(summary), status_code


@app.route("/health", methods=["GET"])
def health() -> tuple[Response, int]:
    """Health check endpoint — Cloud Run checks whether the server is running."""
    return jsonify({"status": "ok"}), 200


# ---------------------------------------------------------------------------
# Local CLI mode (without starting the Flask server)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Macro Data Pipeline")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run pipeline without writing to BigQuery (test mode)",
    )
    parser.add_argument(
        "--serve",
        action="store_true",
        help="Start the HTTP server (Flask) instead of a one-shot run",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8080,
        help="HTTP server port (default: 8080)",
    )
    args = parser.parse_args()

    if args.serve:
        # Server mode — used by Cloud Run
        app.run(host="0.0.0.0", port=args.port, debug=False)
    else:
        # One-shot mode — e.g. local testing
        summary = run_pipeline(dry_run=args.dry_run)
        if summary["status"] != "ok":
            sys.exit(1)
