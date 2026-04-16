"""
Base class for all fetchers.

Contains shared logic:
- HTTP GET with automatic retry
- API error handling (429 Too Many Requests, 5xx Server Error)
- logging

Each concrete fetcher (OECDFetcher, IMFFetcher) inherits from BaseFetcher
and implements the fetch() method returning raw data from the API.
"""

import logging
import time
from abc import ABC, abstractmethod

import requests
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from pipeline.config import DatasetConfig

logger = logging.getLogger(__name__)

# Timeout for a single HTTP request (seconds)
REQUEST_TIMEOUT = 30

# HTTP headers sent with every request
DEFAULT_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "macro-data-pipeline/0.1 (github.com/lukasz-kowalczyk-dev/macro-data-pipeline)",
}


class FetchError(Exception):
    """Exception raised when a fetcher cannot retrieve data."""

    pass


class BaseFetcher(ABC):
    """
    Abstract base class for fetchers.

    Subclasses must implement:
    - build_url(config) → str
    - parse_response(response_json, config) → list[dict]
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)

    @abstractmethod
    def build_url(self, config: DatasetConfig) -> str:
        """Builds the API URL from the dataset configuration."""
        ...

    @abstractmethod
    def parse_response(self, response_json: dict, config: DatasetConfig) -> list[dict]:
        """
        Parses the raw JSON response from the API.
        Returns a list of dictionaries — raw data before transformation.
        """
        ...

    def fetch(self, config: DatasetConfig) -> list[dict]:
        """
        Fetches data from the API for the given dataset.
        Automatically handles retries on network errors and rate limiting.

        Returns:
            List of raw dictionaries with data.

        Raises:
            FetchError: when data cannot be fetched after all retry attempts.
        """
        url = self.build_url(config)
        logger.info(f"[{config.source}] Fetching: {url}")

        try:
            response_json = self._get_with_retry(url)
        except Exception as e:
            raise FetchError(
                f"Failed to fetch data from {config.source} for dataset {config.dataset_code}: {e}"
            ) from e

        records = self.parse_response(response_json, config)
        logger.info(
            f"[{config.source}] Fetched {len(records)} records from dataset {config.dataset_code}"
        )
        return records

    @retry(
        # Retry on network errors and HTTP errors
        retry=retry_if_exception_type(
            (requests.ConnectionError, requests.Timeout, requests.HTTPError)
        ),
        # Maximum 4 attempts (1 original + 3 retries)
        stop=stop_after_attempt(4),
        # Exponential backoff: 2s, 4s, 8s
        wait=wait_exponential(multiplier=2, min=2, max=30),
        # Log each attempt
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _get_with_retry(self, url: str) -> dict:
        """
        Performs HTTP GET with retry logic.

        Exponential backoff means: if the API responds with an error,
        we wait 2s, then 4s, then 8s before the next attempt.
        This avoids overloading the server during rate limiting (HTTP 429).
        """
        response = self.session.get(url, timeout=REQUEST_TIMEOUT)

        # Handle rate limiting — wait as long as the server requests
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 10))
            logger.warning(f"Rate limit (429). Waiting {retry_after}s...")
            time.sleep(retry_after)
            response.raise_for_status()  # raise exception → trigger retry

        # Raise exception for 4xx/5xx errors (except 429 already handled)
        response.raise_for_status()

        return response.json()
