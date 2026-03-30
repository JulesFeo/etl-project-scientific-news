import logging
from abc import ABC, abstractmethod
from datetime import date

import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)


class BaseExtractor(ABC):
    """Abstract base for all source extractors."""

    def __init__(self, source_config: dict, retry_config: dict, logger: logging.LoggerAdapter):
        self.config = source_config
        self.retry_config = retry_config
        self.logger = logger
        self.timeout = source_config.get("timeout", 60)

    def _build_retry(self):
        return retry(
            stop=stop_after_attempt(self.retry_config["max_attempts"]),
            wait=wait_exponential(
                min=self.retry_config["min_wait"],
                max=self.retry_config["max_wait"],
            ),
            retry=retry_if_exception_type((requests.RequestException, ConnectionError)),
            before_sleep=before_sleep_log(self.logger.logger, logging.WARNING),
            reraise=True,
        )

    @abstractmethod
    def extract(self, start_date: date, end_date: date) -> list[dict]:
        """Fetch records from the source for the given date range.

        Each dict must contain at minimum:
            external_id, title, url, published_at
        And optionally:
            abstract, doi, language, sentiment_score,
            authors: list[{name, orcid}],
            tags: list[{name, score}]
        """
