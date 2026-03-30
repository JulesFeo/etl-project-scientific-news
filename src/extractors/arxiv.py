import logging
import time
from datetime import date

import feedparser
import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from src.extractors.base import BaseExtractor


class ArxivExtractor(BaseExtractor):
    """Extract preprints from arXiv API (Atom XML via feedparser)."""

    def _build_arxiv_retry(self):
        """arXiv needs longer waits due to aggressive rate limiting."""
        return retry(
            stop=stop_after_attempt(5),
            wait=wait_exponential(min=5, max=60),
            retry=retry_if_exception_type((requests.RequestException, ConnectionError)),
            before_sleep=before_sleep_log(self.logger.logger, logging.WARNING),
            reraise=True,
        )

    def extract(self, start_date: date, end_date: date) -> list[dict]:
        cfg = self.config
        categories = cfg.get("categories", ["cs.AI"])
        max_results = cfg.get("max_results", 50)
        delay = cfg.get("request_delay", 3)

        fetch = self._build_arxiv_retry()(self._fetch_page)

        all_records: list[dict] = []

        for cat in categories:
            if len(all_records) >= max_results:
                break

            time.sleep(delay)

            remaining = max_results - len(all_records)
            records = fetch(cat, start_date, end_date, remaining)
            all_records.extend(records)
            self.logger.info("arXiv [%s]: fetched %d papers", cat, len(records))

        self.logger.info("arXiv extract complete: %d records", len(all_records))
        return all_records

    def _fetch_page(self, category: str, start_date: date, end_date: date, max_results: int) -> list[dict]:
        s = start_date.strftime("%Y%m%d") + "0000"
        e = end_date.strftime("%Y%m%d") + "2359"
        query = f"cat:{category} AND submittedDate:[{s} TO {e}]"

        params = {
            "search_query": query,
            "start": 0,
            "max_results": max_results,
            "sortBy": "submittedDate",
            "sortOrder": "descending",
        }
        self.logger.info("arXiv: requesting %s (%s -> %s)", category, start_date, end_date)
        resp = requests.get(self.config["base_url"], params=params, timeout=self.timeout)
        resp.raise_for_status()

        feed = feedparser.parse(resp.text)
        return [self._normalize(entry) for entry in feed.entries]

    def _normalize(self, entry) -> dict:
        arxiv_id = entry.get("id", "")
        short_id = arxiv_id.split("/abs/")[-1] if "/abs/" in arxiv_id else arxiv_id

        authors = []
        for author in entry.get("authors", []):
            authors.append({"name": author.get("name", ""), "orcid": None})

        tags = []
        for tag in entry.get("tags", []):
            tags.append({"name": tag.get("term", ""), "score": None})

        pdf_url = ""
        for link in entry.get("links", []):
            if link.get("type") == "application/pdf":
                pdf_url = link.get("href", "")
                break

        return {
            "external_id": short_id,
            "title": entry.get("title", "").replace("\n", " ").strip(),
            "abstract": entry.get("summary", "").replace("\n", " ").strip(),
            "url": pdf_url or entry.get("link", ""),
            "doi": None,
            "language": "en",
            "published_at": entry.get("published", "")[:10],
            "sentiment_score": None,
            "authors": authors,
            "tags": tags,
        }
