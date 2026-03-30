from datetime import date

import requests

from src.extractors.base import BaseExtractor


def _invert_abstract(inverted_index: dict | None) -> str | None:
    """Reconstruct plain text from OpenAlex abstract_inverted_index."""
    if not inverted_index:
        return None
    word_positions: list[tuple[int, str]] = []
    for word, positions in inverted_index.items():
        for pos in positions:
            word_positions.append((pos, word))
    word_positions.sort(key=lambda x: x[0])
    return " ".join(w for _, w in word_positions)


class OpenAlexExtractor(BaseExtractor):
    """Extract scientific works from OpenAlex API (JSON, cursor pagination)."""

    SELECT_FIELDS = (
        "id,title,doi,publication_date,authorships,topics,"
        "abstract_inverted_index,language,type"
    )

    def extract(self, start_date: date, end_date: date) -> list[dict]:
        cfg = self.config
        max_records = cfg.get("max_records", 50)
        per_page = cfg.get("per_page", 25)

        fetch = self._build_retry()(self._fetch_page)

        all_records: list[dict] = []
        cursor = "*"

        while cursor and len(all_records) < max_records:
            data = fetch(start_date, end_date, per_page, cursor)
            results = data.get("results", [])
            if not results:
                break

            for work in results:
                all_records.append(self._normalize(work))
                if len(all_records) >= max_records:
                    break

            cursor = data.get("meta", {}).get("next_cursor")
            self.logger.info(
                "OpenAlex: fetched %d/%d (available: %s)",
                len(all_records), max_records, data.get("meta", {}).get("count", "?"),
            )

        self.logger.info("OpenAlex extract complete: %d records", len(all_records))
        return all_records

    def _fetch_page(self, start_date: date, end_date: date, per_page: int, cursor: str) -> dict:
        params = {
            "filter": f"from_publication_date:{start_date},to_publication_date:{end_date}",
            "per_page": per_page,
            "cursor": cursor,
            "select": self.SELECT_FIELDS,
        }
        lang = self.config.get("language")
        if lang:
            params["filter"] += f",language:{lang}"

        headers = {"User-Agent": self.config.get("user_agent", "mailto:etl@example.com")}

        self.logger.info("OpenAlex: requesting cursor=%s", cursor[:20])
        resp = requests.get(self.config["base_url"], params=params, headers=headers, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def _normalize(self, work: dict) -> dict:
        openalex_id = work.get("id", "")
        short_id = openalex_id.rsplit("/", 1)[-1] if openalex_id else ""

        authors = []
        for auth_entry in work.get("authorships", []):
            author = auth_entry.get("author", {})
            authors.append({
                "name": author.get("display_name", ""),
                "orcid": (author.get("orcid") or "").rsplit("/", 1)[-1] or None,
            })

        tags = []
        for topic in work.get("topics", []):
            tags.append({
                "name": topic.get("display_name", ""),
                "score": topic.get("score"),
            })

        doi_raw = work.get("doi") or ""
        doi = doi_raw.replace("https://doi.org/", "") if doi_raw else None

        return {
            "external_id": short_id,
            "title": work.get("title") or "",
            "abstract": _invert_abstract(work.get("abstract_inverted_index")),
            "url": work.get("doi") or openalex_id,
            "doi": doi,
            "language": work.get("language"),
            "published_at": work.get("publication_date"),
            "sentiment_score": None,
            "authors": authors,
            "tags": tags,
        }
