import logging
from datetime import datetime


REQUIRED_FIELDS = {"external_id", "title", "published_at"}


def transform(
    raw_records: list[dict],
    logger: logging.LoggerAdapter,
    etl_id: str,
    source_name: str,
) -> dict:
    """Normalize extractor output into relational structure.

    Returns dict with keys: articles, authors, article_authors, tags, article_tags
    """
    loaded_at = datetime.utcnow().isoformat()
    seen_ids: set[str] = set()

    articles = []
    authors_map: dict[str, dict] = {}
    tags_map: dict[str, str] = {}
    article_authors = []
    article_tags = []

    for i, rec in enumerate(raw_records):
        missing = REQUIRED_FIELDS - set(rec.keys())
        if missing:
            logger.warning("Record #%d: missing %s — skipped", i, missing)
            continue

        article_id = f"{source_name}:{rec['external_id']}"

        if article_id in seen_ids:
            logger.debug("Duplicate %s — skipped", article_id)
            continue
        seen_ids.add(article_id)

        title = (rec.get("title") or "").strip()
        if not title:
            logger.warning("Record #%d: empty title — skipped", i)
            continue

        articles.append({
            "id": article_id,
            "source_name": source_name,
            "external_id": rec["external_id"],
            "title": title,
            "abstract": (rec.get("abstract") or "").strip() or None,
            "url": rec.get("url"),
            "doi": rec.get("doi"),
            "language": rec.get("language"),
            "published_at": rec.get("published_at"),
            "sentiment_score": rec.get("sentiment_score"),
            "etl_id": etl_id,
            "loaded_at": loaded_at,
        })

        for pos, author in enumerate(rec.get("authors", [])):
            name = (author.get("name") or "").strip()
            if not name:
                continue
            orcid = author.get("orcid")
            author_key = f"{name}|{orcid or ''}"
            authors_map[author_key] = {"name": name, "orcid": orcid}
            article_authors.append({
                "article_id": article_id,
                "author_key": author_key,
                "position": pos,
            })

        for tag_entry in rec.get("tags", []):
            tag_name = (tag_entry.get("name") or "").strip()
            if not tag_name:
                continue
            tag_key = f"{tag_name}|{source_name}"
            tags_map[tag_key] = tag_name
            article_tags.append({
                "article_id": article_id,
                "tag_key": tag_key,
                "score": tag_entry.get("score"),
            })

    skipped = len(raw_records) - len(articles)
    if skipped:
        logger.warning("Transform [%s]: skipped %d records", source_name, skipped)
    logger.info("Transform [%s]: %d articles, %d authors, %d tags",
                source_name, len(articles), len(authors_map), len(tags_map))

    return {
        "articles": articles,
        "authors_map": authors_map,
        "tags_map": tags_map,
        "article_authors": article_authors,
        "article_tags": article_tags,
        "source_name": source_name,
    }
