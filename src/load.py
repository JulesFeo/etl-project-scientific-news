import logging
import os
import sqlite3
from datetime import date

DDL = """
CREATE TABLE IF NOT EXISTS sources (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    name      TEXT UNIQUE NOT NULL,
    base_url  TEXT
);

CREATE TABLE IF NOT EXISTS articles (
    id              TEXT PRIMARY KEY,
    source_id       INTEGER NOT NULL REFERENCES sources(id),
    external_id     TEXT NOT NULL,
    title           TEXT NOT NULL,
    abstract        TEXT,
    url             TEXT,
    doi             TEXT,
    language        TEXT,
    published_at    TEXT,
    sentiment_score REAL,
    etl_id          TEXT NOT NULL,
    loaded_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS authors (
    id    INTEGER PRIMARY KEY AUTOINCREMENT,
    name  TEXT NOT NULL,
    orcid TEXT,
    UNIQUE(name, orcid)
);

CREATE TABLE IF NOT EXISTS article_authors (
    article_id TEXT NOT NULL REFERENCES articles(id),
    author_id  INTEGER NOT NULL REFERENCES authors(id),
    position   INTEGER DEFAULT 0,
    PRIMARY KEY (article_id, author_id)
);

CREATE TABLE IF NOT EXISTS tags (
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
    name   TEXT NOT NULL,
    source TEXT,
    UNIQUE(name, source)
);

CREATE TABLE IF NOT EXISTS article_tags (
    article_id TEXT NOT NULL REFERENCES articles(id),
    tag_id     INTEGER NOT NULL REFERENCES tags(id),
    score      REAL,
    PRIMARY KEY (article_id, tag_id)
);

CREATE TABLE IF NOT EXISTS etl_runs (
    etl_id          TEXT NOT NULL,
    source_name     TEXT NOT NULL,
    start_date      TEXT,
    end_date        TEXT,
    records_loaded  INTEGER DEFAULT 0,
    status          TEXT NOT NULL,
    started_at      TEXT NOT NULL,
    finished_at     TEXT,
    PRIMARY KEY (etl_id, source_name)
);
"""

SEED_SOURCES = [
    ("openalex", "https://api.openalex.org/works"),
    ("arxiv", "http://export.arxiv.org/api/query"),
    ("pubmed", "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"),
]


def _get_connection(config: dict) -> sqlite3.Connection:
    db_path = config["database"]["path"]
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_db(config: dict, logger: logging.LoggerAdapter) -> None:
    conn = _get_connection(config)
    try:
        conn.executescript(DDL)
        for name, url in SEED_SOURCES:
            conn.execute(
                "INSERT OR IGNORE INTO sources (name, base_url) VALUES (?, ?)",
                (name, url),
            )
        conn.commit()
        logger.info("Database initialized at %s", config["database"]["path"])
    finally:
        conn.close()


def get_source_id(config: dict, source_name: str) -> int:
    conn = _get_connection(config)
    try:
        row = conn.execute("SELECT id FROM sources WHERE name = ?", (source_name,)).fetchone()
        if not row:
            raise ValueError(f"Unknown source: {source_name}")
        return row[0]
    finally:
        conn.close()


def get_last_loaded_date(config: dict, source_name: str) -> date | None:
    """Return the max published date for a specific source, or None."""
    conn = _get_connection(config)
    try:
        row = conn.execute(
            """
            SELECT MAX(DATE(a.published_at))
            FROM articles a
            JOIN sources s ON a.source_id = s.id
            WHERE s.name = ?
            """,
            (source_name,),
        ).fetchone()
        if row and row[0]:
            return date.fromisoformat(row[0])
        return None
    finally:
        conn.close()


def save_etl_run(
    config: dict,
    etl_id: str,
    source_name: str,
    start_date: str,
    end_date: str,
    status: str,
    started_at: str,
    finished_at: str | None = None,
    records_loaded: int = 0,
) -> None:
    conn = _get_connection(config)
    try:
        conn.execute(
            """
            INSERT INTO etl_runs (etl_id, source_name, start_date, end_date,
                                  records_loaded, status, started_at, finished_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(etl_id, source_name) DO UPDATE SET
                records_loaded = excluded.records_loaded,
                status = excluded.status,
                finished_at = excluded.finished_at
            """,
            (etl_id, source_name, start_date, end_date, records_loaded, status,
             started_at, finished_at),
        )
        conn.commit()
    finally:
        conn.close()


def load(config: dict, logger: logging.LoggerAdapter, data: dict) -> int:
    """Insert normalized data into all related tables in a single transaction."""
    articles = data["articles"]
    if not articles:
        logger.info("No records to load for %s", data["source_name"])
        return 0

    source_name = data["source_name"]
    source_id = get_source_id(config, source_name)

    conn = _get_connection(config)
    inserted = 0
    try:
        cur = conn.cursor()

        # 1. Authors — insert unique, build key->id mapping
        author_id_map: dict[str, int] = {}
        for key, author in data["authors_map"].items():
            cur.execute(
                "INSERT OR IGNORE INTO authors (name, orcid) VALUES (?, ?)",
                (author["name"], author["orcid"]),
            )
            row = cur.execute(
                "SELECT id FROM authors WHERE name = ? AND (orcid IS ? OR orcid = ?)",
                (author["name"], author["orcid"], author["orcid"]),
            ).fetchone()
            if row:
                author_id_map[key] = row[0]

        # 2. Tags — insert unique, build key->id mapping
        tag_id_map: dict[str, int] = {}
        for key, tag_name in data["tags_map"].items():
            cur.execute(
                "INSERT OR IGNORE INTO tags (name, source) VALUES (?, ?)",
                (tag_name, source_name),
            )
            row = cur.execute(
                "SELECT id FROM tags WHERE name = ? AND source = ?",
                (tag_name, source_name),
            ).fetchone()
            if row:
                tag_id_map[key] = row[0]

        # 3. Articles
        for art in articles:
            cur.execute(
                """
                INSERT OR IGNORE INTO articles
                    (id, source_id, external_id, title, abstract, url, doi,
                     language, published_at, sentiment_score, etl_id, loaded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (art["id"], source_id, art["external_id"], art["title"],
                 art["abstract"], art["url"], art["doi"], art["language"],
                 art["published_at"], art["sentiment_score"],
                 art["etl_id"], art["loaded_at"]),
            )
            inserted += cur.rowcount

        # 4. Article-Author links
        for link in data["article_authors"]:
            author_id = author_id_map.get(link["author_key"])
            if author_id:
                cur.execute(
                    "INSERT OR IGNORE INTO article_authors (article_id, author_id, position) VALUES (?, ?, ?)",
                    (link["article_id"], author_id, link["position"]),
                )

        # 5. Article-Tag links
        for link in data["article_tags"]:
            tag_id = tag_id_map.get(link["tag_key"])
            if tag_id:
                cur.execute(
                    "INSERT OR IGNORE INTO article_tags (article_id, tag_id, score) VALUES (?, ?, ?)",
                    (link["article_id"], tag_id, link["score"]),
                )

        conn.commit()
        logger.info("Load [%s]: %d articles inserted", source_name, inserted)
    finally:
        conn.close()

    return inserted
