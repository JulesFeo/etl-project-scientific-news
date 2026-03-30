import uuid
from datetime import date, datetime

from src.logger import setup_logger
from src.extractors import EXTRACTORS
from src.transform import transform
from src.load import init_db, save_etl_run, load


def run(config: dict) -> dict:
    """Execute the ETL pipeline for all enabled sources.

    Returns a results dict:
        {"etl_id": str, "date": str, "sources": {name: {status, records_loaded, articles}}}
    """
    etl_id = str(uuid.uuid4())
    logger = setup_logger(config, etl_id)

    logger.info("=== ETL pipeline started ===")

    init_db(config, logger)

    sources = config.get("sources", {})
    total_loaded = 0

    target_date = date.today()

    results: dict = {
        "etl_id": etl_id,
        "date": target_date.isoformat(),
        "sources": {},
    }

    logger.info("Fetching articles for date: %s", target_date)

    for source_name, source_cfg in sources.items():
        if not source_cfg.get("enabled", False):
            logger.info("[%s] disabled — skipping", source_name)
            continue

        if source_name not in EXTRACTORS:
            logger.warning("[%s] no extractor registered — skipping", source_name)
            continue

        started_at = datetime.utcnow().isoformat()
        start_date_str = target_date.isoformat()
        end_date_str = target_date.isoformat()

        try:
            save_etl_run(config, etl_id, source_name, start_date_str, end_date_str,
                         "running", started_at)

            extractor_cls = EXTRACTORS[source_name]
            extractor = extractor_cls(source_cfg, config["retry"], logger)
            raw_data = extractor.extract(target_date, target_date)

            transformed = transform(raw_data, logger, etl_id, source_name)

            records_loaded = load(config, logger, transformed)
            total_loaded += records_loaded

            save_etl_run(config, etl_id, source_name, start_date_str, end_date_str,
                         "success", started_at,
                         finished_at=datetime.utcnow().isoformat(),
                         records_loaded=records_loaded)

            article_summaries = [
                {"title": a["title"], "url": a.get("url", "")}
                for a in transformed.get("articles", [])
            ]
            results["sources"][source_name] = {
                "status": "success",
                "records_loaded": records_loaded,
                "articles": article_summaries,
            }

        except Exception as exc:
            logger.error("[%s] failed: %s", source_name, exc, exc_info=True)
            save_etl_run(config, etl_id, source_name, start_date_str, end_date_str,
                         "failed", started_at,
                         finished_at=datetime.utcnow().isoformat())
            results["sources"][source_name] = {
                "status": "failed",
                "records_loaded": 0,
                "articles": [],
                "error": str(exc),
            }

    logger.info("=== ETL pipeline finished: %d total records loaded ===", total_loaded)
    return results
