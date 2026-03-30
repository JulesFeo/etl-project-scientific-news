import logging
import os
from logging.handlers import RotatingFileHandler


class EtlIdFilter(logging.Filter):
    """Inject etl_id into every log record, even those not sent via LoggerAdapter."""

    def __init__(self, etl_id: str):
        super().__init__()
        self.etl_id = etl_id

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "etl_id"):
            record.etl_id = self.etl_id
        return True


def setup_logger(config: dict, etl_id: str) -> logging.LoggerAdapter:
    """Configure logging with etl_id propagated to every log record."""
    log_cfg = config["logging"]
    log_level = getattr(logging, log_cfg["level"].upper(), logging.INFO)
    log_file = log_cfg["log_file"]

    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    logger = logging.getLogger("etl")
    logger.setLevel(log_level)
    logger.handlers.clear()
    logger.filters.clear()

    logger.addFilter(EtlIdFilter(etl_id))

    fmt = "%(asctime)s | %(levelname)-8s | etl_id=%(etl_id)s | %(message)s"
    formatter = logging.Formatter(fmt, datefmt="%Y-%m-%d %H:%M:%S")

    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = RotatingFileHandler(
        log_file, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    adapter = logging.LoggerAdapter(logger, extra={"etl_id": etl_id})
    return adapter
