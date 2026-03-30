import logging
import os
from datetime import datetime

import requests


TG_MAX_LENGTH = 4096
REPORT_DIR = "data"


def _split_message(text: str, max_len: int = TG_MAX_LENGTH) -> list[str]:
    """Split a long message into chunks that fit Telegram's limit."""
    if len(text) <= max_len:
        return [text]

    chunks: list[str] = []
    while text:
        if len(text) <= max_len:
            chunks.append(text)
            break
        cut = text.rfind("\n", 0, max_len)
        if cut <= 0:
            cut = max_len
        chunks.append(text[:cut])
        text = text[cut:].lstrip("\n")
    return chunks


def _build_message(results: dict) -> str:
    """Build an HTML-formatted Telegram message from ETL results."""
    lines: list[str] = []

    run_date = results.get("date", "???")
    lines.append(f"<b>ETL Report  {run_date}</b>\n")

    sources = results.get("sources", {})
    if not sources:
        lines.append("No enabled sources were processed.")
        return "\n".join(lines)

    total_found = 0
    total_new = 0
    for src_name, src_data in sources.items():
        status = src_data.get("status", "unknown")
        new_count = src_data.get("records_loaded", 0)
        articles = src_data.get("articles", [])
        found_count = len(articles)
        total_found += found_count
        total_new += new_count

        status_icon = {"success": "+", "no_new_data": "=", "failed": "!"}.get(status, "?")

        if status == "no_new_data":
            lines.append(f"<b>[{status_icon}] {src_name}</b>: нет новых статей на эту дату\n")
            continue

        if status == "failed":
            err = src_data.get("error", "unknown error")
            lines.append(f"<b>[{status_icon}] {src_name}</b>: ошибка")
            lines.append(f"    {err}\n")
            continue

        lines.append(f"<b>[{status_icon}] {src_name}</b>: найдено {found_count}, новых {new_count}")

        if not articles:
            lines.append("    Статей не найдено.\n")
            continue

        for i, art in enumerate(articles, 1):
            title = art.get("title", "Untitled")
            url = art.get("url", "")
            if url:
                lines.append(f'  {i}. <a href="{url}">{title}</a>')
            else:
                lines.append(f"  {i}. {title}")
        lines.append("")

    lines.append(f"<b>Итого: найдено {total_found}, новых {total_new}</b>")
    return "\n".join(lines)


def _build_plain_report(results: dict) -> str:
    """Build a plain-text report for local file output."""
    lines: list[str] = []

    run_date = results.get("date", "???")
    lines.append(f"ETL Report  {run_date}")
    lines.append("=" * 60)

    sources = results.get("sources", {})
    if not sources:
        lines.append("No enabled sources were processed.")
        return "\n".join(lines)

    total_found = 0
    total_new = 0
    for src_name, src_data in sources.items():
        status = src_data.get("status", "unknown")
        new_count = src_data.get("records_loaded", 0)
        articles = src_data.get("articles", [])
        found_count = len(articles)
        total_found += found_count
        total_new += new_count

        if status == "no_new_data":
            lines.append(f"\n[{src_name}] нет новых статей на эту дату")
            lines.append("-" * 40)
            continue

        if status == "failed":
            err = src_data.get("error", "unknown error")
            lines.append(f"\n[{src_name}] ошибка: {err}")
            lines.append("-" * 40)
            continue

        lines.append(f"\n[{src_name}] найдено {found_count}, новых {new_count}")
        lines.append("-" * 40)

        if not articles:
            lines.append("  Статей не найдено.")
            continue

        for i, art in enumerate(articles, 1):
            title = art.get("title", "Untitled")
            url = art.get("url", "")
            lines.append(f"  {i}. {title}")
            if url:
                lines.append(f"     {url}")

    lines.append(f"\n{'=' * 60}")
    lines.append(f"Итого: найдено {total_found}, новых {total_new}")
    return "\n".join(lines)


def save_report(results: dict, logger: logging.LoggerAdapter) -> str:
    """Save ETL results to a local text file. Always runs regardless of Telegram."""
    os.makedirs(REPORT_DIR, exist_ok=True)
    report = _build_plain_report(results)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(REPORT_DIR, f"report_{ts}.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write(report)
    logger.info("Report saved to %s", path)
    return path


def send_telegram(config: dict, results: dict, logger: logging.LoggerAdapter) -> None:
    """Send ETL results to a Telegram chat and save a local report."""
    save_report(results, logger)

    tg_cfg = config.get("telegram", {})
    if not tg_cfg.get("enabled", False):
        logger.info("Telegram notifications disabled")
        return

    bot_token = tg_cfg.get("bot_token", "")
    chat_id = tg_cfg.get("chat_id", "")
    if not bot_token or not chat_id:
        logger.warning("Telegram bot_token or chat_id not configured")
        return

    proxies = None
    proxy_url = tg_cfg.get("proxy", "")
    if proxy_url:
        proxies = {"https": proxy_url, "http": proxy_url}
        logger.info("Using proxy for Telegram: %s", proxy_url.split("@")[-1])

    message = _build_message(results)
    chunks = _split_message(message)

    api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    for i, chunk in enumerate(chunks, 1):
        payload = {
            "chat_id": chat_id,
            "text": chunk,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        try:
            resp = requests.post(api_url, json=payload, timeout=30, proxies=proxies)
            if resp.ok:
                logger.info("Telegram message chunk %d/%d sent", i, len(chunks))
            else:
                logger.error("Telegram API error %d: %s", resp.status_code, resp.text)
        except requests.RequestException as exc:
            logger.error("Failed to send Telegram message: %s", exc)
