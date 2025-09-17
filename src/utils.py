from __future__ import annotations
import re
import logging
import os
from datetime import datetime
from typing import Any, Optional


def get_logger(name: str = "parser_vtb") -> logging.Logger:
    level_name = os.getenv("PARSER_LOGLEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        handler.setFormatter(logging.Formatter(fmt))
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger

logger = get_logger()

DATE_RE = re.compile(r"\d{2}[,.]\d{2}[,.]\d{4}")

def format_date_from_match(value: str) -> str:
    return value.replace(",", ".")

def extract_date(value: Any) -> Optional[str]:
    """
    Попытка получить дату в формате DD.MM.YYYY из значения ячейки.
    Поддерживает datetime и строки с разделителем ',' или '.'
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%d.%m.%Y")
    s = str(value).strip()
    if not s:
        return None
    m = DATE_RE.search(s)
    if m:
        return format_date_from_match(m.group(0))
    return None

def to_num_safe(v: Any) -> float:
    """
    Пытаемся превратить значение в float, возвращаем 0.0 при ошибке / пустом значении.
    Убирает NBSP, пробелы, заменяет запятые на точки.
    """
    if v is None:
        return 0.0
    try:
        s = str(v).replace("\u00A0", " ").replace(" ", "").replace(",", ".")
        return float(s) if s not in ("", "-", "--") else 0.0
    except Exception:
        try:
            return float(str(v).replace(",", "."))
        except Exception:
            return 0.0


def to_int_safe(v: Any) -> int:
    """
    Аналогично, безопасно в int (через float).
    """
    try:
        return int(round(float(str(v).replace("\u00A0", " ").replace(" ", "").replace(",", ".") or 0.0)))
    except Exception:
        return 0