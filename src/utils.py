from __future__ import annotations
import re
import logging
import os
from datetime import datetime
from typing import Any, Optional

# Логгер (уровень можно задать через PARSER_LOGLEVEL)
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

# Регулярка для поиска даты в строке (DD.MM.YYYY или DD,MM,YYYY)
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
