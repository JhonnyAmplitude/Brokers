from __future__ import annotations
import re
import pandas as pd
from typing import Optional
from src.utils import logger, extract_date

PERIOD_RE = re.compile(
    r"за период с (\d{2}\.\d{2}\.\d{4}) по (\d{2}\.\d{2}\.\d{4})", re.IGNORECASE
)
SUBACCOUNT_RE = re.compile(r"№\s*субсчета[:\s]*([0-9\-]+)", re.IGNORECASE)


def parse_header(file_path: str) -> dict:
    """
    Читает верхнюю часть xlsx через pandas (header=None) и извлекает:
      - account_id (№ субсчета)
      - account_date_start (дата соглашения рядом с 'о предоставлении услуг')
      - date_start / date_end (период отчёта)
    """
    df = pd.read_excel(file_path, header=None)
    df = df.fillna("")

    account_id: Optional[str] = None
    account_date_start: Optional[str] = None
    date_start: Optional[str] = None
    date_end: Optional[str] = None

    for _, row in df.iterrows():
        cells = [str(c).strip() for c in row if str(c).strip()]
        if not cells:
            continue
        joined = " ".join(cells).lower()

        if not (date_start and date_end):
            m = PERIOD_RE.search(joined)
            if m:
                date_start, date_end = m.group(1), m.group(2)
                logger.debug("Found period: %s - %s", date_start, date_end)

        if "о предоставлении услуг" in joined and not account_date_start:
            account_date_start = next(
                (d for cell in row for d in [extract_date(cell)] if d),
                None,
            )
            logger.debug("Found agreement date: %s", account_date_start)

        if not account_id:
            m2 = SUBACCOUNT_RE.search(joined)
            if m2:
                account_id = m2.group(1)
                logger.debug("Found account id: %s", account_id)

        if account_id and account_date_start and date_start and date_end:
            break

    result = {
        "account_id": account_id,
        "account_date_start": account_date_start,
        "date_start": date_start,
        "date_end": date_end,
    }
    logger.info("Header parsed: %s", result)
    return result
