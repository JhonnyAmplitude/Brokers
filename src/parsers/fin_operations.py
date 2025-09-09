from __future__ import annotations
import re
from typing import List, Optional, Dict, Any, Tuple

import pandas as pd

from src.utils import logger, extract_date
from src.OperationDTO import OperationDTO
import src.constants

# Patterns
ISIN_RE = re.compile(r"\b[A-Z]{2}[A-Z0-9]{9}\d\b", re.IGNORECASE)
REG_RE = re.compile(r"\b(?:reg|reg\.|рег\.|рег|регистр\.|регистрац(?:.ию)?)[:\s]*([A-Z0-9\-\/]+)\b", re.IGNORECASE)
# fallback generic reg number detection (digits with optional dash/slash)
REG_FALLBACK_RE = re.compile(r"\b[0-9]{2,6}[-/][0-9A-Z]{2,10}\b")

# header keywords mapping (russian-ish)
HEADER_KEYWORDS = {
    "date": ["дата", "дата операции"],
    "type": ["операц", "вид операции", "тип операции", "наименование операции"],
    "sum": ["сумма", "сумма платежа", "сумма / сумма платежа", "платёж", "платеж"],
    "currency": ["валюта", "валютa", "вал."],
    "comment": ["коммент", "примечан", "назначение", "информация"],
    "price": ["цена"],
    "quantity": ["количеств", "объем", "кол-во", "кол-во/объем"],
    "ticker": ["тикер", "код"],
    "isin": ["isin"],
    "aci": ["нкд", "накопленный купонный доход", "нкд/aci", "aci"],
    "operation_id": ["номер", "id", "ид"],
}

SECTION_START_MARKERS = ("финансовые операции", "финансовые операции по счету", "финансовые операции по счёту")


def to_float(v: Any) -> float:
    if v is None:
        return 0.0
    try:
        s = str(v).strip().replace(" ", "").replace("\u00A0", "")
        s = s.replace(",", ".")
        return float(s)
    except Exception:
        return 0.0


def to_int(v: Any) -> int:
    try:
        return int(float(str(v).strip().replace(",", ".").replace(" ", "")))
    except Exception:
        return 0


def find_section_start(df: pd.DataFrame) -> Optional[int]:
    """Найти индекс строки, где начинается секция 'Финансовые операции'"""
    for idx, row in df.iterrows():
        joined = " ".join([str(c) for c in row if str(c).strip()]).lower()
        if any(marker in joined for marker in SECTION_START_MARKERS):
            logger.debug("Found section start at row %s", idx)
            return idx
    return None


def find_header_row(df: pd.DataFrame, start_idx: int, lookahead: int = 12) -> Optional[int]:
    """
    После section start ищем заголовок таблицы — строку, в которой есть ключевые слова (дата, сумма, валюта...)
    Возвращаем индекс заголовка.
    """
    n = len(df)
    end = min(n, start_idx + lookahead + 1)
    for i in range(start_idx + 1, end):
        row = df.iloc[i]
        cells = [str(c).strip().lower() for c in row if str(c).strip()]
        if not cells:
            continue
        joined = " ".join(cells)
        # простая эвристика: если есть "дата" и "сумма" или "валюта" — это заголовок
        if "дата" in joined and ("сумма" in joined or "валюта" in joined or "операц" in joined):
            logger.debug("Found header row at %s: %s", i, joined)
            return i
    # fallback: scan further for any row containing 'дата' + one of other keywords
    for i in range(start_idx + 1, min(n, start_idx + 50)):
        row = df.iloc[i]
        joined = " ".join([str(c).strip().lower() for c in row if str(c).strip()])
        if "дата" in joined and ("сумма" in joined or "валюта" in joined):
            logger.debug("Found header row (fallback) at %s: %s", i, joined)
            return i
    return None


def map_header_indices(header_row) -> Dict[str, int]:
    """
    На основе строки заголовка возвращаем маппинг колонок:
    { 'date': idx, 'type': idx, 'sum': idx, ... }
    """
    cols = {}
    for idx, cell in enumerate(header_row):
        if not str(cell).strip():
            continue
        low = str(cell).strip().lower()
        for key, keywords in HEADER_KEYWORDS.items():
            if any(k in low for k in keywords):
                # prefer first mapping only if not already set
                if key not in cols:
                    cols[key] = idx
    return cols


def extract_isin_and_reg(comment: str) -> Tuple[Optional[str], Optional[str]]:
    if not comment:
        return None, None
    c = str(comment)
    m_isin = ISIN_RE.search(c)
    isin = m_isin.group(0).upper() if m_isin else None

    # Try explicit reg pattern first
    m_reg = REG_RE.search(c)
    reg = m_reg.group(1) if m_reg else None
    if not reg:
        m_fallback = REG_FALLBACK_RE.search(c)
        reg = m_fallback.group(0) if m_fallback else None

    return isin, reg


def parse_fin_operations(file_path: str) -> List[OperationDTO]:
    logger.info("Parsing financial operations from %s", file_path)
    df = pd.read_excel(file_path, header=None, dtype=object)
    df = df.fillna("")

    start_idx = find_section_start(df)
    if start_idx is None:
        logger.info("Section 'Финансовые операции' not found")
        return []

    header_idx = find_header_row(df, start_idx)
    if header_idx is None:
        logger.warning("Header row for financial operations not found, aborting")
        return []

    header_row = df.iloc[header_idx]
    cols = map_header_indices(header_row)
    logger.debug("Detected columns: %s", cols)

    ops: List[OperationDTO] = []
    # rows after header
    for i in range(header_idx + 1, len(df)):
        row = df.iloc[i]
        cells = [str(c).strip() for c in row if str(c).strip()]
        if (not cells) or any(k in " ".join(cells).lower() for k in ("итого", "всего", "баланс", "остаток")):
            # treat as end of section
            break

        # If there is a row that looks like a new section title, stop
        joined_low = " ".join(cells).lower()
        if any(marker in joined_low for marker in SECTION_START_MARKERS):
            break

        # extract fields using cols mapping with safe fallback
        def g(col_key: str) -> Any:
            idx = cols.get(col_key)
            return row[idx] if idx is not None else None

        # date
        date_val = extract_date(g("date"))
        # operation type raw
        op_raw = g("type")
        op_raw_s = str(op_raw).strip() if op_raw is not None else ""

        # payment sum and currency
        payment_raw = g("sum") or g("payment") or None
        payment_sum = to_float(payment_raw)

        currency_raw = g("currency")
        currency = str(currency_raw).strip() if currency_raw is not None else ""
        currency_normalized = constants.CURRENCY_DICT.get(currency.upper(), currency.upper() if currency else "")

        comment_raw = g("comment")
        comment = str(comment_raw).strip() if comment_raw is not None else ""

        # try to extract ticker/isin/reg from specific columns or comment
        ticker = ""
        if "ticker" in cols:
            ticker = str(g("ticker") or "").strip()

        isin_col = str(g("isin") or "").strip()
        isin = isin_col or None
        reg_number = ""

        if not isin or isin == "":
            isin_c, reg_c = extract_isin_and_reg(comment)
            if isin_c:
                isin = isin_c
            if reg_c:
                reg_number = reg_c

        # If reg column exists explicitly
        if "reg_number" in cols:
            reg_number = str(g("reg_number") or reg_number or "").strip()

        # price and quantity and aci
        price = to_float(g("price"))
        quantity = to_int(g("quantity"))
        aci = to_float(g("aci"))

        # Operation filtering / mapping
        # Normalize operation name for matching
        op_norm = op_raw_s.strip()
        # Try to find first matching valid operation name in constants.VALID_OPERATIONS or SKIP_OPERATIONS
        # Many operation names can be long; we'll try direct match or substring match
        op_type = None
        if op_norm:
            # exact match
            if op_norm in constants.SKIP_OPERATIONS:
                logger.debug("Skipping operation (skip list): %s (row %s)", op_norm, i)
                continue
            if op_norm in constants.VALID_OPERATIONS:
                op_type = constants.OPERATION_TYPE_MAP.get(op_norm) or op_norm.lower().replace(" ", "_")
            else:
                # substring match
                found = None
                for valid in constants.VALID_OPERATIONS:
                    if valid.lower() in op_norm.lower():
                        found = valid
                        break
                if found:
                    op_type = constants.OPERATION_TYPE_MAP.get(found) or found.lower().replace(" ", "_")

        # special handlers
        if not op_type and op_norm in constants.SPECIAL_OPERATION_HANDLERS:
            try:
                handler = constants.SPECIAL_OPERATION_HANDLERS[op_norm]
                op_type = handler(payment_sum, None)  # second arg placeholder (could be entire row)
            except Exception:
                op_type = None

        # If still not resolved, try map by substring keys
        if not op_type:
            for k, v in constants.OPERATION_TYPE_MAP.items():
                if k.lower() in op_norm.lower():
                    op_type = v
                    break

        # If operation name empty, skip
        if not op_type:
            # If operation name absent but payment nonzero, still keep as 'other'
            if constants.is_nonzero(payment_sum):
                op_type = "other"
            else:
                logger.debug("Skipping empty/irrelevant operation at row %s: %s", i, op_norm)
                continue

        dto = OperationDTO(
            date=date_val,
            operation_type=op_type,
            payment_sum=payment_sum,
            currency=currency_normalized,
            ticker=ticker,
            isin=(isin or ""),
            reg_number=(reg_number or ""),
            price=price,
            quantity=quantity,
            aci=aci,
            comment=comment,
            operation_id=str(g("operation_id") or "") or ""
        )

        ops.append(dto)

    logger.info("Parsed %s financial operations", len(ops))
    return ops
