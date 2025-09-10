from __future__ import annotations
import re
from typing import List, Optional, Dict, Any, Tuple

import pandas as pd

from src.utils import logger, extract_date
from src.OperationDTO import OperationDTO
import src.constants

# Patterns (raw strings to avoid escape warnings)
ISIN_RE = re.compile(r"\b[A-Z]{2}[A-Z0-9]{9}\d\b", re.IGNORECASE)

REG_LONG_RE = re.compile(r"\b[0-9A-ZА-Я]{1,6}[-/][0-9A-ZА-Я\-\/]{3,}[0-9A-ZА-Я]?\b", re.IGNORECASE)
REG_SHORT_RE = re.compile(r"\b[КKkК]\d{3,8}\b", re.IGNORECASE)

# more flexible section detection
SECTION_RE_1 = re.compile(r"финанс\w*\s+операц", re.IGNORECASE)
SECTION_RE_2 = re.compile(r"операц\w*.*сч(?:ет|ёт)|операции.*сч(?:ет|ёт)|операции\s+по\s+счет", re.IGNORECASE)

# header keywords mapping
HEADER_KEYWORDS = {
    "date": ["дата", "дата операции"],
    "type": ["операц", "вид операции", "тип операции", "наименование операции"],
    "sum": ["сумма", "сумма платежа", "платёж", "платеж"],
    "currency": ["валюта", "валютa", "вал."],
    "comment": ["коммент", "примечан", "назначение", "информация"],
    "price": ["цена"],
    "quantity": ["количеств", "объем", "кол-во", "кол-во/объем"],
    "ticker": ["тикер", "код"],
    "isin": ["isin"],
    "aci": ["нкд", "накопленный купонный доход", "нкд/aci", "aci"],
    "operation_id": ["номер", "id", "ид"],
}

# words that likely mark the end-of-section / totals
SECTION_END_KEYWORDS = ("итого", "всего", "баланс", "остаток")


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
    """
    Ищем начало секции с более гибкими эвристиками:
      - строка содержит 'финанс*' и 'операц*'
      - или содержит 'операции' и 'счёт/счет/по счет'
    """
    for idx, row in df.iterrows():
        joined = " ".join([str(c) for c in row if str(c).strip()]).lower()
        if not joined:
            continue
        if SECTION_RE_1.search(joined) or SECTION_RE_2.search(joined):
            logger.debug("Found section start at row %s -> %s", idx, joined)
            return idx
    return None


def find_header_row(df: pd.DataFrame, start_idx: int, lookahead: int = 40) -> Optional[int]:
    """
    После section start ищем строку заголовка таблицы (дата + сумма/валюта/операция).
    Увеличил lookahead — иногда заголовок идет через несколько строк.
    """
    n = len(df)
    end = min(n, start_idx + lookahead + 1)
    for i in range(start_idx + 1, end):
        row = df.iloc[i]
        # join lower-case cells
        cells = [str(c).strip().lower() for c in row if str(c).strip()]
        if not cells:
            continue
        joined = " ".join(cells)
        # If row contains 'дата' and one of ('сумма', 'валюта', 'операц')
        if "дата" in joined and ("сумма" in joined or "валюта" in joined or "операц" in joined):
            logger.debug("Found header row at %s: %s", i, joined)
            return i

    # fallback: scan a bit further for looser matches
    for i in range(start_idx + 1, min(n, start_idx + 200)):
        row = df.iloc[i]
        joined = " ".join([str(c).strip().lower() for c in row if str(c).strip()])
        if "дата" in joined and ("сумма" in joined or "валюта" in joined):
            logger.debug("Found header row (fallback) at %s: %s", i, joined)
            return i
    return None


def map_header_indices(header_row) -> Dict[str, int]:
    cols = {}
    for idx, cell in enumerate(header_row):
        if not str(cell).strip():
            continue
        low = str(cell).strip().lower()
        for key, keywords in HEADER_KEYWORDS.items():
            if any(k in low for k in keywords):
                if key not in cols:
                    cols[key] = idx
    return cols


def extract_isin_and_reg(comment: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Возвращает (isin, reg_number). Ищем ISIN, затем длинный reg, затем короткий (К123456).
    """
    if not comment:
        return None, None
    c = str(comment)

    m_isin = ISIN_RE.search(c)
    isin = m_isin.group(0).upper() if m_isin else None

    m_reg_long = REG_LONG_RE.search(c)
    reg = m_reg_long.group(0) if m_reg_long else None

    if not reg:
        m_reg_short = REG_SHORT_RE.search(c)
        reg = m_reg_short.group(0) if m_reg_short else None

    if reg:
        reg = reg.strip().strip(".,;")

    return isin, reg


def debug_print_matching_rows(file_path: str, keywords: List[str], max_rows: int = 200):
    """
    Утилита: напечатает строки, где встречаются keywords — удобно для отладки.
    Запускать вручную при необходимости.
    """
    df = pd.read_excel(file_path, header=None, dtype=object).fillna("")
    for idx, row in df.iterrows():
        joined = " ".join([str(c).strip().lower() for c in row if str(c).strip()])
        if any(k in joined for k in keywords):
            logger.info("Row %s: %s", idx, joined)
        if idx >= max_rows:
            break


def parse_fin_operations(file_path: str) -> List[OperationDTO]:
    logger.info("Parsing financial operations from %s", file_path)
    df = pd.read_excel(file_path, header=None, dtype=object)
    df = df.fillna("")

    start_idx = find_section_start(df)
    if start_idx is None:
        logger.info("Section 'Финансовые операции' not found (tried flexible heuristics).")
        # For debugging: print lines that look similar to 'финансов'/'операц'
        logger.debug("Dumping rows that contain 'финанс' or 'операц' for inspection...")
        debug_print_matching_rows(file_path, ["финанс", "операц", "операции", "субсчет", "субсчета"], max_rows=200)
        return []

    header_idx = find_header_row(df, start_idx)
    if header_idx is None:
        logger.warning("Header row for financial operations not found, aborting")
        return []

    header_row = df.iloc[header_idx]
    cols = map_header_indices(header_row)
    logger.debug("Detected columns: %s", cols)

    ops: List[OperationDTO] = []
    for i in range(header_idx + 1, len(df)):
        row = df.iloc[i]
        cells = [str(c).strip() for c in row if str(c).strip()]
        if (not cells) or any(k in " ".join(cells).lower() for k in SECTION_END_KEYWORDS):
            break

        joined_low = " ".join(cells).lower()

        if "внебиржев" in joined_low or "внебиржевой рынок" in joined_low:
            logger.info("Encountered 'Внебиржевой рынок' at row %s — stopping financial operations parsing.", i)
            break

        def g(col_key: str) -> Any:
            idx = cols.get(col_key)
            return row[idx] if idx is not None else None

        date_val = extract_date(g("date"))
        op_raw = g("type")
        op_raw_s = str(op_raw).strip() if op_raw is not None else ""

        payment_raw = g("sum") or g("payment") or None
        payment_sum = to_float(payment_raw)

        currency_raw = g("currency")
        currency = str(currency_raw).strip() if currency_raw is not None else ""
        currency_normalized = src.constants.CURRENCY_DICT.get(currency.upper(), currency.upper() if currency else "")

        comment_raw = g("comment")
        comment = str(comment_raw).strip() if comment_raw is not None else ""

        ticker = ""
        if "ticker" in cols:
            ticker = str(g("ticker") or "").strip()

        isin_col = str(g("isin") or "").strip()
        isin = isin_col or None
        reg_number = ""

        if not isin:
            isin_c, reg_c = extract_isin_and_reg(comment)
            if isin_c:
                isin = isin_c
            if reg_c:
                reg_number = reg_c

        if "reg_number" in cols:
            reg_number = str(g("reg_number") or reg_number or "").strip()

        price = to_float(g("price"))
        quantity = to_int(g("quantity"))
        aci = to_float(g("aci"))

        op_norm = op_raw_s.strip()
        op_type = None
        if op_norm:
            if op_norm in src.constants.SKIP_OPERATIONS:
                logger.debug("Skipping operation (skip list): %s (row %s)", op_norm, i)
                continue
            if op_norm in src.constants.VALID_OPERATIONS:
                op_type = src.constants.OPERATION_TYPE_MAP.get(op_norm) or op_norm.lower().replace(" ", "_")
            else:
                found = None
                for valid in src.constants.VALID_OPERATIONS:
                    if valid.lower() in op_norm.lower():
                        found = valid
                        break
                if found:
                    op_type = src.constants.OPERATION_TYPE_MAP.get(found) or found.lower().replace(" ", "_")

        if not op_type and op_norm in src.constants.SPECIAL_OPERATION_HANDLERS:
            try:
                handler = src.constants.SPECIAL_OPERATION_HANDLERS[op_norm]
                op_type = handler(payment_sum, None)
            except Exception:
                op_type = None

        if not op_type:
            for k, v in src.constants.OPERATION_TYPE_MAP.items():
                if k.lower() in op_norm.lower():
                    op_type = v
                    break

        if not op_type:
            if src.constants.get_sign(payment_sum):
                op_type = "other"
            else:
                logger.debug("Skipping empty/irrelevant operation at row %s: %s", i, op_norm)
                continue

        if op_type == "coupon" and (payment_sum is None or float(payment_sum) <= 0.0):
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
