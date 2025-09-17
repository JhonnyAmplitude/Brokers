# src/parsers/fin_operations.py
from __future__ import annotations
from typing import Any, List, Optional, Dict, Tuple
import re
import pandas as pd

from src.utils import logger, extract_date
from src.OperationDTO import OperationDTO
import src.constants


# ----------------- вспомогательные функции -----------------
def to_float(v: Any) -> float:
    if v is None:
        return 0.0
    try:
        s = str(v).strip().replace(" ", "").replace("\u00A0", "")
        s = s.replace(",", ".")
        return float(s) if s not in ("", "-", "--") else 0.0
    except Exception:
        try:
            return float(str(v).replace(",", "."))
        except Exception:
            return 0.0


def to_int(v: Any) -> int:
    try:
        return int(round(float(str(v).strip().replace(",", ".").replace(" ", ""))))
    except Exception:
        return 0


ISIN_RE = re.compile(r"\b[A-Z]{2}[A-Z0-9]{9}\d\b", re.IGNORECASE)

SECTION_RE_1 = re.compile(r"финанс\w*\s+операц", re.IGNORECASE)
SECTION_RE_2 = re.compile(r"операц\w*.*сч(?:ет|ёт)|операции.*сч(?:ет|ёт)|операции\s+по\s+счет", re.IGNORECASE)

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

SECTION_END_KEYWORDS = ("итого", "всего", "баланс", "остаток")


def debug_print_matching_rows(file_path: str, keywords: List[str], max_rows: int = 200):
    """
    Утилита: напечатает строки, где встречаются keywords — удобно для отладки.
    """
    try:
        df = pd.read_excel(file_path, header=None, dtype=object).fillna("")
    except Exception:
        return
    for idx, row in df.iterrows():
        joined = " ".join([str(c).strip().lower() for c in row if str(c).strip()])
        if any(k in joined for k in keywords):
            logger.info("Row %s: %s", idx, joined)
        if idx >= max_rows:
            break


def find_section_start(df: pd.DataFrame) -> Optional[int]:
    """
    Ищем начало секции 'Финансовые операции' с гибкими эвристиками.
    Возвращаем индекс строки, где начинается секция (строка с заголовком),
    или None если не найдено.
    """
    for idx, row in df.iterrows():
        joined = " ".join([str(c) for c in row if str(c).strip()]).lower()
        if not joined:
            continue
        if SECTION_RE_1.search(joined) or SECTION_RE_2.search(joined):
            logger.debug("Found financial section start at row %s -> %s", idx, joined)
            return idx
    return None


def find_header_row(df: pd.DataFrame, start_idx: int, lookahead: int = 40) -> Optional[int]:
    """
    После section start ищем строку заголовка таблицы (дата + сумма/валюта/операция).
    """
    n = len(df)
    end = min(n, start_idx + lookahead + 1)
    for i in range(start_idx + 1, end):
        row = df.iloc[i]
        cells = [str(c).strip().lower() for c in row if str(c).strip()]
        if not cells:
            continue
        joined = " ".join(cells)
        if "дата" in joined and ("сумма" in joined or "валюта" in joined or "операц" in joined):
            logger.debug("Found financial header row at %s: %s", i, joined)
            return i

    for i in range(start_idx + 1, min(n, start_idx + 200)):
        row = df.iloc[i]
        joined = " ".join([str(c).strip().lower() for c in row if str(c).strip()])
        if "дата" in joined and ("сумма" in joined or "валюта" in joined):
            logger.debug("Found financial header row (fallback) at %s: %s", i, joined)
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

    REG_LONG_RE = re.compile(r"\b[0-9A-ZА-Я]{1,6}[-/][0-9A-ZА-Я\-\/]{3,}[0-9A-ZА-Я]?\b", re.IGNORECASE)
    REG_SHORT_RE = re.compile(r"\b[КKkК]\d{3,8}\b", re.IGNORECASE)

    m_reg_long = REG_LONG_RE.search(c)
    reg = m_reg_long.group(0) if m_reg_long else None

    if not reg:
        m_reg_short = REG_SHORT_RE.search(c)
        reg = m_reg_short.group(0) if m_reg_short else None

    if reg:
        reg = reg.strip().strip(".,;")

    return isin, reg


# ----------------- основной парсер финансовых операций -----------------
def parse_fin_operations(file_path: str) -> tuple[List[OperationDTO], dict]:
    logger.info("Parsing financial operations from %s", file_path)
    try:
        df = pd.read_excel(file_path, header=None, dtype=object)
    except Exception as e:
        logger.error("Failed to read excel %s: %s", file_path, e)
        return [], {"error": str(e)}

    df = df.fillna("")

    stats = {
        "total_rows": 0,
        "parsed": 0,
        "skipped_section_not_found": 0,
        "skipped_header_not_found": 0,
        "skipped_skiplist": 0,
        "skipped_zero_unknown": 0,
        "skipped_coupon_nonpositive": 0,
        "unrecognized_names": [],
    }

    # normalized helper & maps
    _norm = getattr(src.constants, "norm_str", lambda x: str(x).strip().lower() if x else "")
    normalized_skip = { _norm(x) for x in getattr(src.constants, "SKIP_OPERATIONS", set()) }
    normalized_valid = { _norm(x) for x in getattr(src.constants, "VALID_OPERATIONS", set()) }
    # normalized map of operation type map keys -> value
    normalized_op_map = { _norm(k): v for k, v in getattr(src.constants, "OPERATION_TYPE_MAP", {}).items() }
    # op_map_keys list (normalized) for substring checks
    op_map_keys = list(normalized_op_map.keys())
    # special handlers normalized mapping (norm_key -> original_key)
    special_handlers = getattr(src.constants, "SPECIAL_OPERATION_HANDLERS", {})
    normalized_special_map = { _norm(k): k for k in special_handlers.keys() }

    start_idx = find_section_start(df)
    if start_idx is None:
        logger.info("Section 'Финансовые операции' not found (tried flexible heuristics).")
        debug_print_matching_rows(file_path, ["финанс", "операц", "операции", "субсчет", "субсчета"], max_rows=200)
        stats["skipped_section_not_found"] = 1
        return [], stats

    header_idx = find_header_row(df, start_idx)
    if header_idx is None:
        logger.warning("Header row for financial operations not found, aborting")
        stats["skipped_header_not_found"] = 1
        return [], stats

    header_row = df.iloc[header_idx]
    cols = map_header_indices(header_row)
    logger.debug("Detected columns: %s", cols)

    ops: List[OperationDTO] = []
    for i in range(header_idx + 1, len(df)):
        stats["total_rows"] += 1
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

        # ------------------ op_type resolution ------------------
        op_norm_raw = _norm(op_raw)  # normalized operation raw text
        op_low = op_norm_raw

        # Quick checks: skip / known via valid/map/special
        is_skip = False
        if op_low:
            is_skip = any(sk in op_low for sk in normalized_skip) or (op_low in normalized_skip)

        is_known = False
        if op_low:
            is_known = (op_low in normalized_valid) or any(k in op_low for k in op_map_keys) or any(s in op_low for s in normalized_special_map)

        # record unrecognized raw name if not skip and not known
        if op_raw_s and (not is_skip) and (not is_known):
            stats["unrecognized_names"].append(op_raw_s)
        elif (not op_raw_s) and comment and (not is_skip) and (not is_known):
            stats["unrecognized_names"].append(comment)

        # 1) skip explicit skip list (substring match)
        if op_low and any(skip_key in op_low for skip_key in normalized_skip):
            logger.debug("Skipping operation (skip list substring): %s (row %s)", op_raw_s, i)
            stats["skipped_skiplist"] += 1
            continue

        # 1.5) Try special handlers (if any special key is substring of op_low)
        op_type: Optional[str] = None
        matched_special_key = None
        for norm_k, orig_k in normalized_special_map.items():
            if norm_k in op_low:
                matched_special_key = orig_k
                try:
                    handler = special_handlers.get(orig_k)
                    if callable(handler):
                        # some handlers expect (sum, entry) as per earlier design
                        entry = {
                            "date": date_val,
                            "raw_type": op_raw_s,
                            "sum": payment_sum,
                            "currency": currency_normalized,
                            "comment": comment,
                            "ticker": ticker,
                            "isin": isin or "",
                            "reg_number": reg_number or "",
                        }
                        op_type = handler(payment_sum, entry)
                except Exception:
                    op_type = None
                break

        # 2) direct mapping if operation known exactly (normalized)
        if not op_type and op_low and op_low in normalized_valid:
            # get from OPERATION_TYPE_MAP if exact key present (normalize map keys)
            op_type = normalized_op_map.get(op_low, op_low.replace(" ", "_"))

        # 3) substring matches against OPERATION_TYPE_MAP normalized keys
        if not op_type:
            for k_norm, v in normalized_op_map.items():
                if k_norm in op_low:
                    op_type = v
                    break

        # 4) final fallback: decide by sign
        if not op_type:
            sign = src.constants.get_sign(payment_sum)
            if sign < 0:
                op_type = "withdrawal"
            elif sign > 0:
                op_type = "deposit"
            else:
                stats["skipped_zero_unknown"] += 1
                # already collected op_raw_s/comment in unrecognized_names above
                continue

        # 5) if transfer — choose direction by sign
        if op_type == "transfer":
            sign = src.constants.get_sign(payment_sum)
            if sign < 0:
                op_type = "withdrawal"
            elif sign > 0:
                op_type = "deposit"

        # 6) coupons: keep only positive receipts
        if op_type == "coupon" and (payment_sum is None or float(payment_sum) <= 0.0):
            logger.debug("Skipping coupon with non-positive sum: %s (row %s)", payment_sum, i)
            stats["skipped_coupon_nonpositive"] += 1
            continue

        # ------------------------------------------------------------------------------------
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
        stats["parsed"] += 1

    logger.info("Parsed %s financial operations", len(ops))

    # deduplicate unrecognized names preserving order
    stats["unrecognized_names"] = list(dict.fromkeys([n for n in stats["unrecognized_names"] if n]))
    return ops, stats
