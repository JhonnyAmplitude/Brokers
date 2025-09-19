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

SECTION_RE_1 = re.compile(r"движен\w* денежн\w* средств", re.IGNORECASE)


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
    """Для отладки: печатает строки, где встречаются keywords."""
    try:
        df = pd.read_excel(file_path, header=None, dtype=object).fillna("")
    except Exception:
        return
    for idx, row in df.iterrows():
        joined = " ".join([str(c).strip().lower() for c in row if str(c).strip()])
        if any(k in joined for k in keywords):
            logger.info("Строка %s: %s", idx, joined)
        if idx >= max_rows:
            break


def find_section_start(df: pd.DataFrame) -> Optional[int]:
    """Ищем начало секции финансовых операций"""
    for idx, row in df.iterrows():
        joined = " ".join([str(c) for c in row if str(c).strip()]).lower()
        if not joined:
            continue
        if (SECTION_RE_1.search(joined)):
            logger.debug("Найдена строка начала секции: %s -> %s", idx, joined)
            return idx
    return None


def find_header_row(df: pd.DataFrame, start_idx: int, lookahead: int = 40) -> Optional[int]:
    """Ищем строку с заголовками таблицы"""
    for i in range(start_idx + 1, start_idx + lookahead + 1):
        row = df.iloc[i]
        cells = [str(c).strip().lower() for c in row if str(c).strip()]
        joined = " ".join(cells)

        has_date = any("дата" in cell for cell in cells)
        has_sum = any("сумма" in cell for cell in cells)
        has_currency = any("валюта" in cell for cell in cells)
        has_operation = any("операц" in cell for cell in cells)
        has_type = any("тип" in cell for cell in cells)

        if has_date and (has_sum or has_currency or has_operation or has_type):
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
    """Возвращает (isin, reg_number), если найдены в комментарии."""
    if not comment:
        return None, None
    c = str(comment)

    m_isin = ISIN_RE.search(c)
    isin = m_isin.group(0).upper() if m_isin else None

    REG_LONG_RE = re.compile(r"\b[0-9A-ZА-Я]{1,6}[-/][0-9A-ZА-Я\-\/]{3,}[0-9A-ZА-Я]?\b", re.IGNORECASE)
    REG_SHORT_RE = re.compile(r"\b[КK]\d{3,8}\b", re.IGNORECASE)

    m_reg_long = REG_LONG_RE.search(c)
    reg = m_reg_long.group(0) if m_reg_long else None
    if not reg:
        m_reg_short = REG_SHORT_RE.search(c)
        reg = m_reg_short.group(0) if m_reg_short else None
    if reg:
        reg = reg.strip().strip(".,;")
    return isin, reg


# ----------------- основной парсер -----------------
def parse_fin_operations(file_path: str) -> tuple[List[OperationDTO], dict]:
    logger.info("Парсим финансовые операции из %s", file_path)
    try:
        df = pd.read_excel(file_path, header=None, dtype=object)
    except Exception as e:
        logger.error("Не удалось прочитать Excel %s: %s", file_path, e)
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

    # helpers
    _norm = getattr(src.constants, "norm_str", lambda x: str(x).strip().lower() if x else "")
    normalized_skip = { _norm(x) for x in getattr(src.constants, "SKIP_OPERATIONS", set()) }
    normalized_valid = { _norm(x) for x in getattr(src.constants, "VALID_OPERATIONS", set()) }
    normalized_op_map = { _norm(k): v for k, v in getattr(src.constants, "OPERATION_TYPE_MAP", {}).items() }
    special_handlers = getattr(src.constants, "SPECIAL_OPERATION_HANDLERS", {})
    normalized_special_map = { _norm(k): k for k in special_handlers.keys() }

    start_idx = find_section_start(df)
    if start_idx is None:
        logger.info("Секция финансовых операций не найдена.")
        stats["skipped_section_not_found"] = 1
        return [], stats

    header_idx = find_header_row(df, start_idx)
    if header_idx is None:
        logger.warning("Строка заголовка не найдена")
        stats["skipped_header_not_found"] = 1
        return [], stats

    header_row = df.iloc[header_idx]
    cols = map_header_indices(header_row)
    logger.debug("Обнаружены колонки: %s", cols)

    ops: List[OperationDTO] = []
    for i in range(header_idx + 1, len(df)):
        stats["total_rows"] += 1
        row = df.iloc[i]
        cells = [str(c).strip() for c in row if str(c).strip()]
        joined_low = " ".join(cells).lower()

        if not cells or any(k in joined_low for k in SECTION_END_KEYWORDS):
            break

        def g(col_key: str) -> Any:
            idx = cols.get(col_key)
            return row[idx] if idx is not None else None

        date_val = extract_date(g("date"))
        if not date_val:
            continue

        op_raw = g("type")
        op_raw_s = str(op_raw).strip() if op_raw else ""
        if not op_raw_s:
            continue

        payment_sum = to_float(g("sum"))
        currency_raw = g("currency")
        currency = str(currency_raw).strip() if currency_raw else ""
        currency_normalized = src.constants.CURRENCY_DICT.get(currency.upper(), currency.upper() if currency else "")

        comment = str(g("comment") or "").strip()
        ticker = str(g("ticker") or "").strip() if "ticker" in cols else ""
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

        op_low = _norm(op_raw_s)

        # skiplist
        if op_low in normalized_skip or any(sk in op_low for sk in normalized_skip):
            stats["skipped_skiplist"] += 1
            continue

        # определение типа операции
        op_type: Optional[str] = None

        # специальные обработчики
        for norm_k, orig_k in normalized_special_map.items():
            if norm_k in op_low:
                handler = special_handlers.get(orig_k)
                if callable(handler):
                    entry = {"date": date_val, "raw_type": op_raw_s, "sum": payment_sum, "comment": comment}
                    try:
                        op_type = handler(payment_sum, entry)
                    except Exception:
                        op_type = None
                break

        # точное совпадение
        if not op_type and op_low in normalized_op_map:
            op_type = normalized_op_map[op_low]

        # подстрока
        if not op_type:
            for k_norm, v in normalized_op_map.items():
                if k_norm in op_low:
                    op_type = v
                    break

        # fallback по знаку
        if not op_type:
            sign = src.constants.get_sign(payment_sum)
            if sign < 0:
                op_type = "withdrawal"
            elif sign > 0:
                op_type = "deposit"
            else:
                stats["skipped_zero_unknown"] += 1
                continue

        if op_type == "coupon" and payment_sum <= 0.0:
            stats["skipped_coupon_nonpositive"] += 1
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
        stats["parsed"] += 1

    logger.info("Разобрано %s финансовых операций", len(ops))
    stats["unrecognized_names"] = list(dict.fromkeys(stats["unrecognized_names"]))
    return ops, stats
