import re
from datetime import datetime
from typing import Any, List, Optional, Tuple

import xlrd

from OperationDTO import OperationDTO
from constatns import CURRENCY_DICT, TRADE_TYPE_CONFIG
from utils import extract_rows, normalize_str


def normalize_currency(value: Any) -> str:
    value = str(value).strip().upper() if value else ""
    return CURRENCY_DICT.get(value, value)


def parse_date(value: Any) -> Optional[str]:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, (int, float)):
        try:
            date = datetime(*xlrd.xldate_as_tuple(value, 0))
            return date.strftime("%Y-%m-%d")
        except Exception:
            return None
    if isinstance(value, str):
        for fmt in ("%d.%m.%Y", "%d.%m.%y"):
            try:
                return datetime.strptime(value.strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
    return None


def parse_time(value: Any) -> str:
    if isinstance(value, datetime):
        return value.strftime("%H:%M:%S")
    if isinstance(value, (int, float)):
        try:
            return datetime(*xlrd.xldate_as_tuple(value, 0)).strftime("%H:%M:%S")
        except Exception:
            return "00:00:00"
    if isinstance(value, str):
        for fmt in ("%H:%M:%S", "%H:%M"):
            try:
                return datetime.strptime(value.strip(), fmt).strftime("%H:%M:%S")
            except ValueError:
                continue
    return "00:00:00"


def is_ticker_row(row: List[Any]) -> bool:
    return (
        any(isinstance(cell, str) and "isin" in cell.lower() for cell in row) or
        (isinstance(row[0], str) and re.match(r"^[A-Z]{3,}(_)?[A-Z]{3,}", row[0]) and all(not cell for cell in row[1:]))
    )


def extract_ticker(row: List[Any]) -> Optional[str]:
    if row and isinstance(row[0], str):
        ticker = row[0].strip()
        if re.match(r"^[A-Z]{3,}(_)?[A-Z]{3,}", ticker):
            return ticker
        return ticker.split()[0]
    return ""


def extract_isin(row: List[Any]) -> Optional[str]:
    for i, cell in enumerate(row):
        if isinstance(cell, str) and "isin" in cell.lower():
            match = re.search(r"ISIN[:\s]*([A-Z0-9]{12})", cell)
            if match:
                return match.group(1)
            next_cell = row[i + 1] if i + 1 < len(row) else ""
            if isinstance(next_cell, str) and re.match(r"^[A-Z0-9]{12}$", next_cell.strip()):
                return next_cell.strip()
    return ""


def is_section_start(row: List[Any], keywords: List[str]) -> bool:
    return any(keyword in str(cell).lower() for cell in row if cell for keyword in keywords)


def is_valid_trade_row(row: List[Any]) -> bool:
    if not row or not row[0]:
        return False
    if isinstance(row[0], str) and row[0].lower().startswith("итого"):
        return False
    return any(isinstance(cell, (int, float)) for cell in row)


def safe_get(row: List[Any], idx: Optional[int], default=None):
    if idx is None or not isinstance(idx, int):
        return default
    return row[idx] if idx < len(row) else default



def to_float(value: Any) -> float:
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0


def extract_currency_pair_from_row(row: List[Any]) -> Tuple[Optional[str], Optional[str]]:
    lot_currency = ""
    pair_currency = ""

    for i, cell in enumerate(row):
        if isinstance(cell, str):
            cell_low = cell.lower()
            if "валюта лота" in cell_low:
                lot_currency = next_non_empty_str(row, i)
            elif "сопряж" in cell_low:
                pair_currency = next_non_empty_str(row, i)

    if lot_currency and pair_currency:
        ticker = f"{lot_currency}{pair_currency}"
        return ticker, pair_currency
    return "", ""


def next_non_empty_str(row: List[Any], start_idx: int) -> str:
    for j in range(start_idx + 1, len(row)):
        nxt = row[j]
        if isinstance(nxt, str) and nxt.strip():
            return normalize_currency(nxt)
    return ""


def get_operation_type(config: dict, is_buy: bool) -> str:
    if config["is_currency"]:
        return "currency_buy" if is_buy else "currency_sale"
    return "buy" if is_buy else "sale"


def parse_trade_row(
    row: List[Any],
    trade_type: str,
    ticker: str,
    currency_hint: Optional[str],
    isin: Optional[str] = ""
) -> OperationDTO:
    is_buy = bool(row[3])
    config = TRADE_TYPE_CONFIG[trade_type]
    operation = "buy" if is_buy else "sale"
    indexes = config["indexes"][operation]

    price = to_float(safe_get(row, indexes["price"]))
    quantity = to_float(safe_get(row, indexes["quantity"]))
    payment = to_float(safe_get(row, indexes["payment"]))

    trade_date = parse_date(safe_get(row, indexes.get("date", "")))
    trade_time = parse_time(safe_get(row, indexes.get("time", "")))
    currency = normalize_currency(safe_get(row, indexes.get("currency"))) if indexes.get("currency") else currency_hint
    comment = normalize_str(safe_get(row, indexes.get("comment", ""), "")) if indexes.get("comment") else ""
    aci = to_float(safe_get(row, indexes.get("aci", ""), 0.0)) if trade_type == "bond" else 0.0
    operation_id = str(safe_get(row, 1, "")).strip()

    full_date = f"{trade_date} {trade_time}" if trade_date else ""

    return OperationDTO(
        date=full_date,
        operation_type=get_operation_type(config, is_buy),
        payment_sum=payment,
        currency=currency,
        ticker=normalize_str(ticker),
        isin="" if config["is_currency"] else isin,
        price=price,
        quantity=int(quantity),
        aci=aci,
        comment=comment,
        operation_id=operation_id,
    )


SECTION_KEYWORDS = {
    "stock": ["акция", "адр"],
    "bond": ["облигация"],
    "currency": ["иностранная валюта"]
}


def parse_trades(filepath: str) -> List[OperationDTO]:
    rows = list(extract_rows(filepath))

    result = []
    current_ticker = current_isin = current_currency = None
    current_section = None
    parsing_trades = False

    for row in rows:
        row = row[1:]  # Пропускаем первую колонку
        joined_row = " ".join(map(str, row)).lower()

        if not parsing_trades:
            if "2.1. сделки:" in joined_row:
                parsing_trades = True
            continue

        if is_ticker_row(row):
            current_ticker = extract_ticker(row)
            current_isin = extract_isin(row)
            continue

        for section, keywords in SECTION_KEYWORDS.items():
            if is_section_start(row, keywords):
                current_section = section
                break

        if is_section_start(row, ["заем", "овернайт", "цб"]):
            break

        if current_section == 'currency' and any("сопряж" in str(cell).lower() for cell in row):
            current_ticker, current_currency = extract_currency_pair_from_row(row)
            continue

        if is_valid_trade_row(row):
            dto = parse_trade_row(
                row=row,
                trade_type=current_section,
                ticker=current_ticker,
                currency_hint=current_currency,
                isin=current_isin
            )
            result.append(dto)

    return result
