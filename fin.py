import re
from datetime import datetime
from typing import Any, List, Optional

import xlrd

from OperationDTO import OperationDTO
from constatns import CURRENCY_DICT
from utils import extract_rows


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
            return datetime(*xlrd.xldate_as_tuple(value, 0)).time().strftime("%H:%M:%S")
        except Exception:
            return "00:00:00"
    if isinstance(value, str):
        for fmt in ("%H:%M:%S", "%H:%M"):
            try:
                return datetime.strptime(value.strip(), fmt).time().strftime("%H:%M:%S")
            except ValueError:
                continue
    return "00:00:00"

def detect_new_ticker(row: List[Any]) -> bool:
    return any(isinstance(cell, str) and "isin" in cell.lower() for cell in row)

def extract_ticker(row: List[Any]) -> Optional[str]:
    return str(row[0]).split()[0].strip() if row and isinstance(row[0], str) else None

def extract_isin(row: List[Any]) -> Optional[str]:
    for i, cell in enumerate(row):
        if isinstance(cell, str) and "isin" in cell.lower():
            match = re.search(r"ISIN:\s*([A-Z0-9]+)", cell)
            if match:
                return match.group(1)
            next_cell = row[i + 1] if i + 1 < len(row) else ""
            if isinstance(next_cell, str) and re.match(r"^[A-Z0-9]{12}$", next_cell.strip()):
                return next_cell.strip()
    return None

def is_section_start(row: List[Any], keywords: List[str]) -> bool:
    return any(keyword in str(cell).lower() for cell in row if cell for keyword in keywords)

def is_valid_trade_row(row: List[Any]) -> bool:
    if not row or not row[0]:
        return False
    if isinstance(row[0], str) and row[0].lower().startswith("итого"):
        return False
    return any(isinstance(cell, (int, float)) for cell in row)

def safe_get(row: List[Any], idx: int, default=None) -> Any:
    return row[idx] if idx < len(row) else default

def extract_currency_from_row(row: List[Any]) -> Optional[str]:
    for i, cell in enumerate(row):
        if isinstance(cell, str) and "сопряж" in cell.lower():
            for j in range(i + 1, len(row)):
                candidate = row[j]
                if isinstance(candidate, str) and candidate.strip():
                    return normalize_currency(candidate)
    return None

def parse_trade(row: List[Any], trade_type: str, ticker: str, isin: Optional[str]) -> OperationDTO:
    is_buy = bool(row[3])
    stock_mode = trade_type == "stock"
    price = row[4] if is_buy else row[7 if stock_mode else 8]
    quantity = row[3] if is_buy else row[6 if stock_mode else 7]
    payment = row[5] if is_buy else row[8 if stock_mode else 9]
    currency = normalize_currency(row[10 if stock_mode else 11])
    comment = str(safe_get(row, 17 if stock_mode else 18, "")).strip()
    operation_id = str(row[1]).strip() if row[1] else None
    aci = safe_get(row, 6) or safe_get(row, 10) if not stock_mode else None
    trade_date = parse_date(row[11 if stock_mode else 13])
    trade_time = parse_time(row[12]) if stock_mode else "00:00:00"
    full_date = f"{trade_date} {trade_time}" if trade_date else None

    return OperationDTO(
        date=full_date or "",
        operation_type="buy" if is_buy else "sell",
        payment_sum=payment,
        currency=currency,
        ticker=ticker,
        isin=isin,
        price=price,
        quantity=int(quantity),
        aci=aci,
        comment=comment,
        operation_id=operation_id,
    )

def parse_currency_trade(row: List[Any], ticker: str, currency_hint: Optional[str]) -> OperationDTO:
    is_buy = bool(row[3])
    price = row[3] if is_buy else row[6]
    quantity = row[4] if is_buy else row[7]
    payment = row[5] if is_buy else row[8]
    trade_date = parse_date(row[9])
    trade_time = parse_time(row[10])
    operation_id = str(row[1]).strip() if row[1] else None
    full_date = f"{trade_date} {trade_time}" if trade_date else ""

    return OperationDTO(
        date=full_date,
        operation_type="buy" if is_buy else "sell",
        payment_sum=payment,
        currency=currency_hint,
        ticker=ticker,
        isin=None,
        price=price,
        quantity=int(quantity),
        aci=None,
        comment="",
        operation_id=operation_id,
    )

def parse_trades(filepath: str) -> List[OperationDTO]:
    rows = list(extract_rows(filepath))

    result = []
    current_ticker = current_isin = current_currency = None
    current_section = None  # 'stock', 'bond', 'currency'
    parsing_trades = False

    for row in rows:
        row = row[1:]  # Пропускаем первую колонку
        joined_row = " ".join(map(str, row)).lower()

        if not parsing_trades:
            if "2.1. сделки:" in joined_row:
                parsing_trades = True
            continue

        if detect_new_ticker(row):
            current_ticker = extract_ticker(row)
            current_isin = extract_isin(row)
            continue

        if is_section_start(row, ["акция", "адр"]):
            current_section = 'stock'; continue
        if is_section_start(row, ["облигация"]):
            current_section = 'bond'; continue
        if is_section_start(row, ["иностранная валюта"]):
            current_section = 'currency'; continue
        if is_section_start(row, ["заем", "овернайт", "цб"]):
            break

        if current_section == 'currency' and any("сопряж" in str(cell).lower() for cell in row):
            current_currency = extract_currency_from_row(row)
            continue

        if is_valid_trade_row(row):
            if current_section == 'currency':
                dto = parse_currency_trade(row, current_ticker, current_currency)
            else:
                trade_type = "stock" if current_section == 'stock' else "bond"
                dto = parse_trade(row, trade_type, current_ticker, current_isin)
            result.append(dto)

    return result

