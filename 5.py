# --- Импорт и глобальные переменные ---
from typing import List, Optional, Any, Dict
from dataclasses import dataclass
from datetime import datetime
import openpyxl
import xlrd
import os
import re
import json


CURRENCY_DICT = {
    "SEK": "SEK", "NOK": "NOK", "AED": "AED", "XAG": "XAG", "ZAR": "ZAR",
    "TRY": "TRY", "XAU": "XAU", "HKD": "HKD", "TJS": "TJS", "UZS": "UZS",
    "KGS": "KGS", "KZT": "KZT", "JPY": "JPY", "AMD": "AMD", "РУБЛЬ": "RUB",
    "RUR": "RUB", "RUB": "RUB", "USD": "USD", "EUR": "EUR", "BYN": "BYN",
    "GBP": "GBP", "CHF": "CHF", "CNY": "CNY"
}

@dataclass
class OperationDTO:
    date: str
    operation_type: str
    payment_sum: float
    currency: str
    ticker: str
    isin: Optional[str]
    price: float
    quantity: int
    aci: Optional[float]
    comment: str
    operation_id: Optional[str]
    _sort_key: Optional[str] = None


# --- Вспомогательные функции ---
def read_excel_file(filepath: str, file_ext: str) -> List[List[Any]]:
    if file_ext == '.xlsx':
        sheet = openpyxl.load_workbook(filepath, data_only=True).active
        return list(sheet.iter_rows(values_only=True))
    elif file_ext == '.xls':
        sheet = xlrd.open_workbook(filepath).sheet_by_index(0)
        return [sheet.row_values(i) for i in range(sheet.nrows)]
    raise ValueError(f"Неподдерживаемый формат файла: {file_ext}")

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

def parse_trade(row: List[Any], trade_type: str, ticker: str, isin: Optional[str]) -> OperationDTO:
    is_buy = bool(row[3])
    stock_mode = trade_type == "stock"

    price = row[4] if is_buy else row[7 if stock_mode else 8]
    quantity = row[3] if is_buy else row[6 if stock_mode else 7]
    payment = row[5] if is_buy else row[8 if stock_mode else 9]
    currency = normalize_currency(row[10 if stock_mode else 11])
    comment = str(safe_get(row, 17 if stock_mode else 18, "")).strip()
    operation_id = str(row[1]).strip() if row[1] else None

    trade_date = parse_date(row[11 if stock_mode else 13])
    trade_time = parse_time(row[12]) if stock_mode else "00:00:00"

    aci = None
    if not stock_mode:
        aci = safe_get(row, 6) or safe_get(row, 10)

    full_date = f"{trade_date} {trade_time}" if trade_date else None

    return OperationDTO(
        date=full_date or "",
        operation_type="buy" if is_buy else "sell",
        payment_sum=payment,
        currency=currency,
        ticker=ticker,
        isin=isin if stock_mode else ticker,
        price=price,
        quantity=int(quantity),
        aci=aci,
        comment=comment,
        operation_id=operation_id,
        _sort_key=full_date
    )


# --- Основной парсинг ---
def parse_trades(filepath: str) -> List[Dict[str, Any]]:
    file_ext = os.path.splitext(filepath)[1].lower()
    rows = read_excel_file(filepath, file_ext)

    result = []
    current_ticker = current_isin = None
    parsing_trades = parsing_stocks = parsing_bonds = False

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
            parsing_stocks, parsing_bonds = True, False
            continue
        if is_section_start(row, ["облигация"]):
            parsing_stocks, parsing_bonds = False, True
            continue
        if is_section_start(row, ["заем", "овернайт", "цб"]):
            break

        if is_valid_trade_row(row):
            trade_type = "stock" if parsing_stocks else "bond"
            dto = parse_trade(row, trade_type, current_ticker, current_isin)
            result.append(dto)

    # Сортировка и преобразование к словарю без _sort_key
    return [dict((k, v) for k, v in op.__dict__.items() if not k.startswith("_"))
            for op in sorted(result, key=lambda x: (x._sort_key is None, x._sort_key))]


filepath = "2.xls"
trades = parse_trades(filepath)
print(json.dumps(trades, ensure_ascii=False, indent=2))
