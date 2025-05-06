from typing import List, Optional, Dict, Any
from dataclasses import dataclass
from datetime import datetime
import openpyxl
import xlrd
import os
import re
import json

currency_dict = {
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
    _sort_key: Optional[str] = None  # внутреннее поле, для сортировки по дате-времени

def read_excel_file(filepath: str, file_ext: str) -> List[List[Any]]:
    if file_ext == '.xlsx':
        sheet = openpyxl.load_workbook(filepath, data_only=True).active
        return list(sheet.iter_rows(values_only=True))
    elif file_ext == '.xls':
        sheet = xlrd.open_workbook(filepath).sheet_by_index(0)
        return [sheet.row_values(i) for i in range(sheet.nrows)]
    else:
        raise ValueError('Неподдерживаемый формат файла')

def normalize_currency(value: Any) -> str:
    if not value:
        return ""
    value = str(value).strip().upper()
    return currency_dict.get(value, value)

def clean_date(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, (int, float)):
        try:
            date_obj = datetime(*xlrd.xldate_as_tuple(value, 0))
            return date_obj.strftime("%Y-%m-%d")
        except Exception:
            return None
    if isinstance(value, str):
        value = value.strip()
        for fmt in ("%d.%m.%Y", "%d.%m.%y"):
            try:
                return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
    return None

def clean_time(value):
    if value is None:
        return "00:00:00"
    if isinstance(value, datetime):
        return value.strftime("%H:%M:%S")
    if isinstance(value, (int, float)):
        try:
            time_obj = datetime(*xlrd.xldate_as_tuple(value, 0)).time()
            return time_obj.strftime("%H:%M:%S")
        except Exception:
            return "00:00:00"
    if isinstance(value, str):
        value = value.strip()
        for fmt in ("%H:%M:%S", "%H:%M"):
            try:
                return datetime.strptime(value, fmt).time().strftime("%H:%M:%S")
            except ValueError:
                continue
    return "00:00:00"

def detect_new_ticker(row: List[Any]) -> bool:
    return any('isin' in str(cell).lower() for cell in row if cell)

def extract_ticker(row: List[Any]) -> Optional[str]:
    return row[0].split()[0].strip() if row and isinstance(row[0], str) else None

def extract_isin(row: List[Any]) -> Optional[str]:
    for i, cell in enumerate(row):
        if isinstance(cell, str) and 'isin' in cell.lower():
            match = re.search(r'ISIN:\s*([A-Z0-9]+)', cell)
            if match:
                return match.group(1)
            if i + 1 < len(row):
                next_cell = row[i + 1]
                if isinstance(next_cell, str) and re.match(r'^[A-Z0-9]{12}$', next_cell.strip()):
                    return next_cell.strip()
    return None

def is_valid_trade_row(row: List[Any]) -> bool:
    if not row or not row[0] or (isinstance(row[0], str) and row[0].lower().startswith('итого')):
        return False
    return any(isinstance(cell, (int, float)) for cell in row)

def is_section_start(row: List[Any], keywords: List[str]) -> bool:
    return any(keyword in str(cell).lower() for cell in row if cell for keyword in keywords)

def safe_get(lst: List[Any], idx: int) -> Any:
    return lst[idx] if idx < len(lst) else None

def parse_trade(row: List[Any], trade_type: str, ticker: str, isin: Optional[str] = None) -> OperationDTO:
    is_buy = bool(row[3])
    stock_mode = trade_type == "stock"

    price = row[4] if is_buy else row[7 if stock_mode else 8]
    quantity = row[3] if is_buy else row[6 if stock_mode else 7]
    payment = row[5] if is_buy else row[8 if stock_mode else 9]
    raw_currency = row[10 if stock_mode else 11]
    currency = normalize_currency(raw_currency)

    comment_index = 17 if stock_mode else 18
    comment = str(row[comment_index]).strip() if len(row) > comment_index else ""

    operation_id = str(row[1]).strip() if row[1] else None

    if stock_mode:
        trade_date = clean_date(row[11])
        trade_time = clean_time(row[12])
    else:
        trade_date = clean_date(row[13])
        trade_time = "00:00:00"

    sort_key = f"{trade_date} {trade_time}" if trade_date else None

    return OperationDTO(
        date=f"{trade_date} {trade_time}" if trade_date and trade_time else trade_date,
        operation_type="buy" if is_buy else "sell",
        payment_sum=payment,
        currency=currency,
        ticker=ticker,
        isin=isin if stock_mode else ticker,
        price=price,
        quantity = int(quantity),
        aci="" if stock_mode else (row[6] or row[10]),
        comment=comment,
        operation_id=operation_id,
        _sort_key=sort_key
    )

def parse_trades(filepath: str) -> List[Dict[str, Any]]:
    file_ext = os.path.splitext(filepath)[1].lower()
    rows = read_excel_file(filepath, file_ext)

    result = []
    current_ticker, current_isin = None, None
    parsing_trades = parsing_stocks = parsing_bonds = False

    for row in rows:
        row = row[1:]  # Пропуск первой колонки

        if not parsing_trades:
            if "2.1. сделки:" in ' '.join(map(str, row)).lower():
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
            if parsing_stocks:
                result.append(parse_trade(row, "stock", current_ticker, current_isin))
            elif parsing_bonds:
                result.append(parse_trade(row, "bond", current_ticker))

    # Сортировка по дате-времени (внутреннее поле)
    result.sort(key=lambda x: (x._sort_key is None, x._sort_key))

    # Преобразуем в список словарей и убираем поле _sort_key
    return [dict((k, v) for k, v in op.__dict__.items() if not k.startswith('_')) for op in result]

# Пример запуска
filepath = "2.xls"
trades = parse_trades(filepath)
print(json.dumps(trades, ensure_ascii=False, indent=2))
