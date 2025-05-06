import os
import re
import json
from datetime import datetime
from typing import Any, Generator, List, Dict, Optional, Tuple, Union
from dataclasses import dataclass
from itertools import tee
import xlrd
import openpyxl

# --- Константы ---

VALID_OPERATIONS = {
    "Проценты по займам \"овернайт ЦБ\"", "Приход ДС", "Проценты по займам \"овернайт\"",
    "Покупка/Продажа", "НКД от операций", "Погашение купона", "Вознаграждение компании",
    "Переводы между площадками", "Дивиденды", "Покупка/Продажа (репо)",
    "Частичное погашение облигации", "Погашение облигации", "Вывод ДС", "НДФЛ",
}

SKIP_OPERATIONS = {
    'Займы "овернайт"', "Покупка/Продажа", "НКД от операций", "Покупка/Продажа (репо)",
}

OPERATION_TYPE_MAP = {
    "Приход ДС": "deposit",
    "Погашение купона": "coupon",
    "Дивиденды": "dividend",
    "Погашение облигации": "repayment",
    "Частичное погашение облигации": "amortization",
    "Вывод ДС": "withdrawal",
}

def operation_handler_template(pos_type: str, neg_type: str):
    return lambda income, expense: pos_type if is_nonzero(income) else neg_type

SPECIAL_OPERATION_HANDLERS = {
    'Проценты по займам "овернайт"': operation_handler_template("other_income", "other_expense"),
    'Проценты по займам "овернайт ЦБ"': operation_handler_template("other_income", "other_expense"),
    "Вознаграждение компании": operation_handler_template("commission", "commission_refund"),
    "НДФЛ": operation_handler_template("refund", "withholding"),
}

NOTE_COLUMNS = slice(14, 19)

# --- Структура данных ---

@dataclass
class OperationDTO:
    date: str
    operation_type: str
    payment_sum: float
    currency: str
    isin: Optional[str] = ''
    ticker: Optional[str] = ''
    instrument_name: Optional[str] = ''
    price: Optional[float] = 0.0
    commission: Optional[float] = 0.0
    quantity: Optional[int] = 0
    nkd: Optional[float] = 0.0
    comment: Optional[str] = ''
    operation_id: Optional[str] = ''


# --- Вспомогательные функции ---

def is_nonzero(value: Any) -> bool:
    try:
        return float(str(value).replace(",", ".").replace(" ", "")) != 0
    except (ValueError, TypeError):
        return False

def parse_date(date_value: Union[str, int, float]) -> Optional[str]:
    if not date_value:
        return None
    if isinstance(date_value, (int, float)):
        try:
            dt = datetime(*xlrd.xldate_as_tuple(date_value, 0))
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return None
    date_str = str(date_value or "").strip()
    for fmt in ("%d.%m.%Y", "%d.%m.%y"):
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    return None

def extract_isin(comment: str) -> Optional[str]:
    match = re.search(r'\b[A-Z]{2}[A-Z0-9]{10}\b', comment)
    return match.group(0) if match else None

def extract_dividend_details(comment: str) -> Dict[str, str]:
    result = {}
    parts = comment.split(",")
    if parts:
        result["instrument_name"] = parts[0].strip()
    isin = extract_isin(comment)
    if isin:
        result["isin"] = isin
    match = re.search(r'налог\s+([\d\s]+,\d{2})', comment)
    if match:
        result["withholding"] = match.group(1).replace(" ", "").replace(",", ".")
    return result

def extract_note(row: List[Any]) -> str:
    return " ".join(str(cell).strip() for cell in row[NOTE_COLUMNS] if cell)

def get_rows_from_file(file_path: str) -> Generator[List[Any], None, None]:
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".xls":
        sheet = xlrd.open_workbook(file_path).sheet_by_index(0)
        for i in range(sheet.nrows):
            yield sheet.row_values(i)
    elif ext == ".xlsx":
        sheet = openpyxl.load_workbook(file_path, data_only=True).active
        for row in sheet.iter_rows(values_only=True):
            yield list(row)
    else:
        raise ValueError("Unsupported file format")

def parse_float(value: str) -> float:
    try:
        return float(str(value).replace('\xa0', '').replace(",", ".").strip())
    except (ValueError, TypeError):
        return 0.0


# --- Парсинг операций ---

def process_operation_row(row: List[Any], currency: str) -> Optional[OperationDTO]:
    date = parse_date(row[1])
    if not date:
        return None
    operation = str(row[2]).strip()
    if operation not in VALID_OPERATIONS or operation in SKIP_OPERATIONS:
        return None
    income, expense = str(row[6]).strip(), str(row[7]).strip()
    payment_sum = parse_float(income) if is_nonzero(income) else parse_float(expense)
    comment = extract_note(row)
    operation_type = OPERATION_TYPE_MAP.get(operation, "")
    if handler := SPECIAL_OPERATION_HANDLERS.get(operation):
        operation_type = handler(income, expense)

    operation_dto = OperationDTO(
        date=date,
        operation_type=operation_type,
        payment_sum=payment_sum,
        currency=currency,
        comment=comment,
        operation_id=str(row[0]),
    )

    if operation == "Погашение купона":
        operation_dto.isin = extract_isin(comment)
    elif operation == "Дивиденды":
        details = extract_dividend_details(comment)
        operation_dto.instrument_name = details.get("instrument_name", "")
        operation_dto.isin = details.get("isin", "")
        operation_dto.commission = parse_float(details.get("withholding", "0.0"))

    return operation_dto


def parse_rows(rows: Generator[List[Any], None, None]) -> Tuple[Dict[str, Optional[str]], List[OperationDTO]]:
    header = {"account_id": None, "account_date_start": None, "date_start": None, "date_end": None}
    operations = []
    currency = None
    parsing = False

    for row in rows:
        row_str = " ".join(str(cell) for cell in row[1:] if cell).strip()

        if "Генеральное соглашение:" in row_str:
            if match := re.search(r"Генеральное соглашение:\s*(\d+)", row_str):
                header["account_id"] = match.group(1)
            if date_match := re.search(r"от\s+(\d{2}\.\d{2}\.\d{4})", row_str):
                header["account_date_start"] = parse_date(date_match.group(1))

        elif "Период:" in row_str and "по" in row_str:
            parts = row_str.split()
            try:
                header["date_start"] = parse_date(parts[parts.index("с") + 1])
                header["date_end"] = parse_date(parts[parts.index("по") + 1])
            except (IndexError, ValueError):
                pass

        elif row_str in {"Рубль", "USD", "EUR"}:
            currency = "RUB" if row_str == "Рубль" else row_str

        elif all(x in row_str for x in ["Дата", "Операция", "Сумма зачисления"]):
            parsing = True
            continue

        elif parsing:
            if dto := process_operation_row(row, currency):
                operations.append(dto)

    return header, operations


# --- Парсинг сделок ---

def parse_stock_row(row, isin, name, reg_number):
    return {
        "date": parse_date(row[1]),
        "number": row[2],
        "quantity": float(row[4]),
        "price": float(row[5]),
        "payment": float(row[6]),
        "isin": isin,
        "name": name,
        "reg_number": reg_number
    }

def parse_bond_row(row, isin, name, reg_number):
    return {
        "date": parse_date(row[1]),
        "number": row[2],
        "quantity": float(row[4]),
        "price": float(row[5]),
        "payment": float(row[6]),
        "nkd_purchase": float(row[7]),
        "isin": isin,
        "name": name,
        "reg_number": reg_number
    }

def parse_trades_from_rows(rows: Generator[List[Any], None, None]) -> Dict[str, List[Dict[str, Any]]]:
    stocks, bonds = [], []
    current_type, reg_number, isin, name = None, None, None, None

    for row in rows:
        row_str = [str(cell) for cell in row if cell]

        if any("Номер рег." in cell for cell in row_str):
            for i, cell in enumerate(row_str):
                if "Номер рег." in cell:
                    reg_number = row_str[i + 1].strip()
                elif "ISIN" in cell:
                    isin = row_str[i + 1].strip()
                elif "Общество" in cell or "Публичное" in cell:
                    name = cell.strip()
            continue

        if any("Облигация" in cell for cell in row_str):
            current_type = "bonds"
            continue
        if any("Акция" in cell for cell in row_str):
            current_type = "stocks"
            continue

        if not row_str:
            continue

        try:
            if current_type == "bonds":
                bonds.append(parse_bond_row(row, isin, name, reg_number))
            elif current_type == "stocks":
                stocks.append(parse_stock_row(row, isin, name, reg_number))
        except Exception as e:
            print(f"⚠️ Ошибка при парсинге сделки: {row_str} → {e}")

    return {"stocks": stocks, "bonds": bonds}


# --- Главная функция ---

def parse_full_statement(filepath: str) -> dict:
    rows = list(get_rows_from_file(filepath))
    rows1, rows2 = tee(rows)
    header, operations = parse_rows(rows1)
    trades = parse_trades_from_rows(rows2)

    return {
        "account_info": header,
        "operations": [op.__dict__ for op in operations],
        "stocks": trades.get("stocks", []),
        "bonds": trades.get("bonds", [])
    }


# --- Запуск ---
filepath = "2.xls"  # Путь к файлу
result = parse_full_statement(filepath)
print(json.dumps(result, ensure_ascii=False, indent=2))
