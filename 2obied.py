import os
import re
import json
from datetime import datetime
from typing import Any, Generator, List, Dict, Optional, Tuple, Union

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

SPECIAL_OPERATION_HANDLERS = {
    'Проценты по займам "овернайт"': lambda i, e: "other_income" if is_nonzero(i) else "other_expense",
    'Проценты по займам "овернайт ЦБ"': lambda i, e: "other_income" if is_nonzero(i) else "other_expense",
    "Вознаграждение компании": lambda i, e: "commission" if is_nonzero(i) else "commission_refund",
    "НДФЛ": lambda i, e: "refund" if is_nonzero(i) else "withholding",
}

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
    date_str = str(date_value).strip()
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
    NOTE_COLUMNS = slice(14, 19)
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

# --- Первая часть: операции ---

def process_operation_row(row: List[Any], currency: str) -> Optional[Dict[str, Any]]:
    date = parse_date(row[1])
    if not date:
        return None
    operation = str(row[2]).strip()
    if operation not in VALID_OPERATIONS or operation in SKIP_OPERATIONS:
        return None
    income = str(row[6]).strip()
    expense = str(row[7]).strip()
    payment_sum = income if is_nonzero(income) else expense
    comment = extract_note(row)
    operation_type = OPERATION_TYPE_MAP.get(operation, "")
    if handler := SPECIAL_OPERATION_HANDLERS.get(operation):
        operation_type = handler(income, expense)

    entry = {
        "date": date,
        "operation": operation,
        "operation_type": operation_type,
        "currency": currency,
        "comment": comment,
        "payment_sum": payment_sum,
    }
    if operation == "Погашение купона":
        entry["isin"] = extract_isin(comment)
    elif operation == "Дивиденды":
        entry.update(extract_dividend_details(comment))
    return entry

def parse_rows(rows: Generator[List[Any], None, None]) -> Tuple[Dict[str, Optional[str]], List[Dict[str, Any]]]:
    header_data = {"account_id": None, "account_date_start": None, "date_start": None, "date_end": None}
    operations = []
    currency = None
    parsing = False

    for row in rows:
        row_str = " ".join(str(cell) for cell in row[1:] if cell).strip()

        if "Генеральное соглашение:" in row_str:
            agreement_match = re.search(r"Генеральное соглашение:\s*(\d+)", row_str)
            if agreement_match:
                header_data["account_id"] = agreement_match.group(1)
            date_match = re.search(r"от\s+(\d{2}\.\d{2}\.\d{4})", row_str)
            if date_match:
                header_data["account_date_start"] = parse_date(date_match.group(1))

        elif "Период:" in row_str and "по" in row_str:
            parts = row_str.split()
            if "с" in parts and "по" in parts:
                try:
                    header_data["date_start"] = parse_date(parts[parts.index("с") + 1])
                    header_data["date_end"] = parse_date(parts[parts.index("по") + 1])
                except IndexError:
                    pass

        elif row_str in {"Рубль", "USD", "EUR"}:
            currency = row_str

        elif all(x in row_str for x in ["Дата", "Операция", "Сумма зачисления"]):
            parsing = True
            continue

        elif parsing:
            entry = process_operation_row(row, currency)
            if entry:
                operations.append(entry)

    return header_data, operations

# --- Вторая часть: сделки ---

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
    stocks = []
    bonds = []
    current_type = None
    reg_number = None
    isin = None
    name = None

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

# --- Основной сборщик ---

def parse_full_statement(filepath: str) -> dict:
    rows1 = get_rows_from_file(filepath)
    header_data, operations = parse_rows(rows1)

    rows2 = get_rows_from_file(filepath)
    trades_data = parse_trades_from_rows(rows2)

    return {
        "account_info": header_data,
        "operations": operations,
        "stocks": trades_data.get("stocks", []),
        "bonds": trades_data.get("bonds", [])
    }

# --- Запуск ---


filepath = "1.xls"  # Путь к файлу
result = parse_full_statement(filepath)

print(json.dumps(result, ensure_ascii=False, indent=2))
