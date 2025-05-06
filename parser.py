import json
import os
import re
from datetime import datetime
from typing import Any, Generator, List, Dict, Optional, Tuple, Union

import xlrd
import openpyxl

FILE_PATH = "6.xls"


VALID_OPERATIONS = {
    "Проценты по займам \"овернайт ЦБ\"", "Приход ДС", "Проценты по займам \"овернайт\"",
    "Погашение купона", "Вознаграждение компании",
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


currency_dict = {
    "SEK": "SEK", "NOK": "NOK", "AED": "AED", "XAG": "XAG", "ZAR": "ZAR",
    "TRY": "TRY", "XAU": "XAU", "HKD": "HKD", "TJS": "TJS", "UZS": "UZS",
    "KGS": "KGS", "KZT": "KZT", "JPY": "JPY", "AMD": "AMD", "Рубль": "RUB",
    "USD": "USD", "EUR": "EUR", "BYN": "BYN", "GBP": "GBP", "CHF": "CHF", "CNY": "CNY"
}

# --- Вспомогательные функции ---
def is_nonzero(value: Any) -> bool:
    try:
        return float(str(value).replace(",", ".").replace(" ", "")) != 0
    except (ValueError, TypeError):
        return False


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


def parse_date(date_str: Union[str, int, float]) -> Optional[str]:
    if not date_str:
        return None
    if isinstance(date_str, (int, float)):
        try:
            result = datetime(*xlrd.xldate_as_tuple(date_str, 0)).date().isoformat()
            print(f"Parsed date from float: {date_str} -> {result}")  # Логирование
            return result
        except Exception:
            return None

    date_str = str(date_str).strip()
    for fmt in ("%d.%m.%Y", "%d.%m.%y"):
        try:
            result = datetime.strptime(date_str, fmt).date().isoformat()
            return result
        except ValueError:
            continue
    return None



def extract_note(row: List[Any]) -> str:
    NOTE_COLUMNS = slice(14, 19)
    return " ".join(str(cell).strip() for cell in row[NOTE_COLUMNS] if cell)


# --- Парсинг Excel ---

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
        raise ValueError("Неподдерживаемый формат файла")


# --- Основной парсер ---
def process_operation_row(row: List[Any], currency: str, stock_mode: bool, ticker: str, operation_id: str) -> Optional[Dict[str, Any]]:
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

    # Восстановление старой логики для типа операции
    operation_type = OPERATION_TYPE_MAP.get(operation, "")
    if handler := SPECIAL_OPERATION_HANDLERS.get(operation):
        operation_type = handler(income, expense)

    # Определение типа операции на основе текста (Покупка/Продажа)
    if "Покупка" in operation:
        operation_type = "buy"
    elif "Продажа" in operation:
        operation_type = "sell"

    # Используем currency_dict для получения валюты
    currency_value = currency_dict.get(currency, currency)  # Если валюты нет в словаре, возвращаем саму валюту как есть

    # Получаем цену, количество и ACI (если applicable)
    price = safe_float(row[8])  # или другая колонка, если цена в другом месте
    quantity = safe_float(row[9])  # или другая колонка, если количество в другом месте

    # Собираем итоговую запись в словарь
    entry = {
        "date": date,
        "operation_type": operation_type,  # Возвращаем тип операции
        "payment_sum": payment_sum,
        "currency": currency_value,
        "ticker": ticker,
        "isin": extract_isin(comment) if not stock_mode else ticker,
        "price": price,
        "quantity": quantity,
        "aci": "" if stock_mode else (row[6] or row[10]),  # Проверка на stock_mode для ACI
        "comment": comment,
        "operation_id": operation_id,
    }

    return entry


# --- Основной парсер ---
def parse_rows(rows: Generator[List[Any], None, None]) -> Tuple[Dict[str, Optional[str]], List[Dict[str, Any]]]:
    header_data = {"account_id": None, "account_date_start": None, "date_start": None, "date_end": None}
    operations = []
    currency = None
    parsing = False
    stock_mode = False  # Пример: определите, если нужно
    ticker = ""  # Пример: извлеките или задайте тикер, если необходимо
    operation_id = ""  # Пример: получите или задайте ID операции, если необходимо

    for row in rows:
        row_str = " ".join(str(cell) for cell in row[1:] if cell).strip()

        if "Генеральное соглашение:" in row_str:
            print("Processing row:", row_str)  # Логируем строку для отладки

            # Используем регулярное выражение для поиска номера соглашения
            agreement_match = re.search(r"Генеральное соглашение:\s*(\d+)", row_str)
            if agreement_match:
                account_id = agreement_match.group(1)
                header_data["account_id"] = account_id
                print(f"Extracted account_id: {account_id}")  # Логируем для отладки

            # Используем регулярное выражение для поиска даты после "от"
            date_match = re.search(r"от\s+(\d{2}\.\d{2}\.\d{4})", row_str)
            if date_match:
                date_str = date_match.group(1)
                header_data["account_date_start"] = parse_date(date_str)
                print(f"Extracted account_date_start: {header_data['account_date_start']}")  # Логируем для отладки

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
            # Передаем дополнительные аргументы в process_operation_row
            entry = process_operation_row(row, currency, stock_mode, ticker, operation_id)
            if entry:
                operations.append(entry)

    return header_data, operations


def safe_float(value):
    try:
        return float(str(value).replace(',', '.'))
    except (ValueError, TypeError):
        return None


rows = list(get_rows_from_file(FILE_PATH))
header_data, operations = parse_rows(rows)
result = {
    **header_data,
    "operations": operations,
}

print(json.dumps(result, ensure_ascii=False, indent=2))






