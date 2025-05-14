import json
import os
import re

from datetime import datetime
from typing import Any, Dict, Generator, List, Optional, Tuple

import openpyxl
import xlrd

from OperationDTO import OperationDTO
from constatns import (
    CURRENCY_DICT,
    OPERATION_TYPE_MAP,
    SKIP_OPERATIONS,
    SPECIAL_OPERATION_HANDLERS,
    VALID_OPERATIONS,
    is_nonzero,
)
from fin import parse_trades
from utils import parse_date


def extract_isin(comment: str) -> Optional[str]:
    """
    Извлечение ISIN из комментария.
    """
    match = re.search(r'\b[A-Z]{2}[A-Z0-9]{10}\b', comment)
    return match.group(0) if match else None

def extract_note(row: List[Any]) -> str:
    """
    Извлечение заметки из строки.
    """
    NOTE_COLUMNS = slice(14, 19)
    return " ".join(str(cell).strip() for cell in row[NOTE_COLUMNS] if cell)

def safe_float(value: Any) -> Optional[float]:
    """
    Преобразование значения в float с безопасной обработкой ошибок.
    """
    try:
        return float(str(value).replace(',', '.'))
    except (ValueError, TypeError):
        return None

def extract_rows(file_path: str) -> Generator[List[Any], None, None]:
    """
    Чтение строк из файла Excel (форматы .xls или .xlsx).
    Поддерживает как чтение из файлов, так и чтение из байтовых данных.
    """
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


def detect_operation_type(op: str, income: str, expense: str) -> str:
    """
    Определение типа операции по названию и значению.
    """
    if "Покупка" in op:
        return "buy"
    if "Продажа" in op:
        return "sell"
    if op in SPECIAL_OPERATION_HANDLERS:
        return SPECIAL_OPERATION_HANDLERS[op](income, expense)
    return OPERATION_TYPE_MAP.get(op, "other")


def get_cell(row: List[Any], index: int) -> Any:
    """
    Безопасное получение ячейки по индексу.
    """
    return row[index] if len(row) > index else None


def process_operation_row(
    row: List[Any],
    currency: str,
    stock_mode: bool,
    ticker: str,
    operation_id: str
) -> Optional[OperationDTO]:
    """
    Обработка строки операции и создание объекта OperationDTO.
    """
    raw_date = get_cell(row, 1)
    operation = str(get_cell(row, 2)).strip()

    # Пропуск неподходящих операций
    if operation not in VALID_OPERATIONS or operation in SKIP_OPERATIONS:
        return None

    date = parse_date(raw_date)
    if not date:
        return None

    income = str(get_cell(row, 6)).strip()
    expense = str(get_cell(row, 7)).strip()
    payment_sum = income if is_nonzero(income) else expense

    comment = extract_note(row)
    operation_type = detect_operation_type(operation, income, expense)
    currency_value = CURRENCY_DICT.get(currency, currency)

    price = safe_float(get_cell(row, 8))
    quantity = safe_float(get_cell(row, 9))

    aci = None
    if not stock_mode:
        aci = safe_float(get_cell(row, 6)) or safe_float(get_cell(row, 10))

    return OperationDTO(
        date=date,
        operation_type=operation_type,
        payment_sum=payment_sum,
        currency=currency_value,
        ticker=ticker,
        isin=extract_isin(comment),
        price=price,
        quantity=quantity,
        aci=aci,
        comment=comment,
        operation_id=operation_id
    )



def parse_header_data(row_str: str, header_data: Dict[str, Optional[str]]) -> None:
    """
    Обновляет словарь header_data на основе строки.
    """
    if "Генеральное соглашение:" in row_str:
        match = re.search(r"Генеральное соглашение:\s*(\d+)", row_str)
        if match:
            header_data["account_id"] = match.group(1)
        date_match = re.search(r"от\s+(\d{2}\.\d{2}\.\d{4})", row_str)
        if date_match:
            header_data["account_date_start"] = parse_date(date_match.group(1))

    elif "Период:" in row_str and "по" in row_str:
        parts = row_str.split()
        try:
            header_data["date_start"] = parse_date(parts[parts.index("с") + 1])
            header_data["date_end"] = parse_date(parts[parts.index("по") + 1])
        except (ValueError, IndexError):
            pass


def is_table_header(row_str: str) -> bool:
    """
    Проверяет, является ли строка заголовком таблицы операций.
    """
    return all(header in row_str for header in ["Дата", "Операция", "Сумма зачисления"])


def parse_financial_operations(
    rows: Generator[List[Any], None, None]
) -> Tuple[Dict[str, Optional[str]], List[OperationDTO]]:
    header_data = {"account_id": None, "account_date_start": None, "date_start": None, "date_end": None}
    operations = []
    current_currency = None
    parsing = False
    stock_mode = False
    ticker = ""
    operation_id = ""

    for row in rows:
        row_str = " ".join(str(cell).strip() for cell in row[1:] if cell).strip()

        if not row_str:
            continue

        if row_str in CURRENCY_DICT:
            current_currency = row_str
            continue

        if is_table_header(row_str):
            parsing = True
            continue

        if not parsing:
            parse_header_data(row_str, header_data)
            continue

        operation = str(get_cell(row, 2)).strip()
        if operation in SKIP_OPERATIONS:
            continue

        if operation not in VALID_OPERATIONS:
            continue

        entry = process_operation_row(row, current_currency, stock_mode, ticker, operation_id)
        if entry:
            operations.append(entry)

    return header_data, operations


def parse_full_statement(file_path: str) -> Dict[str, Any]:
    rows = list(extract_rows(file_path))
    header_data, financial_operations = parse_financial_operations(iter(rows))
    trade_operations = parse_trades(file_path)
    operations = financial_operations + trade_operations

    # Сортируем до сериализации
    operations.sort(key=lambda op: (op._sort_key is None, op._sort_key))

    # Превращаем в dict перед отдачей
    operations_dict = [
        {k: v for k, v in op.__dict__.items() if not k.startswith("_")}
        for op in operations
    ]

    return {
        "account_id": header_data.get("account_id"),
        "account_date_start": header_data.get("account_date_start"),
        "date_start": header_data.get("date_start"),
        "date_end": header_data.get("date_end"),
        "operations": operations_dict,
    }


def default_operation_dto(obj):
    if isinstance(obj, OperationDTO):
        return obj.to_dict()
    raise TypeError(f"Тип {obj.__class__.__name__} не сериализуем")


result = parse_full_statement("pensil.XLSX")
print(json.dumps(result, ensure_ascii=False, indent=2, default=default_operation_dto))
