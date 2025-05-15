import json
import re
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional, Tuple

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
from utils import parse_date, extract_rows


def extract_isin(comment: str) -> Optional[str]:
    match = re.search(r'\b[A-Z]{2}[A-Z0-9]{10}\b', comment)
    return match.group(0) if match else ""


def extract_note(row: List[Any]) -> str:
    NOTE_COLUMNS = slice(14, 19)
    return " ".join(str(cell).strip() for cell in row[NOTE_COLUMNS] if cell and str(cell).strip())


def safe_float(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(str(value).replace(',', '.'))
    except (ValueError, TypeError):
        return 0.0


def detect_operation_type(op: str, income: str, expense: str) -> str:
    if not isinstance(op, str):
        return "other"
    if "Покупка" in op:
        return "buy"
    if "Продажа" in op:
        return "sell"
    if op in SPECIAL_OPERATION_HANDLERS:
        return SPECIAL_OPERATION_HANDLERS[op](income, expense)
    return OPERATION_TYPE_MAP.get(op, "other")


def get_cell(row: List[Any], index: int) -> Any:
    if not isinstance(row, list):
        return ""
    if not isinstance(index, int) or index < 0 or index >= len(row):
        return ""
    return row[index]


def process_operation_row(
    row: List[Any],
    currency: str,
    stock_mode: bool,
    ticker: str,
    operation_id: str
) -> Optional[OperationDTO]:
    raw_date = get_cell(row, 1)
    operation = str(get_cell(row, 2)).strip()

    if operation not in VALID_OPERATIONS or operation in SKIP_OPERATIONS:
        return None

    date = parse_date(raw_date)
    if not date:
        return None

    income = str(get_cell(row, 6)).strip()
    expense = str(get_cell(row, 7)).strip()
    payment_sum = income if is_nonzero(income) else expense
    payment_sum = safe_float(payment_sum)

    comment = extract_note(row)
    operation_type = detect_operation_type(operation, income, expense)
    currency_value = CURRENCY_DICT.get(currency or "", currency or "")

    price = safe_float(get_cell(row, 8))
    quantity = safe_float(get_cell(row, 9))

    aci = 0.0
    # if not stock_mode:
    #     aci = safe_float(get_cell(row, 6)) or safe_float(get_cell(row, 10))

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
        operation_id=operation_id,
    )


def parse_header_data(row_str: str, header_data: Dict[str, Optional[str]]) -> None:
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
        except (ValueError, IndexError, TypeError):
            pass


def is_table_header(row_str: str) -> bool:
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
        if operation in SKIP_OPERATIONS or operation not in VALID_OPERATIONS:
            continue

        entry = process_operation_row(row, current_currency, stock_mode, ticker, operation_id)
        if entry:
            operations.append(entry)

    return header_data, operations


def parse_full_statement(file_path: str) -> Dict[str, Any]:
    try:
        rows = list(extract_rows(file_path))
    except Exception as e:
        raise RuntimeError(f"Ошибка при чтении файла {file_path}: {e}")

    if not rows:
        raise ValueError(f"Файл {file_path} пуст или не содержит данных.")

    header_data, financial_operations = parse_financial_operations(iter(rows))
    trade_operations = parse_trades(file_path)
    operations = financial_operations + trade_operations

    operations.sort(key=lambda op: (op._sort_key is None, op._sort_key))

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


result = parse_full_statement("1.xls")
print(json.dumps(result, ensure_ascii=False, indent=2, default=default_operation_dto))
