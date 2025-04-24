import xlrd
import json
from datetime import datetime

file_path = "B_k-494884_ALL_23-04.xls"

valid_operations = [
    "Проценты по займам \"овернайт ЦБ\"",
    "Приход ДС",
    "Займы \"овернайт\"",
    "Проценты по займам \"овернайт\"",
    "Покупка/Продажа",
    "НКД от операций",
    "Погашение купона",
    "Вознаграждение компании",
    "Переводы между площадками",
    "Дивиденды",
    "Покупка/Продажа (репо)",
    "Частичное погашение облигации",
    "Погашение облигации",
    "Вывод ДС",
    "НДФЛ",
]

skip_operations = [
    'Займы "овернайт"',
    "Покупка/Продажа",
    "НКД от операций",
    "Покупка/Продажа (репо)"
]

operation_type_map = {
    "Приход ДС": "deposit",
    "Погашение купона": "coupon",
    "Дивиденды": "dividend",
    "Погашение облигации": "repayment",
    "Частичное погашение облигации": "amortization",
    "Вывод ДС": "withdrawal",
}

def parse_date(date_str):
    for fmt in ("%d.%m.%Y", "%d.%m.%y"):
        try:
            return datetime.strptime(date_str.strip(), fmt).date().isoformat()
        except:
            continue
    return None

def extract_note(row):
    if len(row) > 14:
        combined_note = " ".join(filter(None, map(str.strip, map(str, row[14:19]))))
        return combined_note.split(",")[0] if combined_note else ""
    return ""

header_data = {
    "account_id": None,
    "account_date_start": None,
    "date_start": None,
    "date_end": None
}
operations = []
currency = None
parsing_operations = False

book = xlrd.open_workbook(file_path)
sheet = book.sheet_by_index(0)

for row_idx in range(sheet.nrows):
    row = sheet.row_values(row_idx)
    row_str = " ".join(str(cell) for cell in row[1:]).strip()

    if "Генеральное соглашение:" in row_str:
        parts = row_str.split()
        for i, p in enumerate(parts):
            if "/" in p:
                header_data["account_id"] = p.split("/")[0]
            if p == "от" and i + 1 < len(parts):
                header_data["account_date_start"] = parse_date(parts[i + 1])

    elif "Период:" in row_str and "по" in row_str:
        parts = row_str.split()
        for i in range(len(parts)):
            if parts[i] == "с" and i + 1 < len(parts):
                header_data["date_start"] = parse_date(parts[i + 1])
            elif parts[i] == "по" and i + 1 < len(parts):
                header_data["date_end"] = parse_date(parts[i + 1])

    elif row_str in ["Рубль", "USD", "EUR"]:
        currency = row_str

    elif all(x in row_str for x in ["Дата", "Операция", "Сумма зачисления"]):
        parsing_operations = True
        continue

    elif parsing_operations:
        parsed_date = parse_date(row[1]) if isinstance(row[1], str) else None
        if not parsed_date:
            continue

        operation = row[2].strip()
        if operation not in valid_operations or operation in skip_operations:
            continue

        note = extract_note(row)
        operation_type = operation_type_map.get(operation, "")

        income = str(row[6]).strip() if len(row) > 6 else ""
        expense = str(row[7]).strip() if len(row) > 7 else ""
        payment_sum = income if income and income not in ["0", "0.0"] else expense

        if operation in ['Проценты по займам "овернайт"', 'Проценты по займам "овернайт ЦБ"']:
            if income and income not in ["0", "0.0"]:
                operation_type = "other_income"
            elif expense and expense not in ["0", "0.0"]:
                operation_type = "other_expense"

        operations.append({
            "date": parsed_date,
            "operation": operation,
            "operation_type": operation_type,
            "currency": currency,
            "comment": note,
            "payment_sum": payment_sum,
        })

result = {
    **header_data,
    "operations": operations
}

print(json.dumps(result, ensure_ascii=False, indent=2))
