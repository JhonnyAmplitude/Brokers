import xlrd
import json
import re

from datetime import datetime

file_path = "B_k-494884_ALL_23-04-COPY.xls"

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

def parse_date(date_str):
    for fmt in ("%d.%m.%Y", "%d.%m.%y"):
        try:
            return datetime.strptime(date_str.strip(), fmt).date().isoformat()
        except:
            continue
    return None


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
    row_str = " ".join([str(cell) for cell in row[1:]]).strip()  # Сдвиг с учетом смещения

    # Шапка
    if "Генеральное соглашение:" in row_str:
        print(f"[Agreement Debug] Row {row_idx} → {row_str}")  # ← отладка
        parts = row_str.split()
        for i, p in enumerate(parts):
            if "/" in p:
                header_data["account_id"] = p.split("/")[0]
            if p == "от" and i + 1 < len(parts):
                header_data["account_date_start"] = parse_date(parts[i + 1])


    elif "Период:" in row_str and "по" in row_str:
        print(f"[Header Debug] Row {row_idx} → {row_str}")  # ← Добавим для отладки

        try:
            parts = row_str.split()
            # Найти дату после слова "с"
            for i in range(len(parts)):
                if parts[i] == "с" and i + 1 < len(parts):
                    header_data["date_start"] = parse_date(parts[i + 1])

                if parts[i] == "по" and i + 1 < len(parts):
                    header_data["date_end"] = parse_date(parts[i + 1])
        except Exception as e:
            print(f"Failed to parse period on row {row_idx}: {e}")

    # Валюта
    elif row_str in ["Рубль", "USD", "EUR"]:
        currency = row_str

    elif (
        "Дата" in row_str and
        "Операция" in row_str and
        "Сумма зачисления" in row_str
    ):
        parsing_operations = True
        continue

    elif row and isinstance(row[1], str) and parse_date(row[1]):
        operation = row[2].strip()

        # Пропускаем операции: "Займы \"овернайт\"", "Покупка/Продажа", "НКД от операций", "Покупка/Продажа (репо)"
        skip_operations = [
            'Займы "овернайт"',
            "Покупка/Продажа",
            "НКД от операций",
            "Покупка/Продажа (репо)"
        ]

        if operation in skip_operations:
            continue  # Пропускаем текущую строку

        # Проверка на валидные операции и пропуск строки, если операция "Итого"
        if operation in valid_operations and "Итого" not in operation:
            note = ""

            # Обработка примечания (ячейки с 14 по 19)
            if len(row) > 14:
                raw_notes = row[14:19]
                combined_note = " ".join(str(cell).strip() for cell in raw_notes if cell)
                note = combined_note.split(",")[0] if combined_note else ""

            # Определение типа операции
            operation_type = ""

            # Спецификация для "Приход ДС"
            if operation == "Приход ДС":
                operation_type = "deposit"
            if operation == "Погашение купона":
                operation_type = "coupon"
            elif operation == "Дивиденды":
                operation_type = "dividend"
            elif operation == "Погашение облигации":
                operation_type = "repayment"
            elif operation == "Частичное погашение облигации":
                operation_type = "amortization"
            elif operation == "Вывод ДС":
                operation_type = "withdrawal"

            # Обработка "Проценты по займам 'овернайт'" и "Проценты по займам 'овернайт ЦБ'" одинаково
            if operation in ['Проценты по займам "овернайт"', 'Проценты по займам "овернайт ЦБ"']:
                income = str(row[6]).strip() if len(row) > 6 else ""  # Сумма зачисления на индексе 6
                expense = str(row[7]).strip() if len(row) > 7 else ""  # Сумма списания на индексе 7
                print(f"[DEBUG] Row {row_idx} → income: '{income}', expense: '{expense}'")  # Дебаг

                # Устанавливаем operation_type в зависимости от значений
                if income and income != "0" and income != "0.0":  # Если есть значение в "Сумма зачисления"
                    operation_type = "other_income"
                elif expense and expense != "0" and expense != "0.0":  # Если есть значение в "Сумма списания"
                    operation_type = "other_expense"

            # Добавляем операцию в результат
            operations.append({
                "date": parse_date(row[1]),
                "operation": operation,
                "operation_type": operation_type,
                "currency": currency,
                "comment": note,
            })

result = {
    **header_data,
    "operations": operations
}
print(json.dumps(result, ensure_ascii=False, indent=2))
