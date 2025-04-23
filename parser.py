import xlrd
import json
import re

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

    # Начало таблицы операций
    elif (
        "Дата" in row_str and
        "Операция" in row_str and
        "Сумма зачисления" in row_str
    ):
        parsing_operations = True
        continue


    elif parsing_operations:
        if len(row) < 2 or not isinstance(row[1], str) or not parse_date(row[1]):
            continue

        operation = row[2].strip() if len(row) > 2 else ""

        if operation in valid_operations and "Итого" not in operation:
            print(f"Row debug: {row_idx} → {row}")

            # Собираем и обрезаем примечание
            # с учётом сдвига (пропущен первый столбец), берём ячейки 15–19 → это индексы 14–18
            note = ' '.join([str(row[i]).strip() for i in range(14, 19) if i < len(row)]).strip()
            note = note.split(',')[0].strip()

            print(f"Note extracted: {note}")
            operations.append({
                "date": parse_date(row[1]),
                "operation": operation,
                "note": note,
                "currency": currency
            })

result = {
    **header_data,
    "operations": operations
}
print(json.dumps(result, ensure_ascii=False, indent=2))
