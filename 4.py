from typing import List, Dict, Any, Optional
import os
import re
import json
import openpyxl
import xlrd
from datetime import datetime

def extract_isin(row):
    """Извлекает ISIN из строки или из соседней ячейки"""
    for i, cell in enumerate(row):
        if isinstance(cell, str) and 'isin' in cell.lower():
            # Пробуем найти в этой же ячейке
            match = re.search(r'ISIN:\s*([A-Z0-9]+)', cell)
            if match:
                return match.group(1)
            # Пробуем взять значение из следующей ячейки
            elif i + 1 < len(row):
                next_cell = row[i + 1]
                if isinstance(next_cell, str) and re.match(r'^[A-Z0-9]{12}$', next_cell.strip()):
                    return next_cell.strip()
    return None

def parse_trades(filepath):
    file_ext = os.path.splitext(filepath)[1].lower()

    rows = read_excel_file(filepath, file_ext)
    stock_trades = []
    bond_trades = []
    current_ticker = None
    current_isin = None
    parsing_trades = False  # флаг, который указывает, что мы начали парсить сделки
    parsing_stocks = False  # флаг для начала парсинга акций
    parsing_bonds = False  # флаг для начала парсинга облигаций

    # Проходим по всем строкам документа
    for idx, row in enumerate(rows):
        row = row[1:]  # Пропускаем первую колонку (пустую)

        # Ищем заголовок "2.1. Сделки:" для начала парсинга
        if not parsing_trades:
            row_str = ' '.join(map(str, row)).lower()
            if "2.1. сделки:" in row_str:
                parsing_trades = True
                continue  # Пропускаем сам заголовок

        # Ищем строки с тикерами (например, ISIN или название компании)
        if parsing_trades:
            if detect_new_ticker(row):
                current_ticker = extract_ticker(row)
                current_isin = extract_isin(row)
                continue

            # Ищем строку, указывающую на начало акций
            if not parsing_stocks and any("акция" in str(cell).lower() for cell in row if cell):
                parsing_stocks = True
                parsing_bonds = False
                continue  # Пропускаем заголовок для акций

            # Ищем строку, указывающую на начало облигаций
            elif not parsing_bonds and any("облигация" in str(cell).lower() for cell in row if cell):
                parsing_bonds = True
                parsing_stocks = False
                continue  # Пропускаем заголовок для облигаций

            # Ищем строку, указывающую на начало АДР
            elif not parsing_stocks and any("адр" in str(cell).lower() for cell in row if cell):
                # Если это АДР, начинаем парсить как акции
                parsing_stocks = True
                parsing_bonds = False
                continue  # Пропускаем заголовок для АДР

            # Прерываем парсинг, если встречаем другие разделы
            if any(keyword in str(cell).lower() for keyword in ["заем", "овернайт", "цб"] for cell in row if cell):
                break  # Прерываем парсинг

            # Обрабатываем строки с данными сделок
            if is_valid_trade_row(row):
                # Парсим акции (в том числе и АДР)
                if parsing_stocks:
                    stock_trade = parse_trade(row, "stock", current_ticker, current_isin)
                    stock_trades.append(stock_trade)
                # Парсим облигации
                elif parsing_bonds:
                    bond_trade = parse_trade(row, "bond", current_ticker)
                    bond_trades.append(bond_trade)

    return {
        "stocks": stock_trades,
        "bonds": bond_trades
    }


def detect_new_ticker(row):
    """Проверка на строки с тикером (например, ISIN)"""
    return any('isin' in str(cell).lower() for cell in row if cell)

def read_excel_file(filepath, file_ext):
    """Чтение данных из Excel-файла"""
    if file_ext == '.xlsx':
        workbook = openpyxl.load_workbook(filepath, data_only=True)
        sheet = workbook.active
        return list(sheet.iter_rows(values_only=True))
    elif file_ext == '.xls':
        workbook = xlrd.open_workbook(filepath)
        sheet = workbook.sheet_by_index(0)
        return [sheet.row_values(i) for i in range(sheet.nrows)]
    else:
        raise ValueError('Неподдерживаемый формат файла')

def extract_ticker(row):
    """Извлечение тикера"""
    if row and isinstance(row[0], str):
        return row[0].split()[0].strip()
    return None

def is_valid_trade_row(row):
    """Проверка на валидность строки с операцией"""
    # Пропускаем строки, которые являются пустыми или начинаются с "итого"
    if not row or not row[0] or isinstance(row[0], str) and row[0].lower().startswith('итого'):
        return False
    # Проверяем, есть ли данные, которые могут быть сделкой (например, цифры, даты)
    return any(isinstance(cell, (int, float)) for cell in row)

def parse_trade(row: List[Any], trade_type: str, ticker: str, isin: Optional[str] = None) -> Dict[str, Any]:
    """Общий парсер для сделок с акциями и облигациями"""
    is_buy = bool(row[3])
    price = row[4] if is_buy else row[7 if trade_type == "stock" else 8]
    quantity = row[3] if is_buy else row[6 if trade_type == "stock" else 7]
    payment = row[5] if is_buy else row[8 if trade_type == "stock" else 9]
    currency = row[10 if trade_type == "stock" else 11]
    comment = str(row[17 if trade_type == "stock" else 18]).strip() if len(row) > (17 if trade_type == "stock" else 18) else ""
    operation_id = str(row[1]).strip() if row[1] else None
    date = f"{row[11]} {row[12]}" if trade_type == "stock" and isinstance(row[11], str) and isinstance(row[12], str) else row[13]

    return {
        "date": date,
        "operation_type": "buy" if is_buy else "sell",
        "payment_sum": payment,
        "currency": currency,
        "ticker": ticker,
        "isin": isin if trade_type == "stock" else ticker,
        "price": price,
        "quantity": quantity,
        "aci": "" if trade_type == "stock" else (row[6] or row[10]),
        "comment": comment,
        "operation_id": operation_id,
    }

# Пример использования
filepath = "2.xls"  # указываете путь к вашему файлу
result = parse_trades(filepath)
print(json.dumps(result, ensure_ascii=False, indent=2))
