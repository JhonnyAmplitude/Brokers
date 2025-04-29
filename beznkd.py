import json
import openpyxl
import xlrd
import os

def parse_trades(filepath):
    file_ext = os.path.splitext(filepath)[1].lower()

    rows = read_excel_file(filepath, file_ext)
    stock_trades = []
    bond_trades = []
    current_ticker = None
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
                continue  # Пропускаем строку с тикером

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

            # Останавливаем парсинг, если встречаем другие разделы
            if any("заем" in str(cell).lower() for cell in row if cell) or any("овернайт" in str(cell).lower() for cell in row if cell):
                break  # Прерываем парсинг

            # Обрабатываем строки с данными сделок
            if is_valid_trade_row(row):
                # Парсим акции
                if parsing_stocks:
                    stock_trade = parse_trade(row, "stock", current_ticker)
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
    # Проверка на строки с тикером (например, ISIN)
    return any('isin' in str(cell).lower() for cell in row if cell)

def extract_ticker(row):
    # Извлекаем тикер из строки (например, из ISIN)
    for cell in row:
        if cell and isinstance(cell, str):
            if 'isin' in cell.lower():
                # Получаем тикер (обычно это 12 символов после "RU" для России)
                return cell.split()[-1]  # Мы предполагаем, что тикер после "ISIN:"
    return None

def is_valid_trade_row(row):
    # Дополнительная проверка на валидность строки для сделок
    return bool(row) and any(cell for cell in row)

def detect_new_ticker(row):
    # Проверка на строки с тикером (например, ISIN)
    return any('isin' in str(cell).lower() for cell in row if cell)

def extract_ticker(row):
    # Извлекаем тикер из строки (например, из ISIN)
    for cell in row:
        if cell and isinstance(cell, str):
            if 'isin' in cell.lower():
                # Получаем тикер (обычно это 12 символов после "RU" для России)
                return cell.split()[-1]  # Мы предполагаем, что тикер после "ISIN:"
    return None

def is_valid_trade_row(row):
    # Дополнительная проверка на валидность строки для сделок
    return bool(row) and any(cell for cell in row)


def detect_new_ticker(row):
    # Проверка на строки с тикером (например, ISIN)
    return any('isin' in str(cell).lower() for cell in row if cell)

def extract_ticker(row):
    # Извлекаем тикер из строки (например, из ISIN)
    for cell in row:
        if cell and isinstance(cell, str):
            if 'isin' in cell.lower():
                # Получаем тикер (обычно это 12 символов после "RU" для России)
                return cell.split()[-1]  # Мы предполагаем, что тикер после "ISIN:"
    return None


def detect_new_ticker(row):
    # Проверка на строки с тикером (например, ISIN)
    return any('isin' in str(cell).lower() for cell in row if cell)

def extract_ticker(row):
    # Извлекаем тикер из строки (например, из ISIN)
    for cell in row:
        if cell and isinstance(cell, str):
            if 'isin' in cell.lower():
                # Получаем тикер (обычно это 12 символов после "RU" для России)
                return cell.split()[-1]  # Мы предполагаем, что тикер после "ISIN:"
    return None


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

def detect_new_ticker(row):
    """Определение нового тикера"""
    if row and isinstance(row[0], str):
        return 'ISIN' in row[0] or 'ISIN' in ''.join(map(str, row))
    return False

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

def parse_trade(row, trade_type, ticker):
    """Общий парсер для сделок с акциями и облигациями"""
    if trade_type == "stock":
        # Обрабатываем сделки с акциями
        trade_data = {
            "trade_date": row[0],
            "ticker": ticker,
            "operation": "buy" if row[3] else "sell",
            "quantity": row[3] if row[3] else row[6],
            "price": row[4] if row[3] else row[7],
            "amount": row[5] if row[3] else row[8],
            "currency": row[9]
        }
    elif trade_type == "bond":
        # Обрабатываем сделки с облигациями
        trade_data = {
            "trade_date": row[0],
            "ticker": ticker,
            "operation": "buy" if row[3] else "sell",
            "quantity": row[3] if row[3] else row[7],
            "price": row[4] if row[3] else row[8],
            "amount": row[5] if row[3] else row[9],
            "currency": row[11]
        }

    return trade_data


filepath = "1.xls"  # здесь указываешь путь к файлу
result = parse_trades(filepath)
print(json.dumps(result, ensure_ascii=False, indent=2))
