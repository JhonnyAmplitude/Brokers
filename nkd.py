import json
import openpyxl
import xlrd
import os

from datetime import datetime


def read_excel_file(filepath, file_ext):
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
    if row and isinstance(row[0], str):
        return 'ISIN' in row[0].upper() or 'ISIN' in ''.join(map(str, row)).upper()
    return False

def extract_ticker(row):
    if row and isinstance(row[0], str):
        return row[0].split()[-1].strip()
    return None


def normalize_trade_datetime(date_str, time_str=None):
    """Преобразование даты и времени в формат ISO"""
    if not date_str:
        return None

    try:
        # Если дата в формате '14.07.23'
        if len(date_str.strip()) == 8:
            date_obj = datetime.strptime(date_str.strip(), "%d.%m.%y")
        else:  # если вдруг в формате '14.07.2023'
            date_obj = datetime.strptime(date_str.strip(), "%d.%m.%Y")

        if time_str:
            # если есть ещё и время
            time_obj = datetime.strptime(time_str.strip(), "%H:%M:%S").time()
            full_datetime = datetime.combine(date_obj.date(), time_obj)
            return full_datetime.strftime("%Y-%m-%d %H:%M:%S")
        else:
            # только дата
            return date_obj.strftime("%Y-%m-%d 00:00:00")

    except Exception as e:
        print(f"Ошибка нормализации даты: {e}")
        return None

def is_valid_trade_row(row):
    """Проверка на валидность строки с операцией"""
    if not row or not row[0]:
        return False

    if isinstance(row[0], str):
        lower_row = ' '.join(str(cell).lower() for cell in row if cell)
        forbidden_words = ['итого', 'дата', 'время', 'куплено', 'продано', 'цена', 'сумма', 'валюта', 'нкд']
        if any(word in lower_row for word in forbidden_words):
            return False

    return any(isinstance(cell, (int, float)) for cell in row)


def extract_ticker_and_isin(row):
    if row and isinstance(row[0], str):
        parts = row[0].split()
        ticker_candidate = parts[0].strip()
        isin_candidate = None

        for part in parts:
            if len(part) == 12 and part.upper().startswith('RU'):
                isin_candidate = part.strip()
                break

        return ticker_candidate, isin_candidate
    return None, None


def parse_trade(row, trade_type, ticker, isin=None):
    def clean_date(value):
        """Помощник: превратить дату из Excel в строку"""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d")
        if isinstance(value, (int, float)):
            try:
                date_obj = datetime(*xlrd.xldate_as_tuple(value, 0))
                return date_obj.strftime("%Y-%m-%d")
            except Exception:
                pass
        if isinstance(value, str):
            value = value.strip()
            for fmt in ("%d.%m.%Y", "%d.%m.%y"):
                try:
                    date_obj = datetime.strptime(value, fmt)
                    return date_obj.strftime("%Y-%m-%d")
                except ValueError:
                    continue
        return None

    def clean_time(value):
        """Помощник: превратить время из Excel в строку"""
        if value is None:
            return "00:00:00"
        if isinstance(value, datetime):
            return value.strftime("%H:%M:%S")
        if isinstance(value, (int, float)):
            try:
                time_obj = datetime(*xlrd.xldate_as_tuple(value, 0)).time()
                return time_obj.strftime("%H:%M:%S")
            except Exception:
                pass
        if isinstance(value, str):
            value = value.strip()
            for fmt in ("%H:%M:%S", "%H:%M"):
                try:
                    time_obj = datetime.strptime(value, fmt).time()
                    return time_obj.strftime("%H:%M:%S")
                except ValueError:
                    continue
        return "00:00:00"

    def safe_get(lst, idx):
        """Безопасное получение элемента из списка"""
        return lst[idx] if idx < len(lst) else None

    # Определяем операция покупка или продажа
    is_buy = safe_get(row, 3) is not None  # Если есть объем покупки
    operation = "buy" if is_buy else "sell"

    if trade_type == "stock":
        quantity = safe_get(row, 3) if is_buy else safe_get(row, 6)
        price = safe_get(row, 4) if is_buy else safe_get(row, 7)
        amount = safe_get(row, 5) if is_buy else safe_get(row, 8)
        currency = safe_get(row, 9)

        trade_date = clean_date(safe_get(row, 11))
        trade_time = clean_time(safe_get(row, 12))

        trade_datetime = f"{trade_date} {trade_time}" if trade_date else None

        return {
            "ticker": ticker,
            "isin": isin,
            "operation": operation,
            "trade_datetime": trade_datetime,
            "quantity": quantity,
            "price": price,
            "amount": amount,
            "currency": currency,
            "nkd": None  # у акций НКД нет
        }

    elif trade_type == "bond":
        quantity = safe_get(row, 3) if is_buy else safe_get(row, 7)
        price = safe_get(row, 4) if is_buy else safe_get(row, 8)
        amount = safe_get(row, 5) if is_buy else safe_get(row, 9)
        nkd = safe_get(row, 6) if is_buy else safe_get(row, 10)
        currency = safe_get(row, 11)

        trade_date = clean_date(safe_get(row, 13))
        trade_datetime = f"{trade_date} 00:00:00" if trade_date else None

        return {
            "ticker": ticker,
            "isin": ticker,  # у облигаций ISIN == тикеру
            "operation_type": operation,
            "trade_datetime": trade_datetime,
            "quantity": quantity,
            "price": price,
            "payment_sum": amount,
            "currency": currency,
            "aci": nkd
        }


def parse_trades(filepath):
    file_ext = os.path.splitext(filepath)[1].lower()
    rows = read_excel_file(filepath, file_ext)

    stock_trades = []
    bond_trades = []
    current_ticker = None
    current_isin = None
    parsing_trades = False
    parsing_stocks = False
    parsing_bonds = False

    for idx, row in enumerate(rows):
        row = row[1:]  # пропускаем первую пустую колонку

        if not parsing_trades:
            row_str = ' '.join(map(str, row)).lower()
            if "2.1. сделки:" in row_str:
                parsing_trades = True
                continue

        if parsing_trades:
            if detect_new_ticker(row):
                current_ticker, current_isin = extract_ticker_and_isin(row)
                continue

            if not parsing_stocks and any("акция" in str(cell).lower() for cell in row if cell):
                parsing_stocks = True
                parsing_bonds = False
                continue

            elif not parsing_bonds and any("облигация" in str(cell).lower() for cell in row if cell):
                parsing_bonds = True
                parsing_stocks = False
                continue

            if any("заем" in str(cell).lower() for cell in row if cell) or any("овернайт" in str(cell).lower() for cell in row if cell):
                break

            if is_valid_trade_row(row):
                if parsing_stocks:
                    stock_trade = parse_trade(row, "stock", current_ticker, current_isin)
                    stock_trades.append(stock_trade)
                elif parsing_bonds:
                    bond_trade = parse_trade(row, "bond", current_ticker)
                    bond_trades.append(bond_trade)

    stock_trades.sort(key=lambda x: (x["trade_datetime"] is None, x["trade_datetime"]))
    bond_trades.sort(key=lambda x: (x["trade_datetime"] is None, x["trade_datetime"]))

    return {
        "stocks": stock_trades,
        "bonds": bond_trades
    }


filepath = "new_5.xls"  # сюда путь к файлу
result = parse_trades(filepath)
print(json.dumps(result, ensure_ascii=False, indent=2))
