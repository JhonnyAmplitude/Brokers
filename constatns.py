from typing import Any

VALID_OPERATIONS = {
    "Проценты по займам \"овернайт ЦБ\"", "Приход ДС", "Проценты по займам \"овернайт\"",
    "Погашение купона", "Вознаграждение компании",
    "Переводы между площадками", "Дивиденды",
    "Частичное погашение облигации", "Погашение облигации", "Вывод ДС", "НДФЛ",
}

SKIP_OPERATIONS = {
    'Займы "овернайт"', "Покупка/Продажа", "НКД от операций", "Покупка/Продажа (репо)",
    'Переводы между площадками', 'Внебиржевая сделка FX (22*)'
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
    "Вознаграждение компании": lambda i, e: "commission_refund" if is_nonzero(i) else "commission",
    "НДФЛ": lambda i, e: "refund" if is_nonzero(i) else "withholding",
}

CURRENCY_DICT = {
    "SEK": "SEK", "NOK": "NOK", "AED": "AED", "XAG": "XAG", "ZAR": "ZAR",
    "TRY": "TRY", "XAU": "XAU", "HKD": "HKD", "TJS": "TJS", "UZS": "UZS",
    "KGS": "KGS", "KZT": "KZT", "JPY": "JPY", "AMD": "AMD", "РУБЛЬ": "RUB", "Рубль":"RUB",
    "USD": "USD", "EUR": "EUR", "BYN": "BYN", "GBP": "GBP", "CHF": "CHF", "CNY": "CNY"
}

def is_nonzero(value: Any) -> bool:
    """
    Проверка на значение, отличное от нуля.
    """
    try:
        return float(str(value).replace(",", ".").replace(" ", "")) != 0
    except (ValueError, TypeError):
        return False