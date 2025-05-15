from utils import is_nonzero

#  Валидные операции, которые обрабатываются
VALID_OPERATIONS = {
    "Вознаграждение компании",
    "Дивиденды",
    "НДФЛ",
    "Погашение купона",
    "Погашение облигации",
    "Приход ДС",
    "Проценты по займам \"овернайт\"",
    "Проценты по займам \"овернайт ЦБ\"",
    "Частичное погашение облигации",
    "Вывод ДС",
}

#  Операции, которые нужно игнорировать
SKIP_OPERATIONS = {
    "Внебиржевая сделка FX (22*)",
    "Займы \"овернайт\"",
    "НКД от операций",
    "Покупка/Продажа",
    "Покупка/Продажа (репо)",
    "Переводы между площадками",
}

#  Маппинг строковых названий операций на типы
OPERATION_TYPE_MAP = {
    "Дивиденды": "dividend",
    "Погашение купона": "coupon",
    "Погашение облигации": "repayment",
    "Приход ДС": "deposit",
    "Частичное погашение облигации": "amortization",
    "Вывод ДС": "withdrawal",
}

#  Обработка операций, тип которых зависит от контекста (доход/расход)
SPECIAL_OPERATION_HANDLERS = {
    'Проценты по займам "овернайт"': lambda i, e: "other_income" if is_nonzero(i) else "other_expense",
    'Проценты по займам "овернайт ЦБ"': lambda i, e: "other_income" if is_nonzero(i) else "other_expense",
    "Вознаграждение компании": lambda i, e: "commission_refund" if is_nonzero(i) else "commission",
    "НДФЛ": lambda i, e: "refund" if is_nonzero(i) else "withholding",
}

#  Индексы и флаги для каждого типа сделок
TRADE_TYPE_CONFIG = {
    "stock": {
        "is_stock": True,
        "is_currency": False,
        "indexes": {
            "buy": {
                "price": 4, "quantity": 3, "payment": 5,
                "date": 11, "time": 12, "currency": 10, "comment": 17
            },
            "sale": {
                "price": 7, "quantity": 6, "payment": 8,
                "date": 11, "time": 12, "currency": 10, "comment": 17
            }
        }
    },
    "bond": {
        "is_stock": False,
        "is_currency": False,
        "indexes": {
            "buy": {
                "price": 4, "quantity": 3, "payment": 5,
                "date": 13, "time": None, "currency": 11, "comment": 18, "aci": 6
            },
            "sale": {
                "price": 8, "quantity": 7, "payment": 9,
                "date": 13, "time": None, "currency": 11, "comment": 18, "aci": 10
            }
        }
    },
    "currency": {
        "is_stock": False,
        "is_currency": True,
        "indexes": {
            "buy": {
                "price": 3, "quantity": 4, "payment": 5,
                "date": 9, "time": 10
            },
            "sale": {
                "price": 6, "quantity": 7, "payment": 8,
                "date": 9, "time": 10
            }
        }
    }
}

#  Нормализация валют
CURRENCY_DICT = {
    "AED": "AED", "AMD": "AMD", "BYN": "BYN", "CHF": "CHF", "CNY": "CNY",
    "EUR": "EUR", "GBP": "GBP", "HKD": "HKD", "JPY": "JPY", "KGS": "KGS",
    "KZT": "KZT", "NOK": "NOK", "RUB": "RUB", "РУБЛЬ": "RUB", "Рубль": "RUB",
    "SEK": "SEK", "TJS": "TJS", "TRY": "TRY", "USD": "USD", "UZS": "UZS",
    "XAG": "XAG", "XAU": "XAU", "ZAR": "ZAR"
}
