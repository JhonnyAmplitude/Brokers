from typing import Any

#  Валидные операции, которые обрабатываются
VALID_OPERATIONS = {
    "Вознаграждение компании",
    "Вознаграждение Брокера",
    "Вознаграждение сторонних организаций",
    "Дивиденды",
    "Купонный доход",
    "Куп. дох.",
    "Погашение облигации",
    "Погашение ценных бумаг",
    "Приход ДС",
    "Зачисление денежных средств",
    "Перевод денежных средств",
    "Проценты по займам \"овернайт\"",
    "Проценты по займам \"овернайт ЦБ\"",
    "Частичное погашение облигации",
    "Вывод ДС",
}

#  Операции, которые нужно игнорировать (служебные / балансные / агрегаты)
SKIP_OPERATIONS = {
    "Внебиржевая сделка FX (22*)",
    "Займы \"овернайт\"",
    "НКД от операций",
    "Покупка/Продажа",
    "Покупка/Продажа (репо)",
    "Переводы между площадками",
    "Перераспределение дохода между субсчетами / торговыми площадками",
    "Сальдо расчётов по сделкам с ценными бумагами",
    "Иные неторговые операции",
}

#  Маппинг строковых названий операций на типы
OPERATION_TYPE_MAP = {
    "Дивиденды": "dividend",
    "Купонный доход": "coupon",
    "Куп. дох.": "coupon",
    "Погашение ценных бумаг": "repayment",
    "Погашение облигаций": "repayment",
    "Зачисление денежных средств": "deposit",
    "Перевод денежных средств": "transfer",
    "Частичное погашение облигации": "amortization",
    "Списание денежных средств": "withdrawal",
    "Вознаграждение Брокера": "commission",
    "Вознаграждение сторонних организаций": "commission",
}

#  Обработка операций, тип которых зависит от контекста (доход/расход)
def _sign_type(val: Any, income_label: str = "other_income", expense_label: str = "other_expense"):
    """
    Вспомогательная: определяет направление по знаку суммы.
    val может быть str/float/None.
    """
    try:
        v = float(str(val).replace(",", ".").replace(" ", ""))
        if v > 0:
            return income_label
        if v < 0:
            return expense_label
    except Exception:
        pass
    return expense_label if val else expense_label

SPECIAL_OPERATION_HANDLERS = {
    "Вознаграждение компании": lambda i, e: "commission_refund" if get_sign(i) > 0 else "commission",
    "НДФЛ": lambda i, e: "refund" if get_sign(i) > 0 else "withholding",
    # "Перевод денежных средств": lambda i, e: "deposit" if get_sign(i) > 0 else "withdrawal",
}

CURRENCY_DICT = {
    "AED": "AED", "AMD": "AMD", "BYN": "BYN", "CHF": "CHF", "CNY": "CNY",
    "EUR": "EUR", "GBP": "GBP", "HKD": "HKD", "JPY": "JPY", "KGS": "KGS",
    "KZT": "KZT", "NOK": "NOK", "RUB": "RUB", "RUR": "RUB",
    "РУБЛЬ": "RUB", "Рубль": "RUB",
    "SEK": "SEK", "TJS": "TJS", "TRY": "TRY", "USD": "USD", "UZS": "UZS",
    "XAG": "XAG", "XAU": "XAU", "ZAR": "ZAR"
}


def get_sign(value: float | int) -> int:
    """
    Возвращает:
    -1 если число отрицательное
     0 если равно нулю или не удалось распарсить
    +1 если положительное
    """
    try:
        v = float(value)
    except Exception:
        return 0
    if v > 0:
        return 1
    elif v < 0:
        return -1
    return 0

