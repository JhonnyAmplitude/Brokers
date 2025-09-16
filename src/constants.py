# src/constants.py
from typing import Any, Callable, Dict, Optional
import re

# ---- Helpers ----
_NBSP_PAT = re.compile(r"[\u00A0\u202F]")

def norm_str(s: Any) -> str:
    """
    Нормализует строку для сравнения:
    - None -> ""
    - заменяет NBSP на обычные пробелы
    - сводит множественные пробелы к одному
    - strip() и lower()
    """
    if s is None:
        return ""
    st = str(s)
    st = _NBSP_PAT.sub(" ", st)
    # unify whitespace
    st = re.sub(r"\s+", " ", st)
    return st.strip().lower()


def _norm_key(s: str) -> str:
    """Короткая удобная обёртка для нормализации ключей в словарях."""
    return norm_str(s)


def to_float_safe(v: Any) -> Optional[float]:
    """Пытается превратить в float, возвращает None при ошибке."""
    if v is None or v == "":
        return None
    try:
        return float(str(v).replace(",", ".").replace(" ", ""))
    except Exception:
        return None


# ---- Sign helper ----
def get_sign(value: Any) -> int:
    """
    Возвращает:
      -1 если число отрицательное;
       0 если равно нулю или не удалось распарсить;
      +1 если положительное.
    """
    try:
        v = float(str(value).replace(",", ".").replace(" ", ""))
    except Exception:
        return 0
    if v > 0:
        return 1
    elif v < 0:
        return -1
    return 0


# ---- Оригинальные множества / словари (читаемые человеком) ----
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


# ---- Нормализованные наборы (для быстрого сравнения в парсере) ----
NORMALIZED_VALID_OPERATIONS = {norm_str(x) for x in VALID_OPERATIONS}
NORMALIZED_SKIP_OPERATIONS = {norm_str(x) for x in SKIP_OPERATIONS}


# ---- Маппинг строковых названий операций -> canonical type
# Ключи — НИЖНИЙ РЕГИСТР и нормализованные; так удобнее сравнивать через "in" / equals
OPERATION_TYPE_MAP: Dict[str, str] = {
    _norm_key("Дивиденды"): "dividend",
    _norm_key("Купонный доход"): "coupon",
    _norm_key("Куп. дох."): "coupon",
    _norm_key("Погашение ценных бумаг"): "repayment",
    _norm_key("Погашение облигаций"): "repayment",
    _norm_key("Зачисление денежных средств"): "deposit",
    _norm_key("Перевод денежных средств"): "transfer",
    _norm_key("Частичное погашение облигации"): "amortization",
    _norm_key("Списание денежных средств"): "withdrawal",
    _norm_key("Вознаграждение Брокера"): "commission",
    _norm_key("Вознаграждение сторонних организаций"): "commission",
    # можно дополнять ключи-синонимы, например:
    _norm_key("приход дс"): "deposit",
    _norm_key("вывод дс"): "withdrawal",
}


# ---- SPECIAL handlers: операции, где значение зависит от знака/контекста ----
# Ключи — нормализованные; значения — callables (amount, entry) -> canonical_type
# Примечание: handler получает сумму (raw или float) и словарь/entry строки (если нужно)
SPECIAL_OPERATION_HANDLERS: Dict[str, Callable[[Any, dict], str]] = {
    _norm_key("Вознаграждение компании"): lambda amount, entry: "commission_refund" if get_sign(amount) > 0 else "commission",
    _norm_key("НДФЛ"): lambda amount, entry: "refund" if get_sign(amount) > 0 else "withholding",
    # пример (раскомментируйте, если хотите обрабатывать переводы через хендлер):
    # _norm_key("Перевод денежных средств"): lambda amount, entry: "deposit" if get_sign(amount) > 0 else "withdrawal",
}


def resolve_special_operation(op_raw: Any, amount: Any, entry: Optional[dict] = None) -> Optional[str]:
    """
    Попытка применить SPECIAL_OPERATION_HANDLERS:
    - делает нормализацию op_raw и ищет handler для любого ключа, который является подстрокой op_raw_norm
    - возвращает canonical operation string или None если не применимо
    """
    if op_raw is None:
        return None
    op_norm = norm_str(op_raw)
    for key, handler in SPECIAL_OPERATION_HANDLERS.items():
        # используем substring match — это надёжнее для вариативных формулировок
        if key in op_norm:
            try:
                return handler(amount, entry or {})
            except Exception:
                # не кидаем ошибку тут — пусть парсер логирует исключение при необходимости
                return None
    return None


# ---- Валюты ----
CURRENCY_DICT = {
    "AED": "AED", "AMD": "AMD", "BYN": "BYN", "CHF": "CHF", "CNY": "CNY",
    "EUR": "EUR", "GBP": "GBP", "HKD": "HKD", "JPY": "JPY", "KGS": "KGS",
    "KZT": "KZT", "NOK": "NOK", "RUB": "RUB", "RUR": "RUB",
    "РУБЛЬ": "RUB", "Рубль": "RUB",
    "SEK": "SEK", "TJS": "TJS", "TRY": "TRY", "USD": "USD", "UZS": "UZS",
    "XAG": "XAG", "XAU": "XAU", "ZAR": "ZAR"
}
