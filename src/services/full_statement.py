# replace your existing parse_full_statement with this

from src.parsers.header import parse_header
from src.parsers.fin_operations import parse_fin_operations
from src.parsers.stocks_bonds import parse_stock_bond_trades
from datetime import datetime
from typing import Any

def _make_fingerprint(op: Any) -> tuple:
    dt = getattr(op, "date", None)
    if isinstance(dt, datetime):
        dstr = dt.isoformat()
    else:
        dstr = str(dt)
    t = getattr(op, "operation_type", "") or ""
    s = getattr(op, "payment_sum", 0) or 0
    try:
        s_norm = round(float(s), 6)
    except Exception:
        s_norm = str(s)
    ticker = (getattr(op, "ticker", "") or "").strip()
    isin = (getattr(op, "isin", "") or "").strip()
    return ("fp", dstr, t, s_norm, ticker, isin)


def parse_full_statement(file_path: str) -> dict:
    """
    Возвращает dict с header + operations, и добавляет в header поля meta:
      - meta.fin_ops_raw_count: сколько строк вернул parse_fin_operations
      - meta.trade_ops_raw_count: сколько строк вернул parse_stock_bond_trades
      - meta.total_ops_count: сколько операций в итоговом объединённом и дедупленном списке
    """
    header = parse_header(file_path)

    # парсим отдельные секции
    fin_ops = parse_fin_operations(file_path)          # List[OperationDTO]
    trade_ops = parse_stock_bond_trades(file_path)     # List[OperationDTO]

    # объединяем и дедуплим (по operation_id если есть, иначе по fingerprint)
    combined = list(fin_ops) + list(trade_ops)

    seen = set()
    ordered_ops = []
    for op in combined:
        op_id = getattr(op, "operation_id", None)
        op_id = str(op_id).strip() if op_id is not None else ""
        if op_id:
            key = ("id", op_id)
        else:
            key = _make_fingerprint(op)

        if key in seen:
            continue
        seen.add(key)
        ordered_ops.append(op)

    # собираем результат
    result = {
        **header,
        "operations": [o.to_dict() for o in ordered_ops]
    }

    # добавляем мета-информацию о подсчётах (удобно как для логов, так и в output json)
    result_meta = {
        "fin_ops_raw_count": len(fin_ops),
        "trade_ops_raw_count": len(trade_ops),
        "total_ops_count": len(ordered_ops),
    }
    # вложим в header.meta (если header — dict)
    if isinstance(header, dict):
        header_meta = header.get("meta", {})
        header_meta.update(result_meta)
        result["header"] = header  # (уже included via **header) — но настаём meta в header
        result["meta"] = result_meta
    else:
        result["meta"] = result_meta

    return result
