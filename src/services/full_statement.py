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
    header = parse_header(file_path)

    # парсим отдельные секции — теперь функции возвращают (ops, stats)
    fin_ops, fin_stats = parse_fin_operations(file_path)
    trade_ops, trade_stats = parse_stock_bond_trades(file_path)

    # объединяем и дедуплим (по operation_id если есть, иначе по fingerprint)
    combined = list(fin_ops) + list(trade_ops)

    seen = set()
    ordered_ops = []
    origin_map = {}  # track origin counts after dedupe
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
        # track origin: was this op from fin_ops or trade_ops
        origin = "fin" if op in fin_ops else "trade"
        origin_map[origin] = origin_map.get(origin, 0) + 1
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

        # детальная статистика от парсеров
        "fin_ops_stats": fin_stats or {},
        "trade_ops_stats": trade_stats or {},

        # сколько после дедупа пришло из каждой секции
        "after_dedupe_from_fin": origin_map.get("fin", 0),
        "after_dedupe_from_trade": origin_map.get("trade", 0),
    }

    if isinstance(header, dict):
        header_meta = header.get("meta", {})
        header_meta.update(result_meta)
        result["header"] = header
        result["meta"] = result_meta
    else:
        result["meta"] = result_meta

    return result
