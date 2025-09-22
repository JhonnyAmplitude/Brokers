# src/services/full_statement.py
from src.parsers.header import parse_header
from src.parsers.fin_operations import parse_fin_operations
from src.parsers.stocks_bonds import parse_stock_bond_trades
from datetime import datetime
from typing import Any, Dict, List


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


def parse_full_statement(file_path: str) -> Dict:
    """
    Парсит заголовок, финансовые операции и сделки с ценными бумагами.
    Возвращает структуру:
    {
      ...header...,  # account_id, date_start, date_end, ...
      "operations": [...],
      "meta": {
          "fin_ops_raw_count": int,
          "trade_ops_raw_count": int,
          "total_operations": int,
          "fin_stats": {...},      # raw stats from fin parser
          "trade_stats": {...},    # raw stats from trades parser
          "unknown_fin_ops": [...],# список нераспознанных названий
      }
    }
    """
    header = parse_header(file_path)

    fin_ops, fin_stats = parse_fin_operations(file_path)
    trade_ops, trade_stats = parse_stock_bond_trades(file_path)

    fin_stats = fin_stats or {}
    trade_stats = trade_stats or {}

    fin_count = fin_stats.get("parsed", len(fin_ops))
    trade_count = trade_stats.get("parsed", len(trade_ops))

    meta = {
        "fin_ops_raw_count": fin_count,
        "trade_ops_raw_count": trade_count,
        "total_operations": len(fin_ops) + len(trade_ops),
        "fin_stats": fin_stats,
        "trade_stats": trade_stats,
        "unknown_fin_ops": fin_stats.get("unrecognized_names", []),
    }

    operations = [*map(lambda o: o.to_dict(), fin_ops + trade_ops)]

    return {
        **header,
        "operations": operations,
        "meta": meta,
    }
