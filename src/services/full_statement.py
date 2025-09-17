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

    fin_ops, fin_stats = parse_fin_operations(file_path)
    trade_ops, trade_stats = parse_stock_bond_trades(file_path)

    unknown_fin_ops = fin_stats.get("unrecognized_names", [])

    return {
        **header,
        "operations": [*map(lambda op: op.to_dict(), fin_ops + trade_ops)],
        "meta": {
            "fin_ops_raw_count": len(fin_ops),
            "trade_ops_raw_count": len(trade_ops),
            "total_ops_count": len(fin_ops) + len(trade_ops),
            "fin_ops_stats": fin_stats,
            "trade_ops_stats": trade_stats,
            "after_dedupe_from_fin": len(fin_ops),
            "after_dedupe_from_trade": len(trade_ops),
            "unknown_fin_ops": unknown_fin_ops,
        },
    }

