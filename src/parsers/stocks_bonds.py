# src/parsers/stock_bond_trades.py
from __future__ import annotations
from typing import List, Any, Optional, Tuple, Union, Dict
import re
import pandas as pd
from datetime import datetime as _dt
from datetime import datetime

from src.OperationDTO import OperationDTO
from src.utils import logger, to_num_safe, to_int_safe
import src.constants

# Patterns
ISIN_RE = re.compile(r"\b[A-Za-z]{2}[A-Za-z0-9]{9}\d\b", re.IGNORECASE)

# Keywords to detect header columns (we only map needed columns)
HEADER_KEYWORDS_TRADES: Dict[str, List[str]] = {
    "instrument": ["наименование ценной бумаги", "isin", "регистрац", "№ гос. регистрац"],
    "datetime": ["дата и время", "дата и время заключения", "дата", "время"],
    "type": ["вид сделки", "вид", "сделк"],
    "quantity": ["колич", "шт"],
    "price": ["цена", "% для облигац", "% для облигаций", "процент"],
    "currency_calc": ["валюта расчет", "валюта расчетов", "валюта"],
    "sum": ["сумма сделки", "сумма сделки в валюте расчетов", "сумма"],
    "aci": ["нкд"],
    "commission": ["комиссия банка", "комиссия"],
    "order_no": ["№ заявки", "заявка"],
    "trade_no": ["№ сделки", "номер сделки", "№сделки"],
    "comment": ["коммент", "комментарий"],
}


def find_trades_block_start(df: pd.DataFrame) -> Optional[int]:
    needle = src.constants.norm_str("Заключенные в отчетном периоде сделки с ценными бумагами")
    for i, row in df.iterrows():
        text = " ".join(str(c) for c in row if str(c).strip())
        if not text:
            continue
        if needle in src.constants.norm_str(text):
            return i + 1
    return None


def find_trades_header_row(df: pd.DataFrame, start_idx: int, lookahead: int = 8) -> Optional[int]:
    n = len(df)
    end = min(n, start_idx + lookahead + 1)
    for i in range(start_idx, end):
        row = df.iloc[i]
        joined = " ".join(str(c).strip().lower() for c in row if str(c).strip())
        if not joined:
            continue
        if "наименование ценной бумаги" in joined or ("наименование" in joined and "ценн" in joined):
            if "дата" in joined or "дата и время" in joined or any("дата" in str(c).lower() for c in row):
                return i
    for i in range(start_idx, min(n, start_idx + 50)):
        row = df.iloc[i]
        joined = " ".join(str(c).strip().lower() for c in row if str(c).strip())
        if "наименование" in joined and "дата" in joined:
            return i
    return None


def _build_combined_header(df: pd.DataFrame, header_idx: int, max_rows: int = 3) -> List[str]:
    """
    Собирает комбинированные заголовки по колонкам, объединяя up to max_rows строк,
    начиная с header_idx. Возвращает список строк длиной = число колонок.
    Это делает парсинг устойчивым к многострочным заголовкам.
    """
    ncols = df.shape[1]
    rows = []
    end = min(len(df), header_idx + max_rows)
    for r in range(header_idx, end):
        rows.append(df.iloc[r].astype(str).fillna("").tolist())

    combined = []
    for c in range(ncols):
        parts = []
        for r in range(len(rows)):
            cell = rows[r][c] if c < len(rows[r]) else ""
            cell = str(cell).strip()
            if cell:
                parts.append(cell)
        combined.append(" ".join(parts).strip().lower())
    return combined


def map_trades_header_indices(header_row: Union[pd.Series, List[str]]) -> Dict[str, int]:
    """
    Универсальная версия: header_row может быть Series (одна строка) или List[str]
    (combination of multiple header lines per column). Возвращает маппинг ключ->index.
    """
    cols: Dict[str, int] = {}
    # если передана серия — превратим в list
    if isinstance(header_row, pd.Series):
        headers = [str(c).strip() for c in header_row.tolist()]
    else:
        headers = [str(h).strip() for h in header_row]

    for idx, cell in enumerate(headers):
        if not cell:
            continue
        low = src.constants.norm_str(cell)
        for key, keywords in HEADER_KEYWORDS_TRADES.items():
            if key in cols:
                continue
            for kw in keywords:
                if src.constants.norm_str(kw) in low:
                    cols[key] = idx
                    break
            if key in cols:
                break
    return cols


def parse_instrument_cell(cell: Any) -> Tuple[str, str, str]:
    """
    Из поля 'Наименование ценной бумаги, № гос. Регистрации, ISIN'
    возвращаем (ticker, isin, reg_number).
    reg_number может быть строкой вида '1-01-20510-F' и т.п.
    """
    s = "" if cell is None else str(cell).strip()
    if not s:
        return "", "", ""
    # try to find ISIN
    m = ISIN_RE.search(s)
    isin = m.group(0).upper() if m else ""
    # split by common separators and try to get reg_number (usually second token)
    parts = [p.strip() for p in re.split(r"[,\t;/]+", s) if p.strip()]
    reg_number = ""
    ticker = ""
    if len(parts) >= 2:
        # heuristic: parts often like [Name, reg_number, ISIN]
        # try to find which part is reg_number (non-ISIN, contains digits and dash)
        for p in parts:
            p_clean = p.strip()
            if p_clean.upper() == isin:
                continue
            if ISIN_RE.search(p_clean):
                continue
            # if looks like reg_number (contains digit and dash)
            if re.search(r"[0-9]", p_clean) and "-" in p_clean:
                reg_number = p_clean
                continue
        # ticker heuristic: take first token of first part if short
        first = parts[0]
        tok0 = first.split()[0]
        if 1 <= len(tok0) <= 8:
            ticker = tok0
        else:
            # fallback - try to find short token among parts
            for p in parts:
                candidate = p.split()[0]
                if re.fullmatch(r"[A-Za-z0-9\-\.]{1,8}", candidate):
                    ticker = candidate
                    break
    else:
        # less structured: try pick a short token as ticker
        tokens = re.split(r"[\s,;/]+", s)
        for t in tokens:
            if re.fullmatch(r"[A-Za-z0-9\-\.]{1,8}", t):
                ticker = t
                break
    return ticker, isin, reg_number


def parse_trades_table(df: pd.DataFrame, header_idx: int, cols: Dict[str, int]) -> tuple[List[OperationDTO], dict]:
    results: List[OperationDTO] = []
    curr_ticker, curr_isin, curr_reg = "", "", ""

    total_rows = 0
    parsed_rows = 0
    skipped_empty = 0
    skipped_no_date = 0
    skipped_no_qty = 0
    skipped_no_type = 0
    skipped_itogo = 0

    # iterate rows after header
    for r_idx in range(header_idx + 1, len(df)):
        total_rows += 1
        row = df.iloc[r_idx]
        cells = list(row)
        text_row = " ".join(str(c).strip() for c in cells if str(c).strip()).lower()

        # --- стоп на новом блоке ---
        if "завершенные" in text_row and "сделки с ценными бумагами" in text_row:
            logger.debug("Reached closing trades block at row %s: %s", r_idx, text_row)
            break

        # --- пропускаем пустые строки ---
        if all((c is None or (isinstance(c, str) and not c.strip())) for c in cells):
            skipped_empty += 1
            continue

        # --- игнорируем строки 'Итого' ---
        if any(isinstance(c, str) and src.constants.norm_str(c).startswith("итого") for c in cells):
            skipped_itogo += 1
            continue

        # instrument: если есть — обновляем, если пусто — используем прошлое
        inst_idx = cols.get("instrument")
        if inst_idx is not None and inst_idx < len(cells):
            inst_cell = cells[inst_idx]
            if inst_cell is not None and str(inst_cell).strip():
                tck, isin, regno = parse_instrument_cell(inst_cell)
                if tck:
                    curr_ticker = tck
                if isin:
                    curr_isin = isin
                if regno:
                    curr_reg = regno

        # datetime
        date_val = None
        dt_idx = cols.get("datetime")
        if dt_idx is not None and dt_idx < len(cells):
            raw_dt = cells[dt_idx]
            if raw_dt and str(raw_dt).strip():
                s = str(raw_dt).strip()
                s_fixed = s.replace(",", ".").replace("\u00A0", " ")
                _try_formats = ("%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M", "%Y-%m-%d %H:%M:%S", "%d.%m.%Y")
                for _fmt in _try_formats:
                    try:
                        date_val = _dt.strptime(s_fixed, _fmt)
                        break
                    except Exception:
                        continue
                if date_val is None:
                    pd_dt = pd.to_datetime(s_fixed, dayfirst=True, errors="coerce")
                    if pd_dt is not None and not pd.isna(pd_dt):
                        date_val = pd_dt.to_pydatetime()
        if date_val is None:
            skipped_no_date += 1
            continue

        # type
        op_type_raw = ""
        t_idx = cols.get("type")
        if t_idx is not None and t_idx < len(cells):
            op_type_raw = str(cells[t_idx]).strip().lower()
        if "покуп" in op_type_raw:
            op = "buy"
        elif "продаж" in op_type_raw or "продажа" in op_type_raw or "продать" in op_type_raw:
            op = "sale"
        else:
            op = "buy" if "куп" in op_type_raw else "sale" if "прод" in op_type_raw else None
        if op is None:
            skipped_no_type += 1
            continue

        # quantity — используем to_int_safe
        qty = 0
        q_idx = cols.get("quantity")
        if q_idx is not None and q_idx < len(cells):
            qty = to_int_safe(cells[q_idx])
        if qty == 0:
            skipped_no_qty += 1
            continue

        # price
        pr = 0.0
        p_idx = cols.get("price")
        if p_idx is not None and p_idx < len(cells):
            pr = to_num_safe(cells[p_idx])

        # currency
        currency = ""
        cur_idx = cols.get("currency_calc")
        if cur_idx is not None and cur_idx < len(cells):
            currency_raw = str(cells[cur_idx]).strip()
            currency = src.constants.CURRENCY_DICT.get(currency_raw.upper(),
                                                       currency_raw.upper() if currency_raw else "")

        # sum
        total = 0.0
        s_idx = cols.get("sum")
        if s_idx is not None and s_idx < len(cells):
            total = to_num_safe(cells[s_idx])

        # aci
        aci = 0.0
        aci_idx = cols.get("aci")
        if aci_idx is not None and aci_idx < len(cells):
            aci = to_num_safe(cells[aci_idx])

        # trade number / operation id
        op_id = ""
        trade_idx = cols.get("trade_no")
        if trade_idx is not None and trade_idx < len(cells):
            op_id = str(cells[trade_idx]).strip()

        # comment
        comment = ""
        comm_idx = cols.get("comment")
        if comm_idx is not None and comm_idx < len(cells):
            comment = str(cells[comm_idx]).strip()

        dto = OperationDTO(
            date=date_val,
            operation_type=op,
            payment_sum=total,
            currency=currency,
            ticker=(curr_ticker or ""),
            isin=(curr_isin or ""),
            reg_number=(curr_reg or ""),
            price=pr,
            quantity=qty,
            aci=aci,
            comment=comment,
            operation_id=op_id,
        )
        results.append(dto)
        parsed_rows += 1

    stats = {
        "total_rows": total_rows,
        "parsed": parsed_rows,
        "skipped_empty": skipped_empty,
        "skipped_no_date": skipped_no_date,
        "skipped_no_qty": skipped_no_qty,
        "skipped_no_type": skipped_no_type,
        "skipped_itogo": skipped_itogo,
    }

    logger.info(
        "Trades parsing stats: total_rows=%s parsed=%s skipped_empty=%s skipped_no_date=%s skipped_no_qty=%s skipped_no_type=%s skipped_itogo=%s",
        total_rows, parsed_rows, skipped_empty, skipped_no_date, skipped_no_qty, skipped_no_type, skipped_itogo
    )

    return results, stats


def parse_stock_bond_trades(file_path: Union[str, Any]) -> tuple[List[OperationDTO], dict]:
    df = pd.read_excel(file_path, header=None, dtype=object).fillna("")
    start_idx = find_trades_block_start(df)
    if start_idx is None:
        logger.info("Trades block 'Заключенные в отчетном периоде сделки с ценными бумагами' not found")
        return [], {}

    header_idx = find_trades_header_row(df, start_idx)
    if header_idx is None:
        logger.warning("Trades header row not found after block start; attempting to use start_idx as header")
        header_idx = start_idx

    # build combined header to support multi-line headers
    combined_header = _build_combined_header(df, header_idx, max_rows=3)
    cols = map_trades_header_indices(combined_header)
    if not cols:
        # fallback: try single header row
        header_row = df.iloc[header_idx]
        cols = map_trades_header_indices(header_row)
    if not cols:
        logger.warning("Could not map any trade columns from header row(s): %s", combined_header[:10])
        return [], {}

    results, stats = parse_trades_table(df, header_idx, cols)

    # sort by date
    def key_fn(o: OperationDTO):
        d = o.date
        if isinstance(d, datetime):
            return d
        try:
            return pd.to_datetime(d, dayfirst=True, errors="coerce")
        except Exception:
            return pd.NaT

    results_sorted = sorted(results, key=key_fn)
    return results_sorted, stats
