from datetime import datetime

from OperationDTO import OperationDTO
from fin import normalize_currency


def parse_forex_trades(sheet) -> list[OperationDTO]:
    operations = []
    current_ticker = None
    currency = None

    for row_idx in range(sheet.nrows):
        row = sheet.row_values(row_idx)

        if row[0] and "TOM" in str(row[0]):
            current_ticker = str(row[0]).strip()
        elif "Валюта лота:" in row:
            try:
                raw_currency = row[row.index("Валюта лота:") + 1]
                currency = normalize_currency(raw_currency)
            except (IndexError, AttributeError, ValueError):
                currency = None

        if row[0] and "Итого по" in str(row[0]):
            continue

        try:
            exec_date_str = str(row[0]).strip()
            operation_id = str(row[1]).strip()
            if not operation_id or not exec_date_str:
                continue
        except IndexError:
            continue

        try:
            deal_date_str = str(row[9]).strip()
            deal_time_str = str(row[10]).strip()
            deal_datetime = datetime.strptime(deal_date_str + " " + deal_time_str, "%d.%m.%y %H:%M:%S")
        except Exception:
            continue

        buy_price = row[3]
        buy_qty = row[4]
        buy_payment = row[5]
        sell_price = row[6]
        sell_qty = row[7]
        sell_payment = row[8]

        try:
            if buy_price:
                operation_type = "currency_buy"
                price = float(buy_price)
                quantity = float(buy_qty)
                payment_sum = float(str(buy_payment).replace(",", ""))
            elif sell_price:
                operation_type = "currency_sale"
                price = float(sell_price)
                quantity = float(sell_qty)
                payment_sum = float(str(sell_payment).replace(",", ""))
            else:
                continue
        except (ValueError, TypeError):
            continue

        op = OperationDTO(
            date=deal_datetime.date(),
            operation_type=operation_type,
            payment_sum=payment_sum,
            currency=normalize_currency("RUB"),
            ticker=current_ticker,
            isin=None,
            price=price,
            quantity=quantity,
            aci=None,
            comment=None,
            operation_id=operation_id,
        )

        print(op)  # 👈 выводим каждую операцию
        operations.append(op)

    return operations
