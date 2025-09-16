import sys
import json
from pathlib import Path

from src.services.full_statement import parse_full_statement
from src.utils import logger


def main():
    if len(sys.argv) < 2:
        logger.error("Usage: python -m src.main <path_to_excel>")
        sys.exit(1)

    path = sys.argv[1]
    out = parse_full_statement(path)

    # сохраняем результат в файл
    out_path = Path("result.json")
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    # достаём мета-данные
    meta = out.get("meta", {})
    fin_count = meta.get("fin_ops_raw_count", 0)
    trade_count = meta.get("trade_ops_raw_count", 0)
    total_ops = meta.get("total_ops_count", len(out.get("operations", [])))
    acct_id = out.get("account_id") or out.get("account") or out.get("client_id", "N/A")

    # лог и печать в консоль
    logger.info(f"Результат сохранён в {out_path.resolve()}")
    print(f"Аккаунт: {acct_id}, операций: {total_ops}")
    print(f"  финансовых операций: {fin_count}, операции с ценными бумагами: {trade_count}")


if __name__ == "__main__":
    main()
