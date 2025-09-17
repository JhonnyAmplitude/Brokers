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

    # сохраняем результат
    out_path = Path("result.json")
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2, default=str)

    # извлекаем нужные метрики
    meta = out.get("meta", {})
    fin_stats = meta.get("fin_ops_stats", {})
    trade_stats = meta.get("trade_ops_stats", {})

    fin_parsed = fin_stats.get("parsed", 0)
    trade_parsed = trade_stats.get("parsed", 0)
    total_ops = meta.get("total_ops_count", len(out.get("operations", [])))
    acct_id = out.get("account_id") or out.get("account") or out.get("client_id", "N/A")

    # нераспознанные названия (уже уникализированные в парсере)
    unrec_names = fin_stats.get("unrecognized_names", []) or []
    unrec_count = len(unrec_names)

    # печать в компактном виде (лимит имён до 50, чтобы не раздувать строку)
    names_part = ""
    if unrec_names:
        display = unrec_names[:50]
        names_part = ": " + "; ".join(display)
        if unrec_count > len(display):
            names_part += f"; ...(+{unrec_count - len(display)} more)"

    # сохранённый лог
    logger.info("Результат сохранён в %s", out_path.resolve())

    # требуемый компактный вывод
    print(f"Аккаунт: {acct_id}, операций: {total_ops}")
    print(f"  финансовых операций: {fin_parsed}, операции с ценными бумагами: {trade_parsed}, не распознанные финансовые операции: {unrec_count}{names_part}")


if __name__ == "__main__":
    main()
