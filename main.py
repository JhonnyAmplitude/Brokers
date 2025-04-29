import json

from parser import FILE_PATH, get_rows_from_file, parse_rows, parse_deals_block


def main():
    rows = list(get_rows_from_file(FILE_PATH))

    # Первая часть — общие данные и операции
    header_data, operations = parse_rows(rows)

    # Вторая часть — сделки
    deals = parse_deals_block(rows)

    result = {
        **header_data,
        "operations": operations,
        "deals": deals,  # добавляем сделки как отдельный блок
    }

    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
