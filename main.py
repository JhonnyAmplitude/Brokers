import json

from parser import FILE_PATH, get_rows_from_file, parse_rows


def main():
    rows = list(get_rows_from_file(FILE_PATH))

    # Первая часть — общие данные и операции
    header_data, operations = parse_rows(rows)

    result = {
        **header_data,
        "operations": operations,
    }

    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
