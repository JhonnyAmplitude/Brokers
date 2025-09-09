import sys
import json
from src.services.full_statement import parse_full_statement
from src.utils import logger

def main():
    if len(sys.argv) < 2:
        logger.error("Usage: python -m src.main <path_to_excel>")
        sys.exit(1)
    path = sys.argv[1]
    out = parse_full_statement(path)
    print(json.dumps(out, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
