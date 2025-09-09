from typing import Dict, Any
from src.parsers.header import parse_header
from src.utils import logger

def parse_full_statement(file_path: str) -> Dict[str, Any]:
    logger.info("Start parsing %s", file_path)
    header = parse_header(file_path)
    result: Dict[str, Any] = {
        **header,
        "operations": []
    }
    logger.info("Finished parsing header for %s", file_path)
    return result
