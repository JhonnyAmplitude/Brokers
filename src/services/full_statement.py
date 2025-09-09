from src.parsers.header import parse_header
from src.parsers.fin_operations import parse_fin_operations

def parse_full_statement(file_path: str) -> dict:
    header = parse_header(file_path)
    fin_ops = parse_fin_operations(file_path)
    return {
        **header,
        "operations": [op.to_dict() for op in fin_ops]
    }
