import json

from parser import parse_xls_header


file_path = "B_k-494884_ALL_23-04.xls"
result = parse_xls_header(file_path)
print(json.dumps(result, ensure_ascii=False, indent=2))