from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
from io import BytesIO

import xlrd
import openpyxl

from final import parse_full_statement
from OperationDTO import OperationDTO

app = FastAPI()

def extract_rows(file_bytes: bytes, file_extension: str):
    if file_extension == ".xls":
        wb = xlrd.open_workbook(file_contents=file_bytes)
        sheet = wb.sheet_by_index(0)
        for row_idx in range(sheet.nrows):
            yield sheet.row_values(row_idx)
    elif file_extension == ".xlsx":
        wb = openpyxl.load_workbook(BytesIO(file_bytes), data_only=True)
        sheet = wb.active
        for row in sheet.iter_rows(values_only=True):
            yield list(row)
    else:
        raise HTTPException(status_code=400, detail="Неподдерживаемый формат файла")

@app.post("/parse-financial-operations")
async def upload_file(file: UploadFile = File(...)):
    file_extension = file.filename.split('.')[-1].lower()

    if file_extension not in ["xls", "xlsx"]:
        raise HTTPException(status_code=400, detail="Поддерживаются только .xls и .xlsx файлы")

    contents = await file.read()

    try:
        file_path = f"/tmp/{file.filename}"
        with open(file_path, "wb") as f:
            f.write(contents)

        result = parse_full_statement(file_path)

        if "operations" in result:
            result["operations"] = [op.to_dict() if isinstance(op, OperationDTO) else op for op in result["operations"]]

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return JSONResponse(content=result)
