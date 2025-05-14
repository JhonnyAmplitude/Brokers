from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
from io import BytesIO

import xlrd
import openpyxl

from final import parse_full_statement
from OperationDTO import OperationDTO

app = FastAPI()

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
