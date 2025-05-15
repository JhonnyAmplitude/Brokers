import os
import logging
import tempfile
from pathlib import Path
from typing import Dict, Any

import xlrd
import openpyxl
from fastapi import FastAPI, File, UploadFile, HTTPException, Depends
from fastapi.responses import JSONResponse
from starlette.middleware.cors import CORSMiddleware

from final import parse_full_statement
from OperationDTO import OperationDTO

# === Настройка логгирования ===
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

setup_logging()
logger = logging.getLogger(__name__)

# === Настройки приложения ===
app = FastAPI(
    title="Финансовый парсер",
    description="API для парсинга отчетов БКС",
    version="1.0.0"
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Разрешаем доступ только с этого адреса
    allow_credentials=True,
    allow_methods=["*"],  # Разрешаем все HTTP методы
    allow_headers=["*"],  # Разрешаем все заголовки
)

ALLOWED_EXTENSIONS = {"xls", "xlsx"}

def validate_file_extension(file: UploadFile) -> str:
    """Проверяет расширение файла и возвращает его, если оно допустимо."""
    extension = Path(file.filename).suffix.lower().lstrip(".")

    if not extension or extension not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Поддерживаются только файлы: {', '.join(ALLOWED_EXTENSIONS)}"
        )

    return extension


import tempfile
import os


async def save_upload_file_tmp(file: UploadFile):
    # Используем стандартный временный каталог
    temp_dir = tempfile.gettempdir()  # Получаем путь к системному временному каталогу
    temp_file_path = os.path.join(temp_dir, file.filename)

    try:
        with open(temp_file_path, "wb") as temp_file:
            temp_file.write(await file.read())
        return temp_file_path
    except Exception as e:
        logger.error(f"Ошибка при сохранении файла: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка при сохранении файла: {e}")


def serialize_operations(result: Dict[str, Any]) -> Dict[str, Any]:
    """Преобразует объекты OperationDTO в словари."""
    operations = result.get("operations")
    if operations:
        result["operations"] = [
            op.to_dict() if isinstance(op, OperationDTO) else op
            for op in operations
        ]
    return result


@app.post(
    "/parse-financial-operations",
    response_model=Dict[str, Any],
    summary="Парсинг финансовых операций из Excel файла",
    description="Загрузите XLS или XLSX файл для извлечения финансовых операций"
)
async def parse_file(
    file: UploadFile = File(..., description="Excel файл с финансовыми операциями"),
    file_extension: str = Depends(validate_file_extension)
):
    """Обрабатывает загруженный Excel файл и извлекает финансовые операции."""
    temp_path = await save_upload_file_tmp(file)
    logger.info(f"Обработка файла: {file.filename} ({file_extension}), путь: {temp_path}")

    try:
        result = parse_full_statement(temp_path)
        return JSONResponse(content=serialize_operations(result))
    except Exception as e:
        logger.error(f"Ошибка при парсинге файла: {e}")
        raise HTTPException(status_code=422, detail=f"Ошибка при парсинге файла: {e}")
    finally:
        try:
            os.unlink(temp_path)
            logger.debug(f"Удален временный файл: {temp_path}")
        except Exception as e:
            logger.warning(f"Не удалось удалить временный файл {temp_path}: {e}")

@app.get("/health", response_model=Dict[str, str])
async def health_check():
    """Проверка состояния сервиса."""
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
