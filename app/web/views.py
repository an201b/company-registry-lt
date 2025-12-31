# company-registry-lt/app/web/views.py 
import os
from fastapi import APIRouter, Request, Depends
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.core.db import get_db
from app.models.company import Company

router = APIRouter()

# Указываем, где лежат наши HTML файлы
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

@router.get("/")
async def index_page(request: Request, code: str = None, db: AsyncSession = Depends(get_db)):
    """
    Главная страница.
    Если передан параметр ?code=..., ищем компанию.
    """
    company = None
    error = None

    if code:
        # Чистим ввод от пробелов
        clean_code = code.strip()
        
        # Ищем в базе
        query = select(Company).where(Company.code == clean_code)
        result = await db.execute(query)
        company = result.scalar_one_or_none()
        
        if not company:
            error = f"Компания с кодом '{clean_code}' не найдена."

    return templates.TemplateResponse(
        "index.html", 
        {
            "request": request, 
            "company": company, 
            "search_code": code,
            "error": error
        }
    )