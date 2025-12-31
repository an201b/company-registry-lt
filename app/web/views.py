# company-registry-lt/app/web/views.py 
import os
from fastapi import APIRouter, Request, Depends, Response
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession
# Добавляем or_ (ИЛИ) для SQL запросов
from sqlalchemy import select, or_

from app.core.db import get_db
from app.models.company import Company
from app.core.translations import TRANSLATIONS

# --- УДАЛЕНА ОШИБОЧНАЯ СТРОКА: from app.web.views import get_locale ---

router = APIRouter()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

# Доступные языки
SUPPORTED_LANGS = ["lt", "en", "pl", "be", "ru"]

def get_locale(request: Request, lang: str = None) -> str:
    """
    Определяет язык в порядке приоритета.
    """
    # 1. Если передан параметр
    if lang and lang in SUPPORTED_LANGS:
        return lang
    
    # 2. Проверяем куки
    cookie_lang = request.cookies.get("company_registry_lang")
    if cookie_lang and cookie_lang in SUPPORTED_LANGS:
        return cookie_lang
    
    # 3. Проверяем браузер
    accept = request.headers.get("accept-language", "")
    browser_lang = accept[:2]
    if browser_lang in SUPPORTED_LANGS:
        return browser_lang
        
    # 4. Дефолт
    return "lt"

@router.get("/")
async def index_page(
    request: Request, 
    response: Response,
    q: str = None, 
    lang: str = None,
    db: AsyncSession = Depends(get_db)
):
    # Здесь мы просто вызываем функцию, объявленную выше
    current_lang = get_locale(request, lang)
    tr = TRANSLATIONS[current_lang]

    companies = [] 
    error = None
    search_query = q 

    if q:
        clean_q = q.strip()
        
        # ЛОГИКА ПОИСКА
        if clean_q.isdigit():
            # Поиск по коду
            stmt = select(Company).where(Company.code == clean_q)
        else:
            # Поиск по имени или адресу
            stmt = select(Company).where(
                or_(
                    Company.name.ilike(f"%{clean_q}%"),
                    Company.address.ilike(f"%{clean_q}%")
                )
            ).limit(50)

        result = await db.execute(stmt)
        companies = result.scalars().all()
        
        if not companies:
            error = tr["not_found"].format(clean_q)

    resp = templates.TemplateResponse(
        "index.html", 
        {
            "request": request, 
            "companies": companies,
            "search_query": search_query,
            "error": error,
            "tr": tr,
            "current_lang": current_lang,
            "langs": TRANSLATIONS
        }
    )
    
    if lang:
        resp.set_cookie(key="company_registry_lang", value=current_lang, max_age=3600*24*30)
        
    return resp