# company-registry-lt/app/web/views.py 
import os
from fastapi import APIRouter, Request, Depends, Response, Form
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, or_

from app.core.db import get_db
from app.models.company import Company
from app.models.settings import Setting
from app.core.translations import TRANSLATIONS

router = APIRouter()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

# Доступные языки
SUPPORTED_LANGS = ["lt", "en", "pl", "be", "ru"]

def get_locale(request: Request, lang: str = None) -> str:
    """Определяет язык в порядке приоритета."""
    if lang and lang in SUPPORTED_LANGS:
        return lang
    cookie_lang = request.cookies.get("company_registry_lang")
    if cookie_lang and cookie_lang in SUPPORTED_LANGS:
        return cookie_lang
    accept = request.headers.get("accept-language", "")
    browser_lang = accept[:2]
    if browser_lang in SUPPORTED_LANGS:
        return browser_lang
    return "lt"

@router.get("/")
async def index_page(
    request: Request, 
    response: Response,
    q: str = None, 
    lang: str = None,
    db: AsyncSession = Depends(get_db)
):
    current_lang = get_locale(request, lang)
    tr = TRANSLATIONS[current_lang]

    companies = [] 
    error = None
    search_query = q 

    if q:
        clean_q = q.strip()
        if clean_q.isdigit():
            stmt = select(Company).where(Company.code == clean_q)
        else:
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

@router.get("/settings")
async def settings_page(request: Request, db: AsyncSession = Depends(get_db)):
    """Отображает страницу настроек."""
    # Получаем текущий язык
    current_lang = get_locale(request)
    
    stmt = select(Setting)
    result = await db.execute(stmt)
    settings_list = result.scalars().all()
    
    settings_dict = {s.key: s for s in settings_list}

    return templates.TemplateResponse(
        "settings.html", 
        {
            "request": request, 
            "settings": settings_dict,
            "tr": TRANSLATIONS[current_lang],
            # --- ВАЖНО: Добавили эти две строки, чтобы шапка не ломалась ---
            "current_lang": current_lang,
            "langs": TRANSLATIONS
        }
    )

@router.post("/settings")
async def settings_save(
    request: Request, 
    jar_url: str = Form(...), 
    pvm_url: str = Form(...),
    db: AsyncSession = Depends(get_db)
):
    """Сохраняет настройки."""
    current_lang = get_locale(request)
    
    # 1. Обновляем JAR URL
    jar_setting = await db.get(Setting, "jar_url")
    if jar_setting:
        jar_setting.value = jar_url.strip()
    
    # 2. Обновляем PVM URL
    pvm_setting = await db.get(Setting, "pvm_url")
    if pvm_setting:
        pvm_setting.value = pvm_url.strip()
        
    await db.commit()
    
    return templates.TemplateResponse(
        "settings.html", 
        {
            "request": request, 
            "settings": {"jar_url": jar_setting, "pvm_url": pvm_setting},
            "tr": TRANSLATIONS[current_lang],
            "message": "Настройки сохранены! Перезапустите обновление.",
            # --- ВАЖНО: И здесь тоже добавили ---
            "current_lang": current_lang,
            "langs": TRANSLATIONS
        }
    )