import uvicorn
from fastapi import FastAPI, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import select

from app.core.db import async_engine, Base
# Импортируем модели, чтобы они зарегистрировались в Base metadata
from app.models import company
from app.models.settings import Setting
from app.api.v1.endpoints import router as api_router
from app.web.views import router as web_router
from app.services.registry_importer import run_full_import

# Дефолтные настройки
DEFAULT_SETTINGS = [
    {
        "key": "jar_url", 
        "value": "https://www.registrucentras.lt/aduomenys/?byla=JAR_IREGISTRUOTI.csv",
        "description": "Ссылка на файл реестра (JAR)"
    },
    {
        "key": "pvm_url", 
        # ОБНОВЛЕНО: Вставили рабочую ссылку data.gov.lt
        "value": "https://get.data.gov.lt/datasets/gov/vmi/pvm_moketojai/Moketoja_duomenys_pvm_moketojai.csv",
        "description": "Ссылка на плательщиков НДС (VMI)"
    },
    {
        "key": "capital_url", 
        "value": "https://www.registrucentras.lt/aduomenys/?byla=JAR_KAPITALAS.csv",
        "description": "Ссылка на уставной капитал (JAR Kapitalas)"
    }
]

# --- НАСТРОЙКА ПЛАНИРОВЩИКА ---
scheduler = BackgroundScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. ЗАПУСК
    print("🚀 [STARTUP] Инициализация сервиса...")
    
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        
        # --- ПРОВЕРКА И СОЗДАНИЕ НАСТРОЕК ---
        from sqlalchemy.orm import Session
        
        # Функция для запуска внутри sync-контекста
        def init_settings(connection):
            session = Session(bind=connection)
            for item in DEFAULT_SETTINGS:
                existing = session.get(Setting, item["key"])
                if not existing:
                    print(f"⚙️ [CONFIG] Создаю настройку по умолчанию: {item['key']}")
                    new_setting = Setting(key=item["key"], value=item["value"], description=item["description"])
                    session.add(new_setting)
            session.commit()
            session.close()

        await conn.run_sync(init_settings)
        # ------------------------------------
        
    print("✅ [DB] Таблицы проверены.")

    # Планируем задачу на 04:00 утра
    scheduler.add_job(run_full_import, 'cron', hour=4, minute=0)
    scheduler.start()
    print("⏰ [SCHEDULER] Автоматическое обновление запланировано на 04:00.")

    yield

    # 2. ОСТАНОВКА
    print("🛑 [SHUTDOWN] Остановка сервиса...")
    scheduler.shutdown()

app = FastAPI(title="Company Registry LT", lifespan=lifespan)

# Статика
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Ручка для принудительного обновления
@app.post("/api/v1/force-update", tags=["Admin"])
async def force_update_db(
    background_tasks: BackgroundTasks,
    # Принимаем параметры запроса (Query Parameters)
    dl_jar: bool = True,
    dl_pvm: bool = True,
    dl_cap: bool = True
):
    """
    Запускает обновление. Можно отключить скачивание файлов флагами.
    """
    # Передаем параметры в функцию импорта
    background_tasks.add_task(run_full_import, dl_jar, dl_pvm, dl_cap)
    return {"message": "Обновление запущено. Проверьте консоль."}
    
# Роутеры
app.include_router(api_router, prefix="/api/v1", tags=["API"])
app.include_router(web_router, tags=["Web"])

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8010, reload=True)