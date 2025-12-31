# company-registry-lt\app\main.py
import uvicorn
# ВАЖНО: Добавил BackgroundTasks в список импортов ниже
from fastapi import FastAPI, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler

from app.core.db import async_engine, Base
from app.models import company
from app.api.v1.endpoints import router as api_router
from app.web.views import router as web_router
from app.services.registry_importer import run_full_import

# --- НАСТРОЙКА ПЛАНИРОВЩИКА ---
scheduler = BackgroundScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. ЗАПУСК
    print("🚀 [STARTUP] Инициализация сервиса...")
    
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
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
async def force_update_db(background_tasks: BackgroundTasks):
    """
    Запускает обновление базы данных прямо сейчас в фоновом режиме.
    """
    background_tasks.add_task(run_full_import)
    return {"message": "Обновление запущено в фоне. Следите за логами."}

# Роутеры
app.include_router(api_router, prefix="/api/v1", tags=["API"])
app.include_router(web_router, tags=["Web"])

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8010, reload=True)