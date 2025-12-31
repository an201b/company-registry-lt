#company-registry-lt\app\core\db.py
import os
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase, sessionmaker

# 1. Определяем путь к файлу базы данных
# Мы вычисляем абсолютный путь, чтобы файл создавался точно в папке data, 
# откуда бы мы ни запускали скрипт.
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "registry.db")

# Строки подключения (Connection Strings)
# Для API (асинхронно): используем драйвер aiosqlite
DATABASE_URL_ASYNC = f"sqlite+aiosqlite:///{DB_PATH}"
# Для Импортера (синхронно): стандартный драйвер sqlite
DATABASE_URL_SYNC = f"sqlite:///{DB_PATH}"

# --- 2. ASYNC НАСТРОЙКИ (Для FastAPI) ---
async_engine = create_async_engine(
    DATABASE_URL_ASYNC,
    echo=False,  # Если True, в консоль будут выводиться все SQL-запросы (удобно для отладки)
    connect_args={"check_same_thread": False}  # Только для SQLite
)

# Фабрика сессий для API
AsyncSessionLocal = async_sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    expire_on_commit=False
)

# --- 3. SYNC НАСТРОЙКИ (Для Pandas/Импортера) ---
sync_engine = create_engine(
    DATABASE_URL_SYNC,
    echo=False,
    connect_args={"check_same_thread": False}
)
# Фабрика сессий для синхронного кода (если понадобится)
SyncSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=sync_engine)

# --- 4. БАЗОВАЯ МОДЕЛЬ (Declarative Base) ---
# От этого класса мы будем наследовать все наши модели (таблицы)
class Base(DeclarativeBase):
    pass

# --- 5. ЗАВИСИМОСТЬ (Dependency) ---
# Эту функцию мы будем использовать в FastAPI endpoints: Depends(get_db)
async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()