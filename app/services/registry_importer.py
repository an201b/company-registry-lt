# company-registry-lt/app/services/registry_importer.py
import os
import requests
import pandas as pd
from sqlalchemy import text
from app.core.db import sync_engine
from app.models.company import Company

# Ссылка на официальный файл открытых данных (формат CSV, разделитель |)
RC_DATA_URL = "https://www.registrucentras.lt/aduomenys/?byla=JAR_IREGISTRUOTI.csv"

# Пути к папкам
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TEMP_DIR = os.path.join(BASE_DIR, "data", "temp")
CSV_PATH = os.path.join(TEMP_DIR, "JAR_IREGISTRUOTI.csv")

def download_file():
    """Скачивает свежий файл реестра во временную папку."""
    print(f"⬇️ [IMPORTER] Начинаю скачивание: {RC_DATA_URL}")
    
    # Создаем папку data/temp, если её нет
    os.makedirs(TEMP_DIR, exist_ok=True)
    
    try:
        response = requests.get(RC_DATA_URL, stream=True)
        response.raise_for_status()
        
        with open(CSV_PATH, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print(f"✅ [IMPORTER] Файл скачан: {CSV_PATH}")
        return True
    except Exception as e:
        print(f"❌ [IMPORTER] Ошибка скачивания: {e}")
        return False

def process_and_save():
    """Читает CSV и обновляет базу данных."""
    if not os.path.exists(CSV_PATH):
        print("❌ [IMPORTER] Файл не найден. Сначала скачайте его.")
        return

    print("🔄 [IMPORTER] Чтение CSV файла (Pandas)...")
    
    try:
        # 1. Читаем CSV
        # Разделитель '|', кодировка utf-8
        df = pd.read_csv(CSV_PATH, sep='|', quotechar='"', dtype=str)
        
        # 2. Переименовываем колонки (CSV -> Database)
        # Соответствие имен колонок нашей модели
        df = df.rename(columns={
            "ja_kodas": "code",
            "ja_pavadinimas": "name",
            "adresas": "address",
            "ja_reg_data": "registration_date",
            "form_kodas": "legal_form_code",
            "form_pavadinimas": "legal_form_name",
            "stat_kodas": "status_code",
            "stat_pavadinimas": "status_name",
            "stat_data_nuo": "status_date_from",
            "formavimo_data": "data_updated_at"
        })

        # Оставляем только те колонки, которые есть в нашей базе
        expected_columns = [
            "code", "name", "address", "registration_date", 
            "legal_form_code", "legal_form_name", 
            "status_code", "status_name", "status_date_from", "data_updated_at"
        ]
        # Выбираем только существующие в DF колонки из списка (защита от изменения формата CSV)
        cols_to_save = [c for c in expected_columns if c in df.columns]
        df = df[cols_to_save]

        # 3. Обработка дат (Pandas умеет умно преобразовывать строки в даты)
        date_cols = ["registration_date", "status_date_from", "data_updated_at"]
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

        print(f"📊 [IMPORTER] Подготовлено {len(df)} записей.")

        # 4. Сохранение в БД
        # Используем синхронный движок (sync_engine)
        with sync_engine.begin() as conn:
            # Вариант А: Полная перезапись (самый быстрый и надежный для реестра)
            print("🧹 [IMPORTER] Очистка старой таблицы...")
            conn.execute(text("DELETE FROM companies")) # Удаляем все старые записи
            
            print("💾 [IMPORTER] Вставка новых данных...")
            # chunksize=1000 помогает не забить память при вставке
            df.to_sql("companies", conn, if_exists='append', index=False, chunksize=2000)
            
        print("✅ [IMPORTER] Импорт завершен успешно!")

    except Exception as e:
        print(f"❌ [IMPORTER] Ошибка обработки: {e}")

def run_full_import():
    """Главная функция для запуска всего процесса."""
    if download_file():
        process_and_save()