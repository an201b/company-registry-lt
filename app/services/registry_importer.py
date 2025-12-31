# company-registry-lt/app/services/registry_importer.py
import os
import requests
import pandas as pd
from sqlalchemy import text, select
from sqlalchemy.orm import Session
from app.core.db import sync_engine
from app.models.settings import Setting

# Пути к файлам
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TEMP_DIR = os.path.join(BASE_DIR, "data", "temp")
JAR_PATH = os.path.join(TEMP_DIR, "JAR.csv")
PVM_PATH = os.path.join(TEMP_DIR, "PVM.csv")

def get_url_from_db(key_name):
    """Получает URL из базы данных синхронно."""
    with Session(sync_engine) as session:
        setting = session.get(Setting, key_name)
        if setting:
            return setting.value
    return None

def download_file(url, path, name="FILE"):
    if not url:
        print(f"⚠️ [IMPORTER] URL для {name} не задан в настройках!")
        return False
        
    print(f"⬇️ [IMPORTER] Скачиваю {name}: {url}")
    os.makedirs(TEMP_DIR, exist_ok=True)
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        response = requests.get(url, headers=headers, stream=True)
        
        if response.status_code == 404:
             print(f"❌ [IMPORTER] Ошибка 404. Ссылка устарела: {url}")
             return False
        response.raise_for_status()
        
        with open(path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"✅ [IMPORTER] {name} скачан.")
        return True
    except Exception as e:
        print(f"❌ [IMPORTER] Ошибка скачивания {name}: {e}")
        return False
        
def process_and_save():
    print("🔄 [IMPORTER] Обработка данных (Merge)...")
    
    if not os.path.exists(JAR_PATH):
        print("❌ [IMPORTER] Файл реестра JAR не найден. Пропуск.")
        return

    try:
        # 1. Читаем Компании (JAR)
        jar_cols = [
            "ja_kodas", "ja_pavadinimas", "adresas", "ja_reg_data", 
            "form_kodas", "form_pavadinimas", 
            "stat_kodas", "stat_pavadinimas", "stat_data_nuo", "formavimo_data"
        ]
        df_jar = pd.read_csv(JAR_PATH, sep='|', quotechar='"', dtype=str, usecols=lambda c: c in jar_cols)
        
        df_jar = df_jar.rename(columns={
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

        # 2. Читаем НДС (VMI)
        if os.path.exists(PVM_PATH) and os.path.getsize(PVM_PATH) > 0:
            print("🔄 [IMPORTER] Читаю файл НДС...")
            try:
                # Читаем CSV VMI
                df_pvm = pd.read_csv(PVM_PATH, sep=';', dtype=str, on_bad_lines='skip')
                
                # Ищем колонки динамически
                code_col = [c for c in df_pvm.columns if 'kodas' in c.lower() and 'pvm' not in c.lower()]
                pvm_col = [c for c in df_pvm.columns if 'pvm' in c.lower() and 'kodas' in c.lower()]

                if code_col and pvm_col:
                    df_pvm = df_pvm[[code_col[0], pvm_col[0]]]
                    df_pvm.columns = ['code', 'pvm_code']
                    
                    # Убираем дубликаты
                    df_pvm = df_pvm.drop_duplicates(subset=['code'], keep='last')
                    
                    print(f"📊 [IMPORTER] Найдено {len(df_pvm)} плательщиков НДС.")
                    
                    # Merge
                    df_final = pd.merge(df_jar, df_pvm, on='code', how='left')
                else:
                    print("⚠️ [IMPORTER] Не удалось распознать колонки в файле PVM. Пропускаю PVM.")
                    df_final = df_jar
            except Exception as e:
                print(f"⚠️ [IMPORTER] Ошибка чтения PVM файла: {e}. Грузим только JAR.")
                df_final = df_jar
        else:
            df_final = df_jar

        # 4. Обработка дат
        date_cols = ["registration_date", "status_date_from", "data_updated_at"]
        for col in date_cols:
            if col in df_final.columns:
                df_final[col] = pd.to_datetime(df_final[col], errors='coerce').dt.date

        # Очистка NaN
        df_final = df_final.where(pd.notnull(df_final), None)

        # 5. Сохранение
        print("💾 [IMPORTER] Сохранение в БД...")
        with sync_engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS companies"))
            
            df_final.to_sql("companies", conn, if_exists='replace', index=False, chunksize=2000)
            
            # Индексы (Восстанавливаем после replace)
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_companies_code ON companies (code)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_companies_name ON companies (name)"))
            # Добавили индекс для PVM
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_companies_pvm ON companies (pvm_code)"))

        print("✅ [IMPORTER] Импорт завершен!")

    except Exception as e:
        print(f"❌ [IMPORTER] Критическая ошибка обработки: {e}")

def run_full_import():
    # 1. Получаем ссылки из БД
    jar_url = get_url_from_db("jar_url")
    pvm_url = get_url_from_db("pvm_url")

    # 2. Качаем
    download_file(jar_url, JAR_PATH, "JAR Registry")
    download_file(pvm_url, PVM_PATH, "VMI PVM Data")
    
    # 3. Обрабатываем (вызываем функцию напрямую)
    process_and_save()