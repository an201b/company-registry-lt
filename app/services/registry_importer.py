# company-registry-lt/app/services/registry_importer.py
import os
import requests
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session
from app.core.db import sync_engine
from app.models.settings import Setting

# Пути к файлам
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TEMP_DIR = os.path.join(BASE_DIR, "data", "temp")
JAR_PATH = os.path.join(TEMP_DIR, "JAR.csv")
PVM_PATH = os.path.join(TEMP_DIR, "PVM.csv")
CAPITAL_PATH = os.path.join(TEMP_DIR, "CAPITAL.csv")

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
        with requests.get(url, headers=headers, stream=True, timeout=120) as r:
            if r.status_code == 404:
                 print(f"❌ [IMPORTER] Ошибка 404. Ссылка устарела: {url}")
                 return False
            r.raise_for_status()
            
            with open(path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
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
        df_final = df_jar
        
        if os.path.exists(PVM_PATH) and os.path.getsize(PVM_PATH) > 100:
            print("🔄 [IMPORTER] Читаю файл НДС...")
            try:
                try:
                    df_pvm = pd.read_csv(PVM_PATH, sep=',', dtype=str, on_bad_lines='skip')
                    if len(df_pvm.columns) < 2:
                        df_pvm = pd.read_csv(PVM_PATH, sep=';', dtype=str, on_bad_lines='skip')
                except:
                    df_pvm = pd.read_csv(PVM_PATH, sep=';', dtype=str, on_bad_lines='skip')
                
                df_pvm.columns = [str(c).lower().strip() for c in df_pvm.columns]

                possible_code_cols = ['mokescio_moketojo_identifikacinis_numeris', 'kodas', 'ja_kodas', 'code']
                possible_pvm_cols = ['pvm_moketojo_kodas', 'pvm_kodas', 'pvm', 'pvm_code']

                found_code = next((c for c in possible_code_cols if c in df_pvm.columns), None)
                found_pvm = next((c for c in possible_pvm_cols if c in df_pvm.columns), None)

                if found_code and found_pvm:
                    df_pvm = df_pvm[[found_code, found_pvm]]
                    df_pvm.columns = ['code', 'pvm_code']
                    
                    df_pvm = df_pvm.dropna(subset=['pvm_code'])
                    df_pvm = df_pvm.drop_duplicates(subset=['code'], keep='last')
                    
                    print(f"📊 [IMPORTER] Найдено {len(df_pvm)} записей с НДС.")
                    
                    # Безопасное слияние
                    df_final = pd.merge(df_jar, df_pvm, on='code', how='left')
                else:
                    print(f"⚠️ [IMPORTER] Не найдены нужные колонки в PVM. Использую только JAR.")
            except Exception as e:
                print(f"⚠️ [IMPORTER] Ошибка чтения PVM файла: {e}. Грузим только JAR.")
        else:
            print("⚠️ [IMPORTER] Файл PVM пуст или слишком мал. Грузим только JAR.")


        # 3. Читаем Капитал
        if os.path.exists(CAPITAL_PATH):
            print("🔄 [IMPORTER] Читаю файл Капитала...")
            try:
                # Пробуем стандартный пайп |
                df_cap = pd.read_csv(CAPITAL_PATH, sep='|', quotechar='"', dtype=str, on_bad_lines='skip')
                
                if len(df_cap.columns) < 2:
                     df_cap = pd.read_csv(CAPITAL_PATH, sep=',', quotechar='"', dtype=str, on_bad_lines='skip')
                
                df_cap.columns = [c.strip().lower() for c in df_cap.columns]

                # Варианты названий (ДОБАВИЛИ ist_kapitalas)
                cap_code_cols = ['ja_kodas', 'kodas', 'code']
                cap_val_cols = ['ist_kapitalas', 'kapitalo_dydis', 'capital', 'amount']
                
                found_code = next((c for c in cap_code_cols if c in df_cap.columns), None)
                found_val = next((c for c in cap_val_cols if c in df_cap.columns), None)

                if found_code and found_val:
                    rename_map = {found_code: "code", found_val: "authorized_capital"}
                    
                    found_curr = next((c for c in ['valiuta', 'currency'] if c in df_cap.columns), None)
                    if found_curr:
                        rename_map[found_curr] = "capital_currency"
                    
                    df_cap = df_cap.rename(columns=rename_map)
                    
                    cols = ["code", "authorized_capital"]
                    if "capital_currency" in df_cap.columns:
                        cols.append("capital_currency")
                    
                    df_cap = df_cap[cols]

                    if "capital_currency" not in df_cap.columns:
                        df_cap["capital_currency"] = "EUR"
                    
                    if "authorized_capital" in df_cap.columns:
                        # 1. Меняем запятую на точку
                        df_cap["authorized_capital"] = df_cap["authorized_capital"].astype(str).str.replace(',', '.', regex=False)
                        # 2. ПРЕВРАЩАЕМ В ЧИСЛО (Float)
                        df_cap["authorized_capital"] = pd.to_numeric(df_cap["authorized_capital"], errors='coerce')                    
                    df_cap = df_cap.drop_duplicates(subset=['code'], keep='last')
                    
                    print(f"💰 [IMPORTER] Найдено {len(df_cap)} записей о капитале.")
                    
                    # ПРИСОЕДИНЯЕМ К ОБЩЕЙ ТАБЛИЦЕ
                    df_final = pd.merge(df_final, df_cap, on='code', how='left')
                    
                else:
                    print(f"⚠️ [IMPORTER] Колонки Капитала не распознаны. Найдены: {list(df_cap.columns)}")
            except Exception as e:
                print(f"⚠️ [IMPORTER] Ошибка чтения файла капитала: {e}")


        # 4. ЗАЩИТА ОТ ОШИБОК SQL
        required_cols = ['pvm_code', 'pvm_date', 'authorized_capital', 'capital_currency']
        for col in required_cols:
            if col not in df_final.columns:
                df_final[col] = None

        # 5. Обработка дат
        date_cols = ["registration_date", "status_date_from", "data_updated_at"]
        for col in date_cols:
            if col in df_final.columns:
                df_final[col] = pd.to_datetime(df_final[col], errors='coerce').dt.date

        # Очистка NaN
        df_final = df_final.where(pd.notnull(df_final), None)
        
        # 6. Сохранение
        print("💾 [IMPORTER] Сохранение в БД...")
        with sync_engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS companies"))
            
            df_final.to_sql("companies", conn, if_exists='replace', index=False, chunksize=2000)
            
            # Индексы
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_companies_code ON companies (code)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_companies_name ON companies (name)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_companies_pvm ON companies (pvm_code)"))

        print("✅ [IMPORTER] Импорт завершен!")

    except Exception as e:
        print(f"❌ [IMPORTER] Критическая ошибка обработки: {e}")

def run_full_import(download_jar: bool = True, download_pvm: bool = True, download_capital: bool = True):
    """
    Запускает процесс импорта.
    Аргументы позволяют пропустить скачивание определенных файлов (для отладки).
    """
    # 1. Получаем ссылки из БД
    jar_url = get_url_from_db("jar_url")
    pvm_url = get_url_from_db("pvm_url")
    capital_url = get_url_from_db("capital_url")

    # 2. Качаем (только если попросили)
    if download_jar:
        download_file(jar_url, JAR_PATH, "JAR Registry")
    else:
        print("⏭️ [IMPORTER] Скачивание JAR пропущено (используем локальный файл).")

    if download_pvm:
        download_file(pvm_url, PVM_PATH, "VMI PVM Data")
    else:
        print("⏭️ [IMPORTER] Скачивание PVM пропущено (используем локальный файл).")

    if download_capital:
        download_file(capital_url, CAPITAL_PATH, "JAR Capital") 
    else:
        print("⏭️ [IMPORTER] Скачивание Capital пропущено (используем локальный файл).")
    
    # 3. Обрабатываем (берет файлы с диска, даже если не качали сейчас)
    process_and_save()