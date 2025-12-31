# company-registry-lt/app/services/registry_importer.py
import pandas as pd
from app.core.db import SessionLocal
from app.models.company import Company
# Импорты для скачивания файла...

def update_registry_data():
    """
    1. Скачивает файл с сайта RC во временную папку.
    2. Читает его через Pandas (read_csv с разделителем |).
    3. Сохраняет в базу данных (метод bulk insert или upsert).
    """
    # Примерная логика с pandas (очень быстрая)
    df = pd.read_csv("data/temp/JAR.csv", sep='|', quotechar='"')
    
    # Переименование колонок под нашу модель
    df = df.rename(columns={"ja_kodas": "code", "ja_pavadinimas": "name", ...})
    
    # Сохранение в SQL (append или replace)
    # df.to_sql('companies', con=engine, if_exists='replace', index=False)
    pass