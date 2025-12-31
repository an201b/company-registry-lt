# company-registry-lt\app\models\company.py
from sqlalchemy import Column, String, Integer, Date, Text
from app.core.db import Base

class Company(Base):
    """
    Модель таблицы компаний.
    Отражает структуру файла JAR + данные об НДС (PVM).
    """
    __tablename__ = "companies"

    # --- Идентификация ---
    # ja_kodas (Код предприятия)
    code = Column(String, primary_key=True, index=True)
    
    # ja_pavadinimas (Наименование)
    name = Column(String, index=True, nullable=False)
    
    # --- Адрес ---
    # adresas (Адрес регистрации)
    address = Column(Text, nullable=True)
    
    # --- Даты ---
    # ja_reg_data (Дата регистрации компании)
    registration_date = Column(Date, nullable=True)
    
    # --- Правовая форма (UAB, MB, AB...) ---
    # form_kodas
    legal_form_code = Column(Integer, nullable=True)
    
    # form_pavadinimas
    legal_form_name = Column(String, nullable=True)
    
    # --- Статус (Действует, Банкрот...) ---
    # stat_kodas
    status_code = Column(Integer, index=True, nullable=True)
    
    # stat_pavadinimas
    status_name = Column(String, nullable=True)
    
    # stat_data_nuo
    status_date_from = Column(Date, nullable=True)
    
    # --- Метаданные ---
    # formavimo_data
    data_updated_at = Column(Date, nullable=True)

    # --- НОВЫЕ ПОЛЯ (НДС / PVM) ---
    # Код PVM (например LT100012345611)
    pvm_code = Column(String, nullable=True, index=True) 
    # С какого числа является плательщиком
    pvm_date = Column(Date, nullable=True)

    def __repr__(self):
        return f"<Company(code='{self.code}', name='{self.name}', pvm='{self.pvm_code}')>"