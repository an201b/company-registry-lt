# company-registry-lt\app\models\company.py
from sqlalchemy import Column, String, Integer, Date, Text
from app.core.db import Base

class Company(Base):
    """
    Модель таблицы компаний.
    Полностью отражает структуру файла JAR_IREGISTRUOTI.csv
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
    # form_kodas (Код формы, например 310)
    legal_form_code = Column(Integer, nullable=True)
    
    # form_pavadinimas (Название формы, например 'Uždaroji akcinė bendrovė')
    legal_form_name = Column(String, nullable=True)
    
    # --- Статус (Действует, Банкрот...) ---
    # stat_kodas (Код статуса, например 0 - Действует)
    status_code = Column(Integer, index=True, nullable=True)
    
    # stat_pavadinimas (Название статуса, например 'Teisinis stat neįregistruotas')
    status_name = Column(String, nullable=True)
    
    # stat_data_nuo (С какой даты действует этот статус)
    status_date_from = Column(Date, nullable=True)
    
    # --- Метаданные ---
    # formavimo_data (Дата выгрузки записи из реестра)
    data_updated_at = Column(Date, nullable=True)

    def __repr__(self):
        return f"<Company(code='{self.code}', name='{self.name}', status='{self.status_name}')>"