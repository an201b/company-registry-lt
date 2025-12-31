# company-registry-lt\app\models\company.py
from sqlalchemy import Column, String, Date, Integer, Text
from app.core.db import Base

class Company(Base):
    __tablename__ = "companies"

    code = Column(String, primary_key=True, index=True) # ja_kodas (Код предприятия)
    name = Column(String, index=True)                   # ja_pavadinimas
    address = Column(Text)                              # adresas
    reg_date = Column(Date, nullable=True)              # ja_reg_data
    status = Column(String, nullable=True)              # stat_pavadinimas
    # ... остальные поля по желанию