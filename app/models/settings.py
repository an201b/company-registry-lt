# company-registry-lt\app\models\settings.py
from sqlalchemy import Column, String
from app.core.db import Base

class Setting(Base):
    __tablename__ = "settings"

    key = Column(String, primary_key=True, index=True) # Например "jar_url"
    value = Column(String, nullable=False)             # Сама ссылка
    description = Column(String, nullable=True)        # Описание для админа