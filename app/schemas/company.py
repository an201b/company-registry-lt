#  company-registry-lt/app/schemas/company.py
from pydantic import BaseModel, ConfigDict
from typing import Optional
from datetime import date

class CompanyResponse(BaseModel):
    """
    То, как данные увидит клиент (1C или браузер).
    """
    code: str
    name: str
    address: Optional[str] = None
    
    registration_date: Optional[date] = None
    
    legal_form_code: Optional[int] = None
    legal_form_name: Optional[str] = None
    
    status_code: Optional[int] = None
    status_name: Optional[str] = None
    status_date_from: Optional[date] = None
    
    data_updated_at: Optional[date] = None

    # Эта настройка позволяет Pydantic читать данные прямо из SQLAlchemy моделей
    model_config = ConfigDict(from_attributes=True)