# company-registry-lt\app\api\v1\endpoints.py
from fastapi import APIRouter, Depends, HTTPException, Path
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.core.db import get_db
from app.models.company import Company
from app.schemas.company import CompanyResponse

router = APIRouter()

@router.get("/company/{code}", response_model=CompanyResponse)
async def get_company_by_code(
    code: str = Path(..., title="Код предприятия", min_length=1, max_length=20),
    db: AsyncSession = Depends(get_db)
):
    """
    Поиск компании по коду (JAR Kodas).
    """
    # Асинхронный запрос к БД
    query = select(Company).where(Company.code == code)
    result = await db.execute(query)
    company = result.scalar_one_or_none()

    if not company:
        raise HTTPException(status_code=404, detail="Компания с таким кодом не найдена")
    
    return company