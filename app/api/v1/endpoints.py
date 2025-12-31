# company-registry-lt\app\api\v1\endpoints.py
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.api import deps
from app.models.company import Company

router = APIRouter()

@router.get("/company/{code}")
def get_company_info(code: str, db: Session = Depends(deps.get_db)):
    company = db.query(Company).filter(Company.code == code).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found in LT registry")
    return company