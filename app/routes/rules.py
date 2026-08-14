from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.database import get_db
from app.models import Rule
from app.schemas import RuleCreate, RuleResponse
import uuid

router = APIRouter()


@router.post("/rules", response_model=RuleResponse, status_code=201)
def create_rule(rule: RuleCreate, db: Session = Depends(get_db)):
    db_rule = Rule(id=str(uuid.uuid4()), keyword=rule.keyword, dm_message=rule.dm_message)
    db.add(db_rule)
    db.commit()
    db.refresh(db_rule)
    return RuleResponse(
        rule_id=db_rule.id,
        keyword=db_rule.keyword,
        dm_message=db_rule.dm_message,
    )
