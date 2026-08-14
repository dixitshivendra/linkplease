from pydantic import BaseModel


class RuleCreate(BaseModel):
    keyword: str
    dm_message: str


class RuleResponse(BaseModel):
    rule_id: str
    keyword: str
    dm_message: str


class StatsResponse(BaseModel):
    sent: int
    failed: int
    queued: int
    duplicates_blocked: int
