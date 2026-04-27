from pydantic import BaseModel, Field, field_validator
from .common import BranchIdStr

class PreviewRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=512)
    city: str = Field(default="almaty", min_length=1, max_length=64)
    max_results: int = Field(default=20, ge=1, le=100)

class BranchPreviewItem(BaseModel):
    gis_branch_id: BranchIdStr
    firm_url: str
    name: str | None = None
    address: str | None = None

class PreviewResponse(BaseModel):
    query: str
    city: str
    count: int
    branches: list[BranchPreviewItem]

class ScrapeRequest(BaseModel):
    city: str = Field(..., min_length=1, max_length=64)
    gis_branch_ids: list[BranchIdStr] = Field(..., min_length=1, max_length=100)
    query: str | None = Field(default=None, max_length=512)

    @field_validator("gis_branch_ids")
    @classmethod
    def _dedupe(cls, v: list[str]) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for x in v:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out
