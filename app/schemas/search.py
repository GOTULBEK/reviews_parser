from pydantic import BaseModel, Field, field_validator, model_validator
import typing
from typing import Any
from .common import BranchIdStr, SourceType

class PreviewRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=512)
    city: str = Field(default="almaty", min_length=1, max_length=64)
    max_results: int = Field(default=20, ge=1, le=100)
    source: typing.Literal["2gis", "zapis", "all"] = Field(default="2gis")

class BranchPreviewItem(BaseModel):
    gis_branch_id: BranchIdStr
    source: SourceType
    firm_url: str
    name: str | None = None
    address: str | None = None

class PreviewResponse(BaseModel):
    query: str
    city: str
    count: int
    branches: list[BranchPreviewItem]

class ScrapeBranchItem(BaseModel):
    gis_branch_id: BranchIdStr
    source: SourceType = SourceType.twogis

class ScrapeRequest(BaseModel):
    city: str = Field(..., min_length=1, max_length=64)
    branches: list[ScrapeBranchItem] = Field(default_factory=list, max_length=100)
    gis_branch_ids: list[str] | None = None
    query: str | None = Field(default=None, max_length=512)

    @model_validator(mode="before")
    @classmethod
    def _backwards_compatibility(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if "gis_branch_ids" in data and not data.get("branches"):
                data["branches"] = [
                    {"gis_branch_id": str(bid), "source": "2gis"} 
                    for bid in data["gis_branch_ids"]
                ]
        return data

    @model_validator(mode="after")
    def _check_not_empty(self) -> 'ScrapeRequest':
        if not self.branches:
            raise ValueError("branches cannot be empty")
        return self

    @field_validator("branches")
    @classmethod
    def _dedupe(cls, v: list[ScrapeBranchItem]) -> list[ScrapeBranchItem]:
        seen = set()
        out = []
        for x in v:
            k = (x.gis_branch_id, x.source)
            if k not in seen:
                seen.add(k)
                out.append(x)
        return out

