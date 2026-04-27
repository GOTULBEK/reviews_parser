from typing import Annotated

def _coerce_branch_id_to_str(v) -> str:
    if isinstance(v, int):
        return str(v)
    if isinstance(v, str):
        if not v.isdigit():
            raise ValueError("gis_branch_id must be numeric digits")
        return v
    raise TypeError(f"gis_branch_id must be int or str, got {type(v).__name__}")

from pydantic import BeforeValidator

BranchIdStr = Annotated[str, BeforeValidator(_coerce_branch_id_to_str)]
