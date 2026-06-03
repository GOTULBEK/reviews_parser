from pydantic import BaseModel


class CityItem(BaseModel):
    id: str
    slug: str
    name: str
    branch_count: int | None = None


class CityListResponse(BaseModel):
    count: int
    cities: list[CityItem]


class TaskCityItem(BaseModel):
    slug: str
    name: str | None = None
    branch_count: int


class TaskCityListResponse(BaseModel):
    count: int
    cities: list[TaskCityItem]
