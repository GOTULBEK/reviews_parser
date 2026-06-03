from fastapi import APIRouter, Query

from app.schemas.cities import CityItem, CityListResponse
from app.services import cities

router = APIRouter()


@router.get(
    "/cities",
    response_model=CityListResponse,
    summary="Список городов Казахстана для выбора в поиске",
)
async def list_cities(refresh: bool = Query(default=False, description="Принудительно обновить из 2ГИС")):
    catalog = await cities.get_cities(force_refresh=refresh)
    return CityListResponse(
        count=len(catalog),
        cities=[CityItem(**c) for c in catalog],
    )
