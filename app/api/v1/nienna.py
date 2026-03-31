from typing import Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field

from app.services.dataset_service import DatasetService, get_dataset_service
from app.services.themes import THEME_DEFINITIONS

router = APIRouter()


class IndicatorSeriesPayload(BaseModel):
    object_names: list[str] = Field(default_factory=list)
    object_level: str = "Регион"
    subsection: str | None = None
    year_from: int | None = None
    year_to: int | None = None


class CompareIndicatorPayload(BaseModel):
    code: str
    subsection: str | None = None


class CompareRegionsPayload(BaseModel):
    year: int
    object_names: list[str] = Field(default_factory=list)
    indicators: list[CompareIndicatorPayload] = Field(default_factory=list)


@router.get("")
def get_service_overview() -> dict[str, object]:
    return {
        "service": "nienna-backend",
        "module": "nienna",
        "display_name": "Nienna",
        "data_domain": "rosstat",
        "description": "Rosstat regional data dashboard backend with explorer, profile and comparison flows.",
        "phase": "mvp",
    }


@router.get("/dataset")
def get_dataset_summary(
    service: DatasetService = Depends(get_dataset_service),
) -> dict[str, object]:
    return service.get_dataset_summary()


@router.get("/filters/meta")
def get_filters_meta(
    service: DatasetService = Depends(get_dataset_service),
) -> dict[str, Any]:
    return service.get_filters_meta()


@router.get("/themes")
def get_themes() -> dict[str, Any]:
    return {"items": THEME_DEFINITIONS}


@router.get("/indicators/search")
def search_indicators(
    query: str | None = Query(default=None),
    section: str | None = Query(default=None),
    theme_id: str | None = Query(default=None),
    limit: int = Query(default=40, ge=1, le=100),
    service: DatasetService = Depends(get_dataset_service),
) -> dict[str, Any]:
    return service.search_indicators(query=query, section=section, theme_id=theme_id, limit=limit)


@router.get("/indicators/{indicator_code}")
def get_indicator_detail(
    indicator_code: str,
    service: DatasetService = Depends(get_dataset_service),
) -> dict[str, Any]:
    return service.get_indicator_detail(indicator_code)


@router.get("/indicators/{indicator_code}/snapshot")
def get_indicator_snapshot(
    indicator_code: str,
    year: int | None = Query(default=None),
    subsection: str | None = Query(default=None),
    object_level: str = Query(default="Регион"),
    service: DatasetService = Depends(get_dataset_service),
) -> dict[str, Any]:
    return service.get_indicator_snapshot(
        indicator_code=indicator_code,
        year=year,
        subsection=subsection,
        object_level=object_level,
    )


@router.post("/indicators/{indicator_code}/series")
def get_indicator_series(
    indicator_code: str,
    payload: IndicatorSeriesPayload,
    service: DatasetService = Depends(get_dataset_service),
) -> dict[str, Any]:
    return service.get_indicator_series(
        indicator_code=indicator_code,
        object_names=payload.object_names,
        subsection=payload.subsection,
        object_level=payload.object_level,
        year_from=payload.year_from,
        year_to=payload.year_to,
    )


@router.get("/regions/{region_name}/profile")
def get_region_profile(
    region_name: str,
    year: int = Query(...),
    benchmark_name: str = Query(default="Российская Федерация"),
    service: DatasetService = Depends(get_dataset_service),
) -> dict[str, Any]:
    return service.get_region_profile(
        region_name=region_name,
        year=year,
        benchmark_name=benchmark_name,
    )


@router.post("/compare")
def compare_regions(
    payload: CompareRegionsPayload,
    service: DatasetService = Depends(get_dataset_service),
) -> dict[str, Any]:
    return service.compare_regions(
        indicators=[item.model_dump() for item in payload.indicators],
        object_names=payload.object_names,
        year=payload.year,
    )


@router.get("/themes/{theme_id}/dashboard")
def get_theme_dashboard(
    theme_id: str,
    year: int = Query(...),
    object_name: str = Query(...),
    service: DatasetService = Depends(get_dataset_service),
) -> dict[str, Any]:
    return service.get_theme_dashboard(theme_id=theme_id, year=year, object_name=object_name)
