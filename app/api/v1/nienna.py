from fastapi import APIRouter, Depends

from app.services.dataset_service import DatasetService, get_dataset_service

router = APIRouter()


@router.get("")
def get_service_overview() -> dict[str, object]:
    return {
        "service": "nienna-backend",
        "module": "nienna",
        "display_name": "Nienna",
        "data_domain": "rosstat",
        "description": "Rosstat regional data dashboard backend.",
        "phase": "bootstrap",
    }


@router.get("/dataset")
def get_dataset_summary(
    service: DatasetService = Depends(get_dataset_service),
) -> dict[str, object]:
    return service.get_dataset_summary()
