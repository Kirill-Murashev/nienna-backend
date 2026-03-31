from fastapi import APIRouter

router = APIRouter()


@router.get("/meta")
def get_meta() -> dict[str, object]:
    return {
        "service": "nienna-backend",
        "version": "0.1.0",
        "module": "nienna",
        "display_name": "Nienna",
        "capabilities": [
            "rosstat-regional-dataset",
            "indicator-explorer",
            "region-profile",
            "compare-regions",
            "theme-dashboards",
        ],
        "status": "mvp-ready",
    }
