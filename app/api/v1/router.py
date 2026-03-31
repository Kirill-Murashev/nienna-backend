from fastapi import APIRouter

from app.api.v1.nienna import router as nienna_router
from app.api.v1.system import router as system_router

api_router = APIRouter()
api_router.include_router(system_router, tags=["system"])
api_router.include_router(nienna_router, prefix="/nienna", tags=["nienna"])
