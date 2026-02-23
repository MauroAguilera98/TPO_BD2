from fastapi import APIRouter, BackgroundTasks, Query
from app.conversion.conversion_model import ConversionRequest
from app.conversion.conversion_service import ConversionService

router = APIRouter(prefix="/conversion", tags=["Conversion"])

@router.post("")
async def execute_conversion(
    req: ConversionRequest, 
    background_tasks: BackgroundTasks, 
    to_system: str = Query(..., description="Sistema destino"),
    version: str = Query("v1", description="Versión de la regla")
):
    return await ConversionService.convert_grade(req, to_system, version, background_tasks)