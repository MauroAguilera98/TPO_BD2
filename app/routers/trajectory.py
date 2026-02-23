from fastapi import APIRouter, Query
from app.trajectory.trajectory_service import TrajectoryService

router = APIRouter(prefix="/trajectory", tags=["Trajectory"])

@router.get("/{student_id}/full")
async def get_full(
    student_id: str, 
    to_system: str = Query("US"),
    version: str = Query("v1")
):
    return await TrajectoryService.get_full_trajectory(student_id, to_system, version)