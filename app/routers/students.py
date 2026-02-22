from __future__ import annotations

from fastapi import APIRouter, Query
from typing import Literal

from app.student.student_model import (
    StudentCreate,
    StudentUpdate,
    StudentAddTrajectory,
    StudentOut,
    TrajectoryExpectedEndYearUpdate,
)
from app.student.student_service import StudentService

router = APIRouter(prefix="/students", tags=["Students"])


@router.post("", response_model=StudentOut, status_code=201)
async def create_student(body: StudentCreate):
    """
    Crea un estudiante (student_id STU-...) y su trayectoria inicial (mínimo 1).
    También registra auditoría CREATE.
    """
    return await StudentService.create(body.model_dump())


@router.get("/{student_id}", response_model=StudentOut)
async def get_student(
    student_id: str,
    include_inactive: bool = Query(False, description="Si true, permite consultar estudiantes dados de baja lógica."),
):
    """
    Devuelve un estudiante por ID.
    """
    return await StudentService.get(student_id, include_inactive=include_inactive)


@router.patch("/{student_id}", response_model=StudentOut)
async def update_student(student_id: str, body: StudentUpdate):
    """
    Actualiza perfil (full_name/email). No permite modificar trayectorias.
    Registra auditoría UPDATE.
    """
    changes = {k: v for k, v in body.model_dump().items() if v is not None}
    return await StudentService.update_profile(student_id, changes)


@router.post("/{student_id}/trajectories", response_model=StudentOut, status_code=201)
async def add_trajectory(student_id: str, body: StudentAddTrajectory):
    """
    Append-only: agrega una nueva trayectoria al estudiante.
    Registra auditoría TRAJECTORY_ADD.
    """
    return await StudentService.add_trajectory(student_id, body.trajectory.model_dump())


@router.delete("/{student_id}", response_model=StudentOut)
async def delete_student(student_id: str):
    """
    Baja lógica del estudiante (soft delete).
    Registra auditoría DELETE.
    """
    return await StudentService.delete(student_id)

@router.patch("/{student_id}/trajectories/{trajectory_id}/expected-end-year", response_model=StudentOut)
async def update_expected_end_year(student_id: str, trajectory_id: str, body: TrajectoryExpectedEndYearUpdate):
    return await StudentService.update_expected_end_year(
        student_id,
        trajectory_id,
        body.expected_end_year
    )