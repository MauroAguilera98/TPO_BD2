from __future__ import annotations

from datetime import datetime
from typing import List, Optional, Literal

from pydantic import BaseModel, ConfigDict, EmailStr, Field, model_validator

# ID “de dominio” (legible) para mantener consistencia entre Mongo/Neo4j/Cassandra/Redis
STUDENT_ID_PATTERN = r"^STU-\d{5,12}$"


# -------------------------
# 1) Trayectorias (append-only)
# -------------------------
class TrajectoryIn(BaseModel):
    """
    Trayectoria educativa que llega desde el cliente (input).
    OJO: es append-only: se agregan nuevas trayectorias, no se editan/reemplazan las existentes.

    expected_end_year = año estipulado/planificado (puede cambiar durante ongoing)
    end_year = año real de finalización (solo cuando completed)
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    country: str = Field(
        ...,
        min_length=2,
        max_length=2,
        description="Código país ISO-3166 alpha-2. Ej: AR, US, UK, DE",
        examples=["AR"],
    )

    institution: str = Field(
        ...,
        min_length=1,
        max_length=120,
        description="Institución. Ej: UADE, UBA, MIT",
        examples=["UADE"],
    )

    level: Optional[str] = Field(
        default=None,
        max_length=60,
        description="Nivel educativo (opcional). Ej: Undergrad, Secondary, Postgrad",
        examples=["Universitario"],
    )

    start_year: int = Field(
        ...,
        ge=1900,
        le=2100,
        description="Año de inicio (obligatorio).",
        examples=[2024],
    )

    expected_end_year: Optional[int] = Field(
        default=None,
        ge=1900,
        le=2100,
        description="Año estimado/planificado de finalización (opcional). Puede cambiar mientras status=ongoing.",
        examples=[2027],
    )

    end_year: Optional[int] = Field(
        default=None,
        ge=1900,
        le=2100,
        description="Año real de finalización. Obligatorio si status=completed. Debe ser >= start_year.",
        examples=[2028],
    )

    status: Literal["ongoing", "completed", "dropped"] = Field(
        default="ongoing",
        description="Estado de la trayectoria. Default: ongoing",
        examples=["ongoing"],
    )

    @model_validator(mode="after")
    def validate_years(self):
        # coherencia básica
        if self.expected_end_year is not None and self.expected_end_year < self.start_year:
            raise ValueError("expected_end_year debe ser >= start_year")

        if self.end_year is not None and self.end_year < self.start_year:
            raise ValueError("end_year debe ser >= start_year")

        # reglas según estado
        if self.status == "ongoing":
            # fin real aún no ocurrió
            if self.end_year is not None:
                raise ValueError("Si status='ongoing', end_year debe ser null (fin real aún no ocurrió).")

        if self.status == "completed":
            # si completó, fin real es obligatorio
            if self.end_year is None:
                raise ValueError("Si status='completed', end_year es obligatorio.")

        return self


class TrajectoryOut(TrajectoryIn):
    """
    Trayectoria tal como se devuelve al cliente (output).
    Incluye metadata de servidor para poder ordenar y auditar.
    """

    model_config = ConfigDict(extra="ignore")

    trajectory_id: str = Field(
        ...,
        description="ID único de la trayectoria (lo genera el servidor).",
        examples=["trj_3f7b2c..."],
    )

    created_at: datetime = Field(
        ...,
        description="Timestamp de creación de la trayectoria (lo genera el servidor).",
    )


# -------------------------
# 2) Estudiante (Create / Update / Output)
# -------------------------
class StudentCreate(BaseModel):
    """
    Input de creación. POST /students
    Trajectories: mínimo 1 (arranque de la carrera).
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    student_id: str = Field(
        ...,
        pattern=STUDENT_ID_PATTERN,
        description="ID del estudiante. Formato STU- + dígitos (5 a 12).",
        examples=["STU-12345"],
    )

    full_name: str = Field(
        ...,
        min_length=2,
        max_length=120,
        description="Nombre completo del estudiante.",
        examples=["Ana Pérez"],
    )

    email: Optional[EmailStr] = Field(
        default=None,
        description="Email del estudiante (opcional).",
        examples=["ana@demo.com"],
    )

    trajectories: List[TrajectoryIn] = Field(
        ...,
        min_length=1,
        description="Lista de trayectorias (mínimo 1). Se agregan, no se editan.",
    )


class StudentUpdate(BaseModel):
    """
    Input de actualización general. PUT/PATCH /students/{id}
    Importante: NO incluye trajectories para respetar append-only.
    Para agregar trayectoria: usar endpoint dedicado (ej: POST /students/{id}/trajectories).
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    full_name: Optional[str] = Field(
        default=None,
        min_length=2,
        max_length=120,
        description="Nuevo nombre completo (opcional).",
    )

    email: Optional[EmailStr] = Field(
        default=None,
        description="Nuevo email (opcional).",
    )


class StudentAddTrajectory(BaseModel):
    """
    Input para agregar una trayectoria (append-only).
    POST /students/{id}/trajectories
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    trajectory: TrajectoryIn


class StudentOut(BaseModel):
    """
    Output recomendado para respuestas (GET).
    Incluye metadatos típicos: created_at, updated_at, is_active, deleted_at.
    """

    model_config = ConfigDict(extra="ignore")

    student_id: str = Field(..., examples=["STU-12345"])
    full_name: str
    email: Optional[EmailStr] = None

    trajectories: List[TrajectoryOut] = Field(default_factory=list)

    is_active: bool = True
    created_at: datetime
    updated_at: datetime
    deleted_at: Optional[datetime] = None

class TrajectoryExpectedEndYearUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    expected_end_year: int = Field(
        ...,
        ge=1900,
        le=2100,
        description="Nuevo año estimado de finalización (planificado).",
        examples=[2028],
    )