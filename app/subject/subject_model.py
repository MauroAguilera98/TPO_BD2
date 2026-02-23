from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

SUBJECT_ID_PATTERN = r"^SUB-[A-Z]{2}-\d{4,12}$"  # Ej: SUB-AR-0001


class SubjectCreate(BaseModel):
    """
    POST /subjects
    """
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    subject_id: str = Field(
        ...,
        pattern=SUBJECT_ID_PATTERN,
        description="ID de materia/evaluación. Formato: SUB-<PAIS>-<DIGITOS>. Ej: SUB-AR-0001",
        examples=["SUB-AR-0001"],
    )
    institution_id: str = Field(..., description="ID de institución (INS-...)")
    name: str = Field(..., min_length=2, max_length=150, examples=["Álgebra I"])

    kind: Literal["subject", "evaluation"] = Field(
        default="subject",
        description="Tipo: materia (subject) o evaluación (evaluation).",
    )

    level: Optional[str] = Field(default=None, description="Nivel (opcional): secundario/terciario/grado/posgrado…")
    credits: Optional[float] = Field(default=None, ge=0, description="Créditos (opcional).")
    external_code: Optional[str] = Field(default=None, description="Código externo (opcional).")

    metadata: Dict[str, Any] = Field(default_factory=dict, description="Metadata flexible (opcional).")

    @field_validator("institution_id")
    @classmethod
    def _strip_institution_id(cls, v: str) -> str:
        return v.strip()


class SubjectUpdate(BaseModel):
    """
    PATCH /subjects/{subject_id}
    """
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    institution_id: Optional[str] = None
    name: Optional[str] = Field(default=None, min_length=2, max_length=150)
    kind: Optional[Literal["subject", "evaluation"]] = None

    level: Optional[str] = None
    credits: Optional[float] = Field(default=None, ge=0)
    external_code: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class SubjectOut(BaseModel):
    model_config = ConfigDict(extra="ignore")

    subject_id: str
    institution_id: str
    name: str
    kind: str

    level: Optional[str] = None
    credits: Optional[float] = None
    external_code: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

    is_active: bool = True
    created_at: datetime
    updated_at: datetime
    deleted_at: Optional[datetime] = None