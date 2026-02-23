from __future__ import annotations

from typing import Optional
from pydantic import BaseModel, ConfigDict, Field

class EquivalenceCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    subject_id_a: str = Field(..., description="Subject origen (SUB-...)")
    subject_id_b: str = Field(..., description="Subject destino (SUB-...)")

    bidirectional: bool = Field(True, description="Si true, crea A->B y B->A")
    partial: bool = Field(False, description="Si true, equivalencia parcial")
    coverage: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Cobertura (0..1) si es parcial (opcional). Ej: 0.5",
    )
    note: Optional[str] = Field(default=None, max_length=200, description="Nota/observación (opcional)")


class EquivalenceDelete(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    subject_id_a: str
    subject_id_b: str
    bidirectional: bool = True