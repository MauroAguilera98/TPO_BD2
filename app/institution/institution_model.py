from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

# ID "de dominio" legible y estable
INSTITUTION_ID_PATTERN = r"^INS-[A-Z]{2}-\d{4,12}$"


class InstitutionCreate(BaseModel):
    """
    POST /institutions
    """
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    institution_id: str = Field(
        ...,
        pattern=INSTITUTION_ID_PATTERN,
        description="ID de institución. Formato: INS-<PAIS>-<DIGITOS> (4 a 12). Ej: INS-AR-0001",
        examples=["INS-AR-0001"],
    )
    name: str = Field(..., min_length=2, max_length=120, examples=["UADE"])
    country: str = Field(..., min_length=2, max_length=2, description="ISO-3166 alpha-2", examples=["AR"])
    system: Optional[str] = Field(
        default=None,
        description="Sistema de calificación predominante (opcional). Ej: AR / US / UK / DE",
        examples=["AR"],
    )
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Metadata flexible (opcional).")

    @field_validator("country")
    @classmethod
    def _upper_country(cls, v: str) -> str:
        return v.upper()

    @field_validator("system")
    @classmethod
    def _upper_system(cls, v: Optional[str]) -> Optional[str]:
        return v.upper() if v else v


class InstitutionUpdate(BaseModel):
    """
    PATCH /institutions/{institution_id}
    """
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    name: Optional[str] = Field(default=None, min_length=2, max_length=120)
    country: Optional[str] = Field(default=None, min_length=2, max_length=2)
    system: Optional[str] = Field(default=None)
    metadata: Optional[Dict[str, Any]] = Field(default=None)

    @field_validator("country")
    @classmethod
    def _upper_country(cls, v: Optional[str]) -> Optional[str]:
        return v.upper() if v else v

    @field_validator("system")
    @classmethod
    def _upper_system(cls, v: Optional[str]) -> Optional[str]:
        return v.upper() if v else v


class InstitutionOut(BaseModel):
    """
    GET responses
    """
    model_config = ConfigDict(extra="ignore")

    institution_id: str
    name: str
    country: str
    system: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

    is_active: bool = True
    created_at: datetime
    updated_at: datetime
    deleted_at: Optional[datetime] = None