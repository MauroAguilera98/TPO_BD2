from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional, Union

from pydantic import BaseModel, ConfigDict, Field


class OriginalGrade(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    scale: str = Field(..., min_length=1, max_length=16)
    value: Union[float, int, str]


class GradeCreate(BaseModel):
    """
    POST /grades
    """
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    student_id: str
    institution_id: str
    subject_id: str

    original_grade: OriginalGrade

    # si no viene, usamos server time
    issued_at: Optional[datetime] = None

    metadata: Dict[str, Any] = Field(default_factory=dict)


class GradeOut(BaseModel):
    model_config = ConfigDict(extra="ignore")

    grade_id: str
    student_id: str
    institution_id: str
    subject_id: str
    country: str

    original_grade: Dict[str, Any]
    metadata: Dict[str, Any]

    issued_at: datetime
    created_at: datetime
    immutable_hash: str

class GradeCorrectionCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    original_grade: OriginalGrade
    issued_at: Optional[datetime] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    reason: Optional[str] = Field(default=None, max_length=200)