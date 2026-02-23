from pydantic import BaseModel

class ConversionRequest(BaseModel):
    student_id: str
    subject_id: str
    original_value: float
    from_system: str