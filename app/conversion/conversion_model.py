from typing import Union
from pydantic import BaseModel

class ConversionRequest(BaseModel):
    student_id: str
    subject_id: str
    original_value: Union[float, str]  
    from_system: str