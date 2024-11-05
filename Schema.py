# Schema.py
from pydantic import BaseModel

class URLModel(BaseModel):
    url: str
