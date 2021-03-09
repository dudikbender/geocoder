from typing import Optional, List
from pydantic import BaseModel
from datetime import datetime

class User(BaseModel):
    email: str
    password: str

class PostcodeResult(BaseModel):
    postcode: str
    latitude: float
    longitude: float
    ward_name: str
    lad_name: str
    easting: float
    northing: float

class GoogleResult(BaseModel):
    formatted_address: str
    postcode: str
    latitude: float
    longitude: float
    ward: str
    local_authority: str
    ward_total_pop_2018: float
    ward_median_pop_2018: float
    local_authority_proj_pop_2030: float

'''class MultiPostcodeResults(BaseModel):
    responses = List[PostcodeResult]'''

class Population(BaseModel):
    description: str
    year: int
    value: float

class Ward(BaseModel):
    code: str
    name: str
    population: Optional[List[Population]] = None

class LocalAuthority(BaseModel):
    code: str
    name: str
    population: Optional[List[Population]] = None


