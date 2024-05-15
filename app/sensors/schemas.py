from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class Sensor(BaseModel):
    id: int
    name: str
    latitude: float
    longitude: float
    joined_at: datetime | None = None
    last_seen: datetime | None = None
    type: str
    mac_address: str
    manufacturer: str
    model: str
    serie_number:str
    firmware_version: str
    battery_level: float | None = None
    temperature: float | None = None
    humidity:  float | None = None
    velocity:  float | None = None
    description: str
    
    
    class Config:
        orm_mode = True
        
class SensorCreate(BaseModel):
    name: str
    longitude: float
    latitude: float
    type: str
    mac_address: str
    manufacturer: str
    model: str
    serie_number: str
    firmware_version: str
    description: str

class SensorData(BaseModel):
    temperature: Optional[float] = None
    humidity: Optional[float] = None
    velocity: Optional[float] = None
    battery_level: float
    last_seen: str