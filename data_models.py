from typing import List
from pydantic import BaseModel, HttpUrl

class Season(BaseModel):
    season: str
    url: HttpUrl


class SeasonTable(BaseModel):
    Seasons: List[Season]


class MRData(BaseModel):
    xmlns: str
    series: str
    url: HttpUrl
    limit: str
    offset: str
    total: str
    SeasonTable: SeasonTable


class Root(BaseModel):
    MRData: MRData