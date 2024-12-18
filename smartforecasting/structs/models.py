from datetime import date
from datetime import datetime
from typing import List

from pydantic import BaseModel
from pydantic import validator

from structs.enums import ForecastModel
from structs.enums import PeriodType


class Period(BaseModel):
    type: PeriodType
    value: float | int


class DataSourceInfo(BaseModel):
    name: str
    period: Period


class Training(BaseModel):
    models: List[ForecastModel]


class DataSource(BaseModel):
    id: int
    datasource_info: DataSourceInfo
    training: Training
    initialized: bool
    trained: bool


class DataPoint(BaseModel):
    ts: date | datetime
    value: float | int

    @validator("ts")
    def convert_to_sql_datetime(cls, value):
        if isinstance(value, datetime) or isinstance(value, date):
            return datetime.strptime(
                value.strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S"
            )
        return value


class ForecastingData(BaseModel):
    date: date | datetime
    steps: int

    @validator("date")
    def convert_to_sql_datetime(cls, value):
        if isinstance(value, datetime) or isinstance(value, date):
            return datetime.strptime(
                value.strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S"
            )
        return value
