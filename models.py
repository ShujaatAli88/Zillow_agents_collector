from pydantic import BaseModel, Field, validator
from typing import Optional
import re

class AgentDataModel(BaseModel):
    agent_name: Optional[str] = Field(alias="Agent Name")
    agent_email: Optional[str] = Field(alias="Agent Email")
    business_name: Optional[str] = Field(alias="Bussiness Name")
    agent_phone: Optional[str] = Field(alias="Agent Phone")
    brokerage_phone: Optional[str] = Field(alias="Brokerage Phone")
    agent_license: Optional[str] = Field(alias="Agent License")
    license_status: Optional[str] = Field(alias="License Status")
    license_type: Optional[str] = Field(alias="License Type")
    license_expiration_date: Optional[str] = Field(alias="License Expiration Date")

    address_1: Optional[str] = Field(alias="Address_1")
    address_2: Optional[str] = Field(alias="Address_2")
    city: Optional[str] = Field(alias="City")
    state: Optional[str] = Field(alias="State")
    postal_code: Optional[str] = Field(alias="Postal Code")

    total_sales: Optional[int] = Field(alias="Total Sales")
    total_sales_last_12_months: Optional[int] = Field(alias="Total Sales Last 12 Months")

    minimum_price_range: Optional[float] = Field(alias="Minimum Price Range")
    maximum_price_range: Optional[float] = Field(alias="Maximum Price Range")
    average_price_range: Optional[float] = Field(alias="Average Price Range")

    @validator('minimum_price_range', 'maximum_price_range', 'average_price_range', pre=True)
    def clean_currency(cls, value):
        """Remove $ and commas and convert to float."""
        if isinstance(value, str):
            return float(re.sub(r'[^\d.]', '', value))
        return value

    @validator('total_sales', 'total_sales_last_12_months', pre=True)
    def parse_int(cls, value):
        if value is None:
            return None
        return int(value)
