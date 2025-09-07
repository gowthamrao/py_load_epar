from pydantic import BaseModel, Field


class SporOmsOrganisation(BaseModel):
    """
    A simplified Pydantic model for a single organisation from the SPOR OMS API.
    """

    org_id: str = Field(..., alias="orgId")
    name: str


class SporOmsResponse(BaseModel):
    """
    A Pydantic model for the paged response from the SPOR OMS /organisations endpoint.
    """

    items: list[SporOmsOrganisation]


class SporSmsSubstance(BaseModel):
    """
    A simplified Pydantic model for a single substance from the SPOR SMS API.
    """

    sms_id: str = Field(..., alias="smsId")
    name: str


class SporSmsResponse(BaseModel):
    """
    A Pydantic model for the paged response from the SPOR SMS /substances endpoint.
    """

    items: list[SporSmsSubstance]
