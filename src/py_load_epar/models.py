import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class PipelineExecution(BaseModel):
    """Pydantic model for pipeline_execution table."""

    execution_id: int
    start_timestamp_utc: datetime.datetime
    end_timestamp_utc: Optional[datetime.datetime] = None
    status: str
    load_strategy: str
    source_file_version: Optional[str] = None
    records_processed: Optional[int] = None
    high_water_mark: Optional[datetime.datetime] = None


class Organization(BaseModel):
    """Pydantic model for organizations table."""

    oms_id: str
    organization_name: str
    country_code: Optional[str] = None
    last_updated: datetime.datetime = Field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc)
    )


class Substance(BaseModel):
    """Pydantic model for substances table."""

    spor_substance_id: str
    substance_name: str
    last_updated: datetime.datetime = Field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc)
    )


class EparIndex(BaseModel):
    """
    Pydantic model for the epar_index table.
    Represents the structured data for a single EPAR entry.
    """

    # Core fields from source
    epar_id: str = Field(..., max_length=100)
    medicine_name: str = Field(..., max_length=500)
    authorization_status: str = Field(..., max_length=50)
    first_authorization_date: Optional[datetime.date] = None
    withdrawal_date: Optional[datetime.date] = None
    last_update_date_source: datetime.date

    # Standard Representation (Raw values)
    active_substance_raw: Optional[str] = None
    marketing_authorization_holder_raw: Optional[str] = Field(None, max_length=500)
    therapeutic_area: str = Field(..., max_length=500)

    # Full Representation (Enriched & standardized)
    mah_oms_id: Optional[str] = Field(None, max_length=50)

    # Pipeline Metadata
    is_active: bool = True
    source_url: Optional[str] = None
    etl_load_timestamp: datetime.datetime = Field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc)
    )
    etl_execution_id: Optional[int] = None

    model_config = ConfigDict(from_attributes=True)


class EparSubstanceLink(BaseModel):
    """Pydantic model for epar_substance_link table."""

    epar_id: str
    spor_substance_id: str


class EparDocument(BaseModel):
    """Pydantic model for epar_documents table."""

    document_id: UUID
    epar_id: str
    document_type: Optional[str] = Field(None, max_length=50)
    language_code: Optional[str] = Field(None, max_length=2)
    source_url: str
    storage_location: Optional[str] = None
    file_hash: Optional[str] = Field(None, max_length=64)
    download_timestamp: Optional[datetime.datetime] = None
