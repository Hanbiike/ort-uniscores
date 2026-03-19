"""Pydantic schemas for API I/O."""

from datetime import datetime

from pydantic import BaseModel, Field


class UniversityOut(BaseModel):
    """University response model."""

    id: int
    external_university_id: int
    name: str
    rector_name: str | None
    address: str | None
    website_url: str | None

    model_config = {"from_attributes": True}


class ProgramOut(BaseModel):
    """Program response model."""

    id: int
    university_id: int
    faculty_name: str
    program_code: str | None
    program_name: str
    specialization_name: str | None
    study_form: str | None
    payment_type: str | None
    annual_fee_som: int | None
    admission_plan: int | None
    threshold_main_score: int | None
    registered_count_reported: int | None

    model_config = {"from_attributes": True}


class ProgramRoundOut(BaseModel):
    """Program round summary response model."""

    id: int
    program_id: int
    round_number: int
    registered_count: int | None
    admitted_confirmed: int | None
    recommended_count: int | None
    vacancies_total: int | None
    summary_text: str | None

    model_config = {"from_attributes": True}


class ChanceRequest(BaseModel):
    """Request payload for chance evaluation."""

    program_id: int = Field(gt=0)
    total_score: int = Field(ge=0)
    round_number: int = Field(default=1, gt=0)
    category_name: str | None = None


class ChanceResponse(BaseModel):
    """Response payload with evaluation details."""

    program_id: int
    round_number: int
    chance_level: str
    estimated_rank: int
    admission_plan: int
    threshold_main_score: int | None
    current_cutoff_score: int | None
    explanation: str


class HealthResponse(BaseModel):
    """Health check response model."""

    status: str
    timestamp: datetime
