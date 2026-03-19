"""FastAPI application for admissions aggregation API."""

from datetime import datetime, timezone

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Program, ProgramRound, University
from app.schemas import (
    ChanceRequest,
    ChanceResponse,
    HealthResponse,
    ProgramOut,
    ProgramRoundOut,
    UniversityOut,
)
from app.services.chance import evaluate_chance

app = FastAPI(
    title="ORT Admissions Aggregator API",
    version="0.1.0",
    description="API for universities/programs and rule-based admission chance",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    """Health check endpoint."""
    return HealthResponse(status="ok", timestamp=datetime.now(timezone.utc))


@app.get("/universities", response_model=list[UniversityOut])
def list_universities(
    q: str | None = Query(default=None, min_length=1),
    limit: int = Query(default=100, ge=1, le=500),
    db: Session = Depends(get_db),
) -> list[UniversityOut]:
    """List universities with optional name search."""
    stmt: Select[tuple[University]] = select(University)
    if q:
        stmt = stmt.where(University.name.ilike(f"%{q}%"))

    stmt = stmt.order_by(University.name).limit(limit)
    return list(db.execute(stmt).scalars().all())


@app.get("/universities/{university_id}/programs", response_model=list[ProgramOut])
def list_programs_by_university(
    university_id: int,
    q: str | None = Query(default=None, min_length=1),
    limit: int = Query(default=200, ge=1, le=1000),
    db: Session = Depends(get_db),
) -> list[ProgramOut]:
    """List programs for a specific university."""
    university = db.get(University, university_id)
    if university is None:
        raise HTTPException(status_code=404, detail="University not found")

    stmt: Select[tuple[Program]] = select(Program).where(
        Program.university_id == university_id
    )
    if q:
        stmt = stmt.where(Program.program_name.ilike(f"%{q}%"))

    stmt = stmt.order_by(Program.program_name).limit(limit)
    return list(db.execute(stmt).scalars().all())


@app.get("/programs/{program_id}/rounds", response_model=list[ProgramRoundOut])
def list_program_rounds(
    program_id: int,
    db: Session = Depends(get_db),
) -> list[ProgramRoundOut]:
    """List available rounds for a program."""
    program = db.get(Program, program_id)
    if program is None:
        raise HTTPException(status_code=404, detail="Program not found")

    stmt: Select[tuple[ProgramRound]] = (
        select(ProgramRound)
        .where(ProgramRound.program_id == program_id)
        .order_by(ProgramRound.round_number)
    )
    return list(db.execute(stmt).scalars().all())


@app.post("/chance/evaluate", response_model=ChanceResponse)
def evaluate_admission_chance(
    payload: ChanceRequest,
    db: Session = Depends(get_db),
) -> ChanceResponse:
    """Evaluate admission chance for the provided score and program."""
    try:
        return evaluate_chance(db, payload)
    except ValueError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
