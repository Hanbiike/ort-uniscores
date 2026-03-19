"""SQLAlchemy ORM models for admissions data."""

from datetime import datetime

from sqlalchemy import (
    CHAR,
    Boolean,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class IngestRun(Base):
    """A single ETL ingestion run."""

    __tablename__ = "ingest_runs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    started_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.current_timestamp()
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    status: Mapped[str] = mapped_column(
        Enum("running", "success", "failed", name="ingest_status"),
        default="running",
    )
    source_root: Mapped[str] = mapped_column(String(1024))
    message: Mapped[str | None] = mapped_column(Text, nullable=True)

    source_files: Mapped[list["SourceFile"]] = relationship(
        back_populates="ingest_run", cascade="all, delete-orphan"
    )


class SourceFile(Base):
    """Metadata and parse status for each processed source file."""

    __tablename__ = "source_files"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    ingest_run_id: Mapped[int] = mapped_column(
        ForeignKey("ingest_runs.id", ondelete="CASCADE")
    )
    relative_path: Mapped[str] = mapped_column(String(1024))
    file_type: Mapped[str] = mapped_column(
        Enum("report", "detail", "other", name="source_file_type")
    )
    file_hash: Mapped[str | None] = mapped_column(CHAR(64), nullable=True)
    parse_status: Mapped[str] = mapped_column(
        Enum("parsed", "skipped", "failed", name="source_file_status")
    )
    parse_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.current_timestamp()
    )

    ingest_run: Mapped[IngestRun] = relationship(back_populates="source_files")


class University(Base):
    """University metadata extracted from report pages."""

    __tablename__ = "universities"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    external_university_id: Mapped[int] = mapped_column(unique=True)
    name: Mapped[str] = mapped_column(String(512))
    rector_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    address: Mapped[str | None] = mapped_column(String(512), nullable=True)
    website_url: Mapped[str | None] = mapped_column(String(512), nullable=True)
    report_file: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.current_timestamp()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        server_default=func.current_timestamp(),
        onupdate=func.current_timestamp(),
    )

    programs: Mapped[list["Program"]] = relationship(
        back_populates="university", cascade="all, delete-orphan"
    )


class Program(Base):
    """Program-level information from a university report page."""

    __tablename__ = "programs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    university_id: Mapped[int] = mapped_column(
        ForeignKey("universities.id", ondelete="CASCADE")
    )
    faculty_name: Mapped[str] = mapped_column(String(512))
    program_code: Mapped[str | None] = mapped_column(String(32), nullable=True)
    program_name: Mapped[str] = mapped_column(String(512))
    specialization_name: Mapped[str | None] = mapped_column(
        String(512), nullable=True
    )
    study_form: Mapped[str | None] = mapped_column(String(255), nullable=True)
    payment_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    annual_fee_som: Mapped[int | None] = mapped_column(Integer, nullable=True)
    admission_plan: Mapped[int | None] = mapped_column(Integer, nullable=True)
    threshold_text: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    threshold_main_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    registered_count_reported: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    detail_report_file: Mapped[str | None] = mapped_column(
        String(255), unique=True, nullable=True
    )
    contact_file: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source_report_file: Mapped[str] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.current_timestamp()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        server_default=func.current_timestamp(),
        onupdate=func.current_timestamp(),
    )

    university: Mapped[University] = relationship(back_populates="programs")
    thresholds: Mapped[list["ProgramThreshold"]] = relationship(
        back_populates="program", cascade="all, delete-orphan"
    )
    rounds: Mapped[list["ProgramRound"]] = relationship(
        back_populates="program", cascade="all, delete-orphan"
    )


class ProgramThreshold(Base):
    """Per-subject threshold requirements for a program."""

    __tablename__ = "program_thresholds"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    program_id: Mapped[int] = mapped_column(
        ForeignKey("programs.id", ondelete="CASCADE")
    )
    subject_name: Mapped[str] = mapped_column(String(128))
    min_score: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.current_timestamp()
    )

    program: Mapped[Program] = relationship(back_populates="thresholds")


class ProgramRound(Base):
    """Round-level summary for each program."""

    __tablename__ = "program_rounds"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    program_id: Mapped[int] = mapped_column(
        ForeignKey("programs.id", ondelete="CASCADE")
    )
    round_number: Mapped[int] = mapped_column(Integer)
    registered_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    admitted_confirmed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    recommended_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    vacancies_total: Mapped[int | None] = mapped_column(Integer, nullable=True)
    summary_text: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    source_detail_file: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.current_timestamp()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        server_default=func.current_timestamp(),
        onupdate=func.current_timestamp(),
    )

    program: Mapped[Program] = relationship(back_populates="rounds")
    categories: Mapped[list["CompetitionCategory"]] = relationship(
        back_populates="program_round", cascade="all, delete-orphan"
    )
    chance_snapshots: Mapped[list["ChanceSnapshot"]] = relationship(
        back_populates="program_round", cascade="all, delete-orphan"
    )


class CompetitionCategory(Base):
    """Regional or quota category within a round."""

    __tablename__ = "competition_categories"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    program_round_id: Mapped[int] = mapped_column(
        ForeignKey("program_rounds.id", ondelete="CASCADE")
    )
    category_name: Mapped[str] = mapped_column(String(255))
    cutoff_value: Mapped[float | None] = mapped_column(
        Numeric(10, 2), nullable=True
    )
    rows_count: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.current_timestamp()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        server_default=func.current_timestamp(),
        onupdate=func.current_timestamp(),
    )

    program_round: Mapped[ProgramRound] = relationship(back_populates="categories")
    ranking_rows: Mapped[list["RankingRowAnonymized"]] = relationship(
        back_populates="competition_category", cascade="all, delete-orphan"
    )


class RankingRowAnonymized(Base):
    """An anonymized ranking row without certificate number."""

    __tablename__ = "ranking_rows_anonymized"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    competition_category_id: Mapped[int] = mapped_column(
        ForeignKey("competition_categories.id", ondelete="CASCADE")
    )
    rank_position: Mapped[int] = mapped_column(Integer)
    primary_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    additional_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    total_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    registration_datetime: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    is_recommended: Mapped[bool] = mapped_column(Boolean, default=False)
    recommendation_note: Mapped[str | None] = mapped_column(
        String(255), nullable=True
    )
    row_fingerprint: Mapped[str] = mapped_column(CHAR(64), unique=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.current_timestamp()
    )

    competition_category: Mapped[CompetitionCategory] = relationship(
        back_populates="ranking_rows"
    )


class ChanceSnapshot(Base):
    """Stored output of chance evaluation for observability."""

    __tablename__ = "chance_snapshots"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    program_round_id: Mapped[int] = mapped_column(
        ForeignKey("program_rounds.id", ondelete="CASCADE")
    )
    category_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    input_total_score: Mapped[int] = mapped_column(Integer)
    estimated_rank: Mapped[int] = mapped_column(Integer)
    admission_plan: Mapped[int] = mapped_column(Integer)
    threshold_main_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    current_cutoff_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    chance_level: Mapped[str] = mapped_column(
        Enum("high", "medium", "low", name="chance_level")
    )
    explanation: Mapped[str] = mapped_column(Text)
    calculated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.current_timestamp()
    )

    program_round: Mapped[ProgramRound] = relationship(
        back_populates="chance_snapshots"
    )
