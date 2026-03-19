"""ETL importer that parses local HTML files and loads MySQL tables."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from hashlib import sha256
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import (
    CompetitionCategory,
    IngestRun,
    Program,
    ProgramRound,
    ProgramThreshold,
    RankingRowAnonymized,
    SourceFile,
    University,
)
from etl.parsers import (
    ParsedCategory,
    ParsedProgram,
    ParsedUniversity,
    parse_detail_file,
    parse_report_file,
    parse_university_index,
)


@dataclass(slots=True)
class ImportStats:
    """Summary metrics returned by one import run."""

    reports_parsed: int = 0
    reports_failed: int = 0
    programs_upserted: int = 0
    details_parsed: int = 0
    details_skipped: int = 0
    details_failed: int = 0
    ranking_rows_loaded: int = 0


def file_sha256(file_path: Path) -> str:
    """Calculate SHA-256 hash for a file."""
    digest = sha256()
    with file_path.open("rb") as source_file:
        while True:
            chunk = source_file.read(65536)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


class HTMLImporter:
    """Importer that loads reports and detailed rankings into DB."""

    def __init__(self, db: Session, site_root: Path) -> None:
        self.db = db
        self.site_root = site_root

    def import_all(self) -> ImportStats:
        """Run end-to-end import for all source files."""
        stats = ImportStats()
        ingest_run_id = self._create_ingest_run()

        report_to_university_id = parse_university_index(self.site_root / "index.html")
        detail_to_program: dict[str, int] = {}

        report_files = sorted(self.site_root.glob("reports*.html"))
        for report_file in report_files:
            relative_path = report_file.relative_to(self.site_root).as_posix()
            checksum = file_sha256(report_file)

            try:
                parsed_university, parsed_programs = parse_report_file(report_file)
                if parsed_university.external_university_id is None:
                    parsed_university.external_university_id = (
                        report_to_university_id.get(report_file.name)
                    )

                if parsed_university.external_university_id is None:
                    raise ValueError(
                        "Unable to determine external university ID"
                    )

                university = self._upsert_university(
                    parsed_university,
                    report_file.name,
                )

                for parsed_program in parsed_programs:
                    program = self._upsert_program(
                        university_id=university.id,
                        parsed_program=parsed_program,
                        source_report_file=report_file.name,
                    )
                    self._sync_thresholds(program, parsed_program)
                    self._upsert_round_from_report(program, parsed_program)

                    if parsed_program.detail_report_file:
                        detail_to_program[parsed_program.detail_report_file] = program.id

                self._record_source_file(
                    ingest_run_id=ingest_run_id,
                    relative_path=relative_path,
                    file_type="report",
                    file_hash=checksum,
                    parse_status="parsed",
                    parse_message=(
                        f"programs={len(parsed_programs)}"
                    ),
                )
                self.db.commit()

                stats.reports_parsed += 1
                stats.programs_upserted += len(parsed_programs)
            except Exception as error:  # pragma: no cover - defensive ETL safety
                self.db.rollback()
                self._record_source_file(
                    ingest_run_id=ingest_run_id,
                    relative_path=relative_path,
                    file_type="report",
                    file_hash=checksum,
                    parse_status="failed",
                    parse_message=str(error)[:2000],
                )
                self.db.commit()
                stats.reports_failed += 1

        detail_dir = self.site_root / "downloaded"
        detail_files = sorted(detail_dir.glob("personalcabinet_report_*.html"))
        for detail_file in detail_files:
            relative_path = detail_file.relative_to(self.site_root).as_posix()
            checksum = file_sha256(detail_file)
            program_id = detail_to_program.get(detail_file.name)

            if program_id is None:
                self._record_source_file(
                    ingest_run_id=ingest_run_id,
                    relative_path=relative_path,
                    file_type="detail",
                    file_hash=checksum,
                    parse_status="skipped",
                    parse_message="No program link from reports*.html",
                )
                self.db.commit()
                stats.details_skipped += 1
                continue

            try:
                parsed_detail = parse_detail_file(detail_file)
                program = self.db.get(Program, program_id)
                if program is None:
                    raise ValueError("Linked program not found")

                if (
                    parsed_detail.admission_plan is not None
                    and program.admission_plan is None
                ):
                    program.admission_plan = parsed_detail.admission_plan

                program_round = self._upsert_round_from_detail(
                    program_id=program_id,
                    detail_file=detail_file.name,
                    parsed_detail=parsed_detail,
                )

                self._replace_round_categories(
                    program_round_id=program_round.id,
                    categories=parsed_detail.categories,
                )

                loaded_rows = sum(
                    len(category.rows)
                    for category in parsed_detail.categories
                )
                stats.ranking_rows_loaded += loaded_rows

                self._record_source_file(
                    ingest_run_id=ingest_run_id,
                    relative_path=relative_path,
                    file_type="detail",
                    file_hash=checksum,
                    parse_status="parsed",
                    parse_message=(
                        f"round={parsed_detail.round_number};"
                        f"rows={loaded_rows};empty={parsed_detail.is_empty}"
                    ),
                )
                self.db.commit()
                stats.details_parsed += 1
            except Exception as error:  # pragma: no cover - defensive ETL safety
                self.db.rollback()
                self._record_source_file(
                    ingest_run_id=ingest_run_id,
                    relative_path=relative_path,
                    file_type="detail",
                    file_hash=checksum,
                    parse_status="failed",
                    parse_message=str(error)[:2000],
                )
                self.db.commit()
                stats.details_failed += 1

        self._finalize_ingest_run(ingest_run_id=ingest_run_id, stats=stats)
        self.db.commit()

        return stats

    def _create_ingest_run(self) -> int:
        """Create a new ingest run row and return its ID."""
        run = IngestRun(
            source_root=str(self.site_root),
            status="running",
        )
        self.db.add(run)
        self.db.commit()
        self.db.refresh(run)
        return int(run.id)

    def _finalize_ingest_run(self, ingest_run_id: int, stats: ImportStats) -> None:
        """Mark ingest run as finished and store summary."""
        run = self.db.get(IngestRun, ingest_run_id)
        if run is None:
            return

        run.finished_at = datetime.utcnow()
        run.status = "success" if stats.reports_failed == 0 else "failed"
        run.message = str(asdict(stats))

    def _record_source_file(
        self,
        ingest_run_id: int,
        relative_path: str,
        file_type: str,
        file_hash: str,
        parse_status: str,
        parse_message: str,
    ) -> None:
        """Insert one source file parse log row."""
        source_file = SourceFile(
            ingest_run_id=ingest_run_id,
            relative_path=relative_path,
            file_type=file_type,
            file_hash=file_hash,
            parse_status=parse_status,
            parse_message=parse_message,
        )
        self.db.add(source_file)

    def _upsert_university(
        self,
        parsed_university: ParsedUniversity,
        report_file_name: str,
    ) -> University:
        """Insert or update one university row."""
        statement = select(University).where(
            University.external_university_id
            == parsed_university.external_university_id
        )
        university = self.db.execute(statement).scalar_one_or_none()

        if university is None:
            university = University(
                external_university_id=parsed_university.external_university_id,
                name=parsed_university.name,
            )
            self.db.add(university)

        university.name = parsed_university.name or university.name
        if parsed_university.rector_name:
            university.rector_name = parsed_university.rector_name
        if parsed_university.address:
            university.address = parsed_university.address
        if parsed_university.website_url:
            university.website_url = parsed_university.website_url
        university.report_file = report_file_name

        self.db.flush()
        return university

    def _upsert_program(
        self,
        university_id: int,
        parsed_program: ParsedProgram,
        source_report_file: str,
    ) -> Program:
        """Insert or update one program row."""
        program: Program | None = None

        if parsed_program.detail_report_file:
            statement = select(Program).where(
                Program.detail_report_file == parsed_program.detail_report_file
            )
            program = self.db.execute(statement).scalar_one_or_none()

        if program is None:
            statement = select(Program).where(
                Program.university_id == university_id,
                Program.faculty_name == parsed_program.faculty_name,
                Program.program_code == parsed_program.program_code,
                Program.program_name == parsed_program.program_name,
                Program.payment_type == parsed_program.payment_type,
            )
            program = self.db.execute(statement).scalar_one_or_none()

        if program is None:
            program = Program(
                university_id=university_id,
                faculty_name=parsed_program.faculty_name,
                program_name=parsed_program.program_name,
                source_report_file=source_report_file,
            )
            self.db.add(program)

        program.university_id = university_id
        program.faculty_name = parsed_program.faculty_name
        program.program_code = parsed_program.program_code
        program.program_name = parsed_program.program_name
        program.specialization_name = parsed_program.specialization_name
        program.study_form = parsed_program.study_form
        program.payment_type = parsed_program.payment_type
        program.annual_fee_som = parsed_program.annual_fee_som
        program.admission_plan = parsed_program.admission_plan
        program.threshold_text = parsed_program.threshold_text
        program.threshold_main_score = parsed_program.threshold_main_score
        program.registered_count_reported = parsed_program.registered_count
        program.contact_file = parsed_program.contact_file
        program.source_report_file = source_report_file

        if parsed_program.detail_report_file:
            program.detail_report_file = parsed_program.detail_report_file

        self.db.flush()
        return program

    def _sync_thresholds(
        self,
        program: Program,
        parsed_program: ParsedProgram,
    ) -> None:
        """Synchronize subject-level thresholds for one program."""
        if not parsed_program.threshold_items:
            return

        threshold_items = (
            self.db.execute(
                select(ProgramThreshold).where(
                    ProgramThreshold.program_id == program.id
                )
            )
            .scalars()
            .all()
        )
        existing_by_subject = {
            threshold.subject_name: threshold for threshold in threshold_items
        }
        incoming_subjects = {
            threshold.subject_name for threshold in parsed_program.threshold_items
        }

        for item in parsed_program.threshold_items:
            existing = existing_by_subject.get(item.subject_name)
            if existing is None:
                self.db.add(
                    ProgramThreshold(
                        program_id=program.id,
                        subject_name=item.subject_name,
                        min_score=item.min_score,
                    )
                )
            else:
                existing.min_score = item.min_score

        for subject_name, existing in existing_by_subject.items():
            if subject_name not in incoming_subjects:
                self.db.delete(existing)

    def _upsert_round_from_report(
        self,
        program: Program,
        parsed_program: ParsedProgram,
    ) -> ProgramRound:
        """Create/update round row using report-level registered count."""
        statement = select(ProgramRound).where(
            ProgramRound.program_id == program.id,
            ProgramRound.round_number == parsed_program.round_number,
        )
        program_round = self.db.execute(statement).scalar_one_or_none()

        if program_round is None:
            program_round = ProgramRound(
                program_id=program.id,
                round_number=parsed_program.round_number,
            )
            self.db.add(program_round)

        if parsed_program.registered_count is not None:
            program_round.registered_count = parsed_program.registered_count

        self.db.flush()
        return program_round

    def _upsert_round_from_detail(
        self,
        program_id: int,
        detail_file: str,
        parsed_detail,
    ) -> ProgramRound:
        """Create/update round row using detailed file summary."""
        statement = select(ProgramRound).where(
            ProgramRound.program_id == program_id,
            ProgramRound.round_number == parsed_detail.round_number,
        )
        program_round = self.db.execute(statement).scalar_one_or_none()

        if program_round is None:
            program_round = ProgramRound(
                program_id=program_id,
                round_number=parsed_detail.round_number,
            )
            self.db.add(program_round)

        if parsed_detail.registered_total is not None:
            program_round.registered_count = parsed_detail.registered_total
        if parsed_detail.admitted_confirmed is not None:
            program_round.admitted_confirmed = parsed_detail.admitted_confirmed
        if parsed_detail.recommended_count is not None:
            program_round.recommended_count = parsed_detail.recommended_count
        if parsed_detail.vacancies_total is not None:
            program_round.vacancies_total = parsed_detail.vacancies_total

        program_round.summary_text = parsed_detail.summary_text
        program_round.source_detail_file = detail_file

        self.db.flush()
        return program_round

    def _replace_round_categories(
        self,
        program_round_id: int,
        categories: list[ParsedCategory],
    ) -> None:
        """Replace all categories and rows for the target round."""
        existing_statement = select(CompetitionCategory).where(
            CompetitionCategory.program_round_id == program_round_id
        )
        existing_categories = self.db.execute(existing_statement).scalars().all()
        for category in existing_categories:
            self.db.delete(category)
        self.db.flush()

        for parsed_category in categories:
            category = CompetitionCategory(
                program_round_id=program_round_id,
                category_name=parsed_category.category_name,
                cutoff_value=parsed_category.cutoff_value,
                rows_count=len(parsed_category.rows),
            )
            self.db.add(category)
            self.db.flush()

            for row in parsed_category.rows:
                ranking_row = RankingRowAnonymized(
                    competition_category_id=category.id,
                    rank_position=row.rank_position,
                    primary_score=row.primary_score,
                    additional_score=row.additional_score,
                    total_score=row.total_score,
                    registration_datetime=row.registration_datetime,
                    is_recommended=row.is_recommended,
                    recommendation_note=row.recommendation_note,
                    row_fingerprint=row.row_fingerprint,
                )
                self.db.add(ranking_row)
