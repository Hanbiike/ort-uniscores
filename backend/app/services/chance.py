"""Rule-based chance evaluation service."""

from sqlalchemy import Select, desc, select
from sqlalchemy.orm import Session

from app.models import (
    ChanceSnapshot,
    CompetitionCategory,
    Program,
    ProgramRound,
    RankingRowAnonymized,
)
from app.schemas import ChanceRequest, ChanceResponse


def _classify_chance(
    total_score: int,
    estimated_rank: int,
    admission_plan: int,
    threshold_main_score: int | None,
    current_cutoff_score: int | None,
    has_observations: bool,
) -> str:
    """Classify chance level using explicit deterministic rules."""
    if threshold_main_score is not None and total_score < threshold_main_score:
        return "low"

    if not has_observations:
        return "medium"

    high_border = max(int(admission_plan * 0.8), 1)
    medium_border = max(int(admission_plan * 1.1), 1)

    if estimated_rank <= high_border:
        return "high"

    if estimated_rank <= medium_border:
        return "medium"

    if current_cutoff_score is not None and total_score >= current_cutoff_score:
        return "medium"

    return "low"


def _build_explanation(
    total_score: int,
    estimated_rank: int,
    admission_plan: int,
    threshold_main_score: int | None,
    current_cutoff_score: int | None,
    category_name: str | None,
    has_observations: bool,
) -> str:
    """Build a user-readable explanation of the rule output."""
    lines: list[str] = []
    lines.append(
        f"Оценочный ранг: {estimated_rank}. "
        f"План набора: {admission_plan}."
    )

    if threshold_main_score is not None:
        lines.append(
            f"Порог по основному баллу: {threshold_main_score}. "
            f"Ваш суммарный балл: {total_score}."
        )

    if current_cutoff_score is not None:
        lines.append(
            f"Текущая граница по данным ранжирования: {current_cutoff_score}."
        )

    if category_name:
        lines.append(f"Расчет выполнен по категории: {category_name}.")
    else:
        lines.append("Расчет выполнен по всем доступным категориям конкурса.")

    if not has_observations:
        lines.append(
            "Для выбранного фильтра нет фактических строк ранжирования; "
            "применена консервативная оценка."
        )

    return " ".join(lines)


def evaluate_chance(db: Session, payload: ChanceRequest) -> ChanceResponse:
    """Evaluate admission chance and store a trace snapshot."""
    program = db.get(Program, payload.program_id)
    if program is None:
        raise ValueError("Program not found")

    round_stmt: Select[tuple[ProgramRound]] = select(ProgramRound).where(
        ProgramRound.program_id == payload.program_id,
        ProgramRound.round_number == payload.round_number,
    )
    program_round = db.execute(round_stmt).scalar_one_or_none()
    if program_round is None:
        raise ValueError("Round not found for selected program")

    category_stmt = select(CompetitionCategory.id).where(
        CompetitionCategory.program_round_id == program_round.id
    )
    if payload.category_name:
        category_stmt = category_stmt.where(
            CompetitionCategory.category_name.ilike(payload.category_name)
        )

    category_ids = [row[0] for row in db.execute(category_stmt).all()]

    scores: list[int] = []
    if category_ids:
        score_stmt = (
            select(RankingRowAnonymized.total_score)
            .where(
                RankingRowAnonymized.competition_category_id.in_(category_ids),
                RankingRowAnonymized.total_score.is_not(None),
            )
            .order_by(desc(RankingRowAnonymized.total_score))
        )
        scores = [int(row[0]) for row in db.execute(score_stmt).all()]

    admission_plan = (
        program.admission_plan
        or program_round.vacancies_total
        or program_round.registered_count
        or max(len(scores), 1)
    )
    admission_plan = max(admission_plan, 1)

    higher_scores = sum(1 for score in scores if score > payload.total_score)
    estimated_rank = higher_scores + 1

    current_cutoff_score: int | None = None
    if scores:
        cutoff_index = min(admission_plan - 1, len(scores) - 1)
        current_cutoff_score = scores[cutoff_index]

    chance_level = _classify_chance(
        total_score=payload.total_score,
        estimated_rank=estimated_rank,
        admission_plan=admission_plan,
        threshold_main_score=program.threshold_main_score,
        current_cutoff_score=current_cutoff_score,
        has_observations=bool(scores),
    )

    explanation = _build_explanation(
        total_score=payload.total_score,
        estimated_rank=estimated_rank,
        admission_plan=admission_plan,
        threshold_main_score=program.threshold_main_score,
        current_cutoff_score=current_cutoff_score,
        category_name=payload.category_name,
        has_observations=bool(scores),
    )

    snapshot = ChanceSnapshot(
        program_round_id=program_round.id,
        category_name=payload.category_name,
        input_total_score=payload.total_score,
        estimated_rank=estimated_rank,
        admission_plan=admission_plan,
        threshold_main_score=program.threshold_main_score,
        current_cutoff_score=current_cutoff_score,
        chance_level=chance_level,
        explanation=explanation,
    )
    db.add(snapshot)
    db.commit()

    return ChanceResponse(
        program_id=payload.program_id,
        round_number=payload.round_number,
        chance_level=chance_level,
        estimated_rank=estimated_rank,
        admission_plan=admission_plan,
        threshold_main_score=program.threshold_main_score,
        current_cutoff_score=current_cutoff_score,
        explanation=explanation,
    )
