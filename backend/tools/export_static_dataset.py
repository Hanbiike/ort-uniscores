"""Export admissions data from MySQL into a static frontend dataset."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import sys
from typing import Any, Dict, List, Set

from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config import settings
from app.database import build_engine


SCORE_TYPES = ("primary", "additional", "total")
TWO_SUBJECT_ADDITIONAL_THRESHOLD = 150


def to_int(value: Any) -> int | None:
    """Convert DB values to int when possible."""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def to_float(value: Any) -> float | None:
    """Convert DB values to float when possible."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def fetch_rows(connection, query: str) -> List[Dict[str, Any]]:
    """Execute SQL and return row mappings as dictionaries."""
    result = connection.execute(text(query))
    return [dict(row._mapping) for row in result]


def init_score_buckets() -> Dict[str, List[int]]:
    """Create empty score buckets for all score types."""
    return {score_type: [] for score_type in SCORE_TYPES}


def extend_score_buckets(
    target: Dict[str, List[int]],
    source: Dict[str, List[int]],
) -> None:
    """Append source scores to target buckets for each score type."""
    for score_type in SCORE_TYPES:
        target[score_type].extend(source.get(score_type, []))


def compute_median(scores: List[int]) -> float | int | None:
    """Compute median for integer scores."""
    if not scores:
        return None

    ordered = sorted(scores)
    middle = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[middle]

    return round((ordered[middle - 1] + ordered[middle]) / 2, 2)


def compute_average(scores: List[int]) -> float | None:
    """Compute average score rounded to 2 decimals."""
    if not scores:
        return None
    return round(sum(scores) / len(scores), 2)


def build_metric_stats(
    scores: List[int],
    recommended_scores: List[int],
) -> Dict[str, Any]:
    """Build score summary for one score type."""
    participants_count = len(scores)
    recommended_count = len(recommended_scores)
    passed_scores = recommended_scores

    return {
        "participants_count": participants_count,
        "recommended_count": recommended_count,
        "lower_passing_score": (
            min(recommended_scores) if recommended_count else None
        ),
        "average_score": compute_average(passed_scores),
        "median_score": compute_median(passed_scores),
        "max_score": max(passed_scores) if recommended_count else None,
    }


def build_score_stats(
    scores_by_type: Dict[str, List[int]],
    recommended_by_type: Dict[str, List[int]],
    has_additional: bool,
) -> Dict[str, Any]:
    """Build grouped score stats for primary/additional/total scores."""
    primary_stats = build_metric_stats(
        scores=scores_by_type.get("primary", []),
        recommended_scores=recommended_by_type.get("primary", []),
    )

    additional_stats = None
    total_stats = None
    if has_additional:
        additional_stats = build_metric_stats(
            scores=scores_by_type.get("additional", []),
            recommended_scores=recommended_by_type.get("additional", []),
        )
        total_stats = build_metric_stats(
            scores=scores_by_type.get("total", []),
            recommended_scores=recommended_by_type.get("total", []),
        )

    return {
        "has_additional": has_additional,
        "primary": primary_stats,
        "additional": additional_stats,
        "total": total_stats,
    }


def build_recommended_only_score_stats(
    scores_by_type: Dict[str, List[float]],
    has_additional: bool,
) -> Dict[str, Any]:
    """Build score stats where the input values are already passed scores."""
    primary_scores = scores_by_type.get("primary", [])
    primary_stats = build_metric_stats(
        scores=primary_scores,
        recommended_scores=primary_scores,
    )

    additional_stats = None
    total_stats = None
    if has_additional:
        additional_scores = scores_by_type.get("additional", [])
        total_scores = scores_by_type.get("total", [])
        additional_stats = build_metric_stats(
            scores=additional_scores,
            recommended_scores=additional_scores,
        )
        total_stats = build_metric_stats(
            scores=total_scores,
            recommended_scores=total_scores,
        )

    return {
        "has_additional": has_additional,
        "primary": primary_stats,
        "additional": additional_stats,
        "total": total_stats,
    }


def sum_stat_values(left: Any, right: Any) -> float | None:
    """Return rounded sum of two metric values when both are numeric."""
    left_value = to_float(left)
    right_value = to_float(right)
    if left_value is None or right_value is None:
        return None
    return round(left_value + right_value, 2)


def build_university_total_from_aggregates(
    primary_stats: Dict[str, Any],
    additional_stats: Dict[str, Any],
) -> Dict[str, Any]:
    """Build university total as aggregate primary(all) + additional(valid)."""
    participants_count = to_int(additional_stats.get("participants_count")) or 0
    recommended_count = to_int(additional_stats.get("recommended_count")) or 0

    return {
        "participants_count": participants_count,
        "recommended_count": recommended_count,
        "lower_passing_score": sum_stat_values(
            primary_stats.get("lower_passing_score"),
            additional_stats.get("lower_passing_score"),
        ),
        "average_score": sum_stat_values(
            primary_stats.get("average_score"),
            additional_stats.get("average_score"),
        ),
        "median_score": sum_stat_values(
            primary_stats.get("median_score"),
            additional_stats.get("median_score"),
        ),
        "max_score": sum_stat_values(
            primary_stats.get("max_score"),
            additional_stats.get("max_score"),
        ),
    }


def build_score_series_payload(
    scores_by_type: Dict[str, List[float]],
    has_additional: bool,
    include_total: bool,
) -> Dict[str, Any]:
    """Build sorted score series for optional top-N frontend recalculation."""
    primary_series = sorted(
        scores_by_type.get("primary", []),
        reverse=True,
    )
    additional_series = None
    total_series = None

    if has_additional:
        additional_series = sorted(
            scores_by_type.get("additional", []),
            reverse=True,
        )
        if include_total:
            total_series = sorted(
                scores_by_type.get("total", []),
                reverse=True,
            )

    return {
        "has_additional": has_additional,
        "primary": primary_series,
        "additional": additional_series,
        "total": total_series,
    }


def build_dataset(
    university_rows: List[Dict[str, Any]],
    program_rows: List[Dict[str, Any]],
    round_rows: List[Dict[str, Any]],
    category_rows: List[Dict[str, Any]],
    threshold_rows: List[Dict[str, Any]],
    score_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Transform relational rows into a nested JSON-friendly structure."""
    round_program_by_id: Dict[int, int] = {}
    for row in round_rows:
        round_id = to_int(row.get("id"))
        program_id = to_int(row.get("program_id"))
        if round_id is None or program_id is None:
            continue
        round_program_by_id[round_id] = program_id

    category_round_by_id: Dict[int, int] = {}
    for row in category_rows:
        category_id = to_int(row.get("id"))
        round_id = to_int(row.get("program_round_id"))
        if category_id is None or round_id is None:
            continue
        category_round_by_id[category_id] = round_id

    program_university_by_id: Dict[int, int] = {}
    program_name_by_id: Dict[int, str] = {}
    program_code_by_id: Dict[int, str | None] = {}
    program_faculty_by_id: Dict[int, str | None] = {}
    program_payment_by_id: Dict[int, str | None] = {}
    for row in program_rows:
        program_id = to_int(row.get("id"))
        university_id = to_int(row.get("university_id"))
        if program_id is None or university_id is None:
            continue
        program_university_by_id[program_id] = university_id
        program_name_by_id[program_id] = row.get("program_name") or ""
        program_code_by_id[program_id] = row.get("program_code")
        program_faculty_by_id[program_id] = row.get("faculty_name")
        program_payment_by_id[program_id] = row.get("payment_type")

    programs_with_required_additional: Set[int] = set()
    for row in threshold_rows:
        program_id = to_int(row.get("program_id"))
        if program_id is None:
            continue
        programs_with_required_additional.add(program_id)

    university_name_by_id: Dict[int, str] = {}
    for row in university_rows:
        university_id = to_int(row.get("id"))
        if university_id is None:
            continue
        university_name_by_id[university_id] = row.get("name") or ""

    scores_by_category: Dict[int, Dict[str, List[int]]] = defaultdict(
        init_score_buckets
    )
    recommended_scores_by_category: Dict[
        int, Dict[str, List[int]]
    ] = defaultdict(init_score_buckets)
    has_additional_by_category: Dict[int, bool] = defaultdict(bool)
    program_additional_max: Dict[int, int] = defaultdict(int)
    ranking_rows: List[Dict[str, Any]] = []

    for row in score_rows:
        category_id = to_int(row.get("competition_category_id"))
        if category_id is None:
            continue

        round_id = category_round_by_id.get(category_id)
        program_id = round_program_by_id.get(round_id)
        university_id = program_university_by_id.get(program_id)

        primary_score = to_int(row.get("primary_score"))
        additional_score = to_int(row.get("additional_score"))
        total_score = to_int(row.get("total_score"))
        is_recommended = bool(row.get("is_recommended"))

        if program_id is not None and additional_score is not None:
            program_additional_max[program_id] = max(
                program_additional_max[program_id],
                additional_score,
            )

        row_scores = {
            "primary": primary_score,
            "additional": additional_score,
            "total": total_score,
        }
        for score_type, score_value in row_scores.items():
            if score_value is None:
                continue
            scores_by_category[category_id][score_type].append(score_value)
            if is_recommended:
                recommended_scores_by_category[category_id][score_type].append(
                    score_value
                )

        has_row_additional = (
            additional_score is not None
            and additional_score > 0
        )
        has_total_increment = (
            primary_score is not None
            and total_score is not None
            and total_score != primary_score
        )
        if has_row_additional or has_total_increment:
            has_additional_by_category[category_id] = True

        if (
            is_recommended
            and program_id is not None
            and university_id is not None
        ):
            ranking_rows.append(
                {
                    "program_id": program_id,
                    "university_id": university_id,
                    "primary_score": primary_score,
                    "additional_score": additional_score,
                    "total_score": total_score,
                }
            )

    round_scores_by_id: Dict[int, Dict[str, List[int]]] = defaultdict(
        init_score_buckets
    )
    round_recommended_by_id: Dict[int, Dict[str, List[int]]] = defaultdict(
        init_score_buckets
    )
    round_has_additional_by_id: Dict[int, bool] = defaultdict(bool)

    categories_by_round: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for row in category_rows:
        round_id = to_int(row.get("program_round_id"))
        category_id = to_int(row.get("id"))
        if round_id is None or category_id is None:
            continue

        category_scores = scores_by_category[category_id]
        category_recommended = recommended_scores_by_category[category_id]
        category_has_additional = has_additional_by_category[category_id]
        category_stats = build_score_stats(
            scores_by_type=category_scores,
            recommended_by_type=category_recommended,
            has_additional=category_has_additional,
        )

        extend_score_buckets(round_scores_by_id[round_id], category_scores)
        extend_score_buckets(
            round_recommended_by_id[round_id],
            category_recommended,
        )
        if category_has_additional:
            round_has_additional_by_id[round_id] = True

        categories_by_round[round_id].append(
            {
                "id": category_id,
                "category_name": row.get("category_name"),
                "cutoff_value": to_float(row.get("cutoff_value")),
                "rows_count": to_int(row.get("rows_count")) or 0,
                "score_stats": category_stats,
            }
        )

    for category_list in categories_by_round.values():
        category_list.sort(key=lambda item: item.get("category_name") or "")

    rounds_by_program: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for row in round_rows:
        round_id = to_int(row.get("id"))
        program_id = to_int(row.get("program_id"))
        if round_id is None or program_id is None:
            continue

        round_scores = round_scores_by_id[round_id]
        round_recommended = round_recommended_by_id[round_id]
        round_has_additional = round_has_additional_by_id[round_id]

        rounds_by_program[program_id].append(
            {
                "id": round_id,
                "round_number": to_int(row.get("round_number")) or 1,
                "registered_count": to_int(row.get("registered_count")),
                "admitted_confirmed": to_int(row.get("admitted_confirmed")),
                "recommended_count": to_int(row.get("recommended_count")),
                "vacancies_total": to_int(row.get("vacancies_total")),
                "summary_text": row.get("summary_text"),
                "direction_score_stats": build_score_stats(
                    scores_by_type=round_scores,
                    recommended_by_type=round_recommended,
                    has_additional=round_has_additional,
                ),
                "categories": categories_by_round.get(round_id, []),
            }
        )

    for round_list in rounds_by_program.values():
        round_list.sort(key=lambda item: item.get("round_number") or 1)

    programs_by_university: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    program_has_additional_by_id: Dict[int, bool] = {}
    for row in program_rows:
        program_id = to_int(row.get("id"))
        university_id = to_int(row.get("university_id"))
        if program_id is None or university_id is None:
            continue

        program_rounds = rounds_by_program.get(program_id, [])
        program_scores = init_score_buckets()
        program_recommended_scores = init_score_buckets()
        program_has_additional = False
        for round_item in program_rounds:
            round_id = to_int(round_item.get("id"))
            if round_id is None:
                continue
            extend_score_buckets(
                program_scores,
                round_scores_by_id[round_id],
            )
            extend_score_buckets(
                program_recommended_scores,
                round_recommended_by_id[round_id],
            )
            if round_has_additional_by_id[round_id]:
                program_has_additional = True

        program_has_additional_by_id[program_id] = program_has_additional

        programs_by_university[university_id].append(
            {
                "id": program_id,
                "university_id": university_id,
                "faculty_name": row.get("faculty_name"),
                "program_code": row.get("program_code"),
                "program_name": row.get("program_name"),
                "specialization_name": row.get("specialization_name"),
                "study_form": row.get("study_form"),
                "payment_type": row.get("payment_type"),
                "annual_fee_som": to_int(row.get("annual_fee_som")),
                "admission_plan": to_int(row.get("admission_plan")),
                "threshold_main_score": to_int(
                    row.get("threshold_main_score")
                ),
                "detail_report_file": row.get("detail_report_file"),
                "direction_score_stats": build_score_stats(
                    scores_by_type=program_scores,
                    recommended_by_type=program_recommended_scores,
                    has_additional=program_has_additional,
                ),
                "rounds": program_rounds,
            }
        )

    for program_list in programs_by_university.values():
        program_list.sort(
            key=lambda item: (
                item.get("faculty_name") or "",
                item.get("program_name") or "",
                item.get("payment_type") or "",
            )
        )

    universities: List[Dict[str, Any]] = []
    for row in university_rows:
        university_id = to_int(row.get("id"))
        if university_id is None:
            continue

        universities.append(
            {
                "id": university_id,
                "external_university_id": to_int(
                    row.get("external_university_id")
                ),
                "name": row.get("name"),
                "rector_name": row.get("rector_name"),
                "address": row.get("address"),
                "website_url": row.get("website_url"),
                "programs": programs_by_university.get(university_id, []),
            }
        )

    universities.sort(key=lambda item: item.get("name") or "")

    programs_count = 0
    rounds_count = 0
    categories_count = 0
    scored_rows_count = 0
    recommended_rows_count = 0

    for university in universities:
        programs = university["programs"]
        programs_count += len(programs)
        for program in programs:
            rounds = program["rounds"]
            rounds_count += len(rounds)
            for round_item in rounds:
                categories = round_item["categories"]
                categories_count += len(categories)
                for category in categories:
                    score_stats = category.get("score_stats") or {}
                    primary_stats = score_stats.get("primary") or {}
                    scored_rows_count += to_int(
                        primary_stats.get("participants_count")
                    ) or 0
                    recommended_rows_count += to_int(
                        primary_stats.get("recommended_count")
                    ) or 0

    requires_two_subjects_by_program: Dict[int, bool] = {}
    for program_id, max_additional in program_additional_max.items():
        requires_two_subjects_by_program[program_id] = (
            max_additional > TWO_SUBJECT_ADDITIONAL_THRESHOLD
        )

    direction_recommended_scores: Dict[int, Dict[str, List[float]]] = (
        defaultdict(init_score_buckets)
    )
    university_recommended_scores: Dict[int, Dict[str, List[float]]] = (
        defaultdict(init_score_buckets)
    )
    for row in ranking_rows:
        program_id = to_int(row.get("program_id"))
        university_id = to_int(row.get("university_id"))
        if program_id is None or university_id is None:
            continue

        primary_score = to_int(row.get("primary_score"))
        additional_score = to_int(row.get("additional_score"))
        total_score = to_int(row.get("total_score"))
        program_requires_additional = (
            program_id in programs_with_required_additional
        )
        requires_two_subjects = requires_two_subjects_by_program.get(
            program_id,
            False,
        )
        additional_divisor = 2 if requires_two_subjects else 1
        adjusted_additional: float | None = None

        if primary_score is not None:
            direction_recommended_scores[program_id]["primary"].append(
                float(primary_score)
            )
            university_recommended_scores[university_id]["primary"].append(
                float(primary_score)
            )

        if additional_score is not None:
            adjusted_additional = round(
                additional_score / additional_divisor,
                2,
            )
            direction_recommended_scores[program_id]["additional"].append(
                adjusted_additional
            )
            if program_requires_additional:
                university_recommended_scores[university_id][
                    "additional"
                ].append(adjusted_additional)

        adjusted_total: float | None = None
        if primary_score is not None and adjusted_additional is not None:
            adjusted_total = round(primary_score + adjusted_additional, 2)
        elif total_score is not None:
            adjusted_total = float(total_score)
        elif primary_score is not None:
            adjusted_total = float(primary_score)

        if adjusted_total is not None:
            direction_recommended_scores[program_id]["total"].append(
                adjusted_total
            )

    university_has_additional_by_id: Dict[int, bool] = defaultdict(bool)
    for university_id, university_scores in university_recommended_scores.items():
        if university_scores.get("additional"):
            university_has_additional_by_id[university_id] = True

    direction_rankings: List[Dict[str, Any]] = []
    for row in program_rows:
        program_id = to_int(row.get("id"))
        university_id = to_int(row.get("university_id"))
        if program_id is None or university_id is None:
            continue

        has_additional = program_has_additional_by_id.get(program_id, False)
        requires_two_subjects = requires_two_subjects_by_program.get(
            program_id,
            False,
        )
        direction_rankings.append(
            {
                "program_id": program_id,
                "university_id": university_id,
                "university_name": university_name_by_id.get(
                    university_id,
                    "",
                ),
                "program_name": program_name_by_id.get(program_id, ""),
                "program_code": program_code_by_id.get(program_id),
                "faculty_name": program_faculty_by_id.get(program_id),
                "payment_type": program_payment_by_id.get(program_id),
                "has_additional": has_additional,
                "requires_two_subjects_additional": requires_two_subjects,
                "additional_divisor": 2 if requires_two_subjects else 1,
                "score_stats": build_recommended_only_score_stats(
                    scores_by_type=direction_recommended_scores[program_id],
                    has_additional=has_additional,
                ),
                "score_series": build_score_series_payload(
                    scores_by_type=direction_recommended_scores[program_id],
                    has_additional=has_additional,
                    include_total=True,
                ),
            }
        )

    direction_rankings.sort(
        key=lambda item: (
            item.get("university_name") or "",
            item.get("program_name") or "",
            item.get("payment_type") or "",
        )
    )

    university_rankings: List[Dict[str, Any]] = []
    for row in university_rows:
        university_id = to_int(row.get("id"))
        if university_id is None:
            continue

        has_additional = university_has_additional_by_id.get(
            university_id,
            False,
        )
        university_stats = build_recommended_only_score_stats(
            scores_by_type=university_recommended_scores[university_id],
            has_additional=has_additional,
        )
        if has_additional:
            university_stats["total"] = build_university_total_from_aggregates(
                primary_stats=university_stats["primary"],
                additional_stats=university_stats["additional"],
            )

        university_rankings.append(
            {
                "university_id": university_id,
                "name": university_name_by_id.get(university_id, ""),
                "has_additional": has_additional,
                "score_stats": university_stats,
                "score_series": build_score_series_payload(
                    scores_by_type=university_recommended_scores[
                        university_id
                    ],
                    has_additional=has_additional,
                    include_total=False,
                ),
            }
        )

    university_rankings.sort(key=lambda item: item.get("name") or "")

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_version": 9,
        "summary": {
            "universities": len(universities),
            "programs": programs_count,
            "rounds": rounds_count,
            "categories": categories_count,
            "scored_rows": scored_rows_count,
            "recommended_rows": recommended_rows_count,
        },
        "rankings": {
            "notes": {
                "additional_two_subject_divisor": 2,
                "two_subject_rule": (
                    "requires_two_subjects_additional=true "
                    "when max additional score for a direction is greater "
                    f"than {TWO_SUBJECT_ADDITIONAL_THRESHOLD}"
                ),
                "total_score_rule": (
                    "for direction rankings, total is calculated as primary "
                    "+ adjusted additional (adjusted additional = "
                    "additional/divisor)"
                ),
                "university_rankings_scope_rule": (
                    "for university rankings, additional uses only "
                    "programs that have at least one subject in "
                    "program_thresholds"
                ),
                "university_total_aggregation_rule": (
                    "for university rankings, total is aggregated as "
                    "primary(all programs) + additional(valid programs)"
                ),
                "top_n_rule": (
                    "score_series arrays are sorted in descending order and "
                    "used by frontend for optional top-N recalculation"
                ),
            },
            "universities": university_rankings,
            "directions": direction_rankings,
        },
        "universities": universities,
    }


def build_argument_parser() -> argparse.ArgumentParser:
    """Create CLI parser for export options."""
    parser = argparse.ArgumentParser(
        description="Export MySQL data to a JSON file for static frontend"
    )
    parser.add_argument(
        "--db-url",
        type=str,
        default=settings.database_url,
        help="SQLAlchemy DSN (default from DATABASE_URL)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("../docs/data/dataset.json"),
        help="Output JSON path relative to backend directory",
    )
    return parser


def main() -> None:
    """Export dataset for static frontend from the current DB."""
    parser = build_argument_parser()
    args = parser.parse_args()

    engine = build_engine(args.db_url)
    with engine.connect() as connection:
        university_rows = fetch_rows(
            connection,
            """
            SELECT
                id,
                external_university_id,
                name,
                rector_name,
                address,
                website_url
            FROM universities
            ORDER BY name
            """,
        )
        program_rows = fetch_rows(
            connection,
            """
            SELECT
                id,
                university_id,
                faculty_name,
                program_code,
                program_name,
                specialization_name,
                study_form,
                payment_type,
                annual_fee_som,
                admission_plan,
                threshold_main_score,
                detail_report_file
            FROM programs
            ORDER BY university_id, faculty_name, program_name, payment_type
            """,
        )
        round_rows = fetch_rows(
            connection,
            """
            SELECT
                id,
                program_id,
                round_number,
                registered_count,
                admitted_confirmed,
                recommended_count,
                vacancies_total,
                summary_text
            FROM program_rounds
            ORDER BY program_id, round_number
            """,
        )
        category_rows = fetch_rows(
            connection,
            """
            SELECT
                id,
                program_round_id,
                category_name,
                cutoff_value,
                rows_count
            FROM competition_categories
            ORDER BY program_round_id, category_name
            """,
        )
        threshold_rows = fetch_rows(
            connection,
            """
            SELECT DISTINCT
                program_id
            FROM program_thresholds
            WHERE subject_name IS NOT NULL
              AND TRIM(subject_name) <> ''
            ORDER BY program_id
            """,
        )
        score_rows = fetch_rows(
            connection,
            """
            SELECT
                competition_category_id,
                primary_score,
                additional_score,
                total_score,
                is_recommended
            FROM ranking_rows_anonymized
            WHERE primary_score IS NOT NULL
            ORDER BY competition_category_id, total_score DESC, primary_score DESC
            """,
        )

    dataset = build_dataset(
        university_rows=university_rows,
        program_rows=program_rows,
        round_rows=round_rows,
        category_rows=category_rows,
        threshold_rows=threshold_rows,
        score_rows=score_rows,
    )

    output_path = args.output.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(dataset, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(
        "Static dataset exported:",
        output_path,
        "| universities:",
        dataset["summary"]["universities"],
        "| programs:",
        dataset["summary"]["programs"],
    )


if __name__ == "__main__":
    main()
