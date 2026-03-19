"""HTML parsers for report and detailed ranking pages."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from urllib.parse import parse_qs, urlparse
import re

from bs4 import BeautifulSoup


@dataclass(slots=True)
class ThresholdItem:
    """Subject-level threshold requirement."""

    subject_name: str
    min_score: int


@dataclass(slots=True)
class ParsedUniversity:
    """University-level data extracted from a report page."""

    external_university_id: int | None
    name: str
    rector_name: str | None
    address: str | None
    website_url: str | None


@dataclass(slots=True)
class ParsedProgram:
    """Program-level row extracted from a report card."""

    faculty_name: str
    program_code: str | None
    program_name: str
    specialization_name: str | None
    study_form: str | None
    payment_type: str | None
    annual_fee_som: int | None
    admission_plan: int | None
    threshold_text: str | None
    threshold_main_score: int | None
    threshold_items: list[ThresholdItem]
    registered_count: int | None
    round_number: int
    detail_report_file: str | None
    contact_file: str | None


@dataclass(slots=True)
class ParsedRankingRow:
    """An anonymized ranking row from a detailed page."""

    rank_position: int
    primary_score: int | None
    additional_score: int | None
    total_score: int | None
    registration_datetime: datetime | None
    is_recommended: bool
    recommendation_note: str | None
    row_fingerprint: str


@dataclass(slots=True)
class ParsedCategory:
    """One competition category table on a detailed page."""

    category_name: str
    cutoff_value: float | None
    rows: list[ParsedRankingRow] = field(default_factory=list)


@dataclass(slots=True)
class ParsedDetailReport:
    """Detailed page payload linked to one program."""

    file_name: str
    round_number: int
    program_title: str | None
    admission_plan: int | None
    recommended_count: int | None
    admitted_confirmed: int | None
    vacancies_total: int | None
    registered_total: int | None
    summary_text: str | None
    is_empty: bool
    categories: list[ParsedCategory] = field(default_factory=list)


def clean_text(value: str) -> str:
    """Collapse whitespace and normalize nbsp-like separators."""
    if not value:
        return ""
    value = value.replace("\xa0", " ")
    return re.sub(r"\s+", " ", value).strip()


def parse_int(value: str) -> int | None:
    """Parse first integer from a text fragment."""
    normalized = clean_text(value)
    match = re.search(r"-?\d+", normalized)
    if not match:
        return None
    return int(match.group(0))


def parse_float(value: str) -> float | None:
    """Parse first float from a text fragment."""
    normalized = clean_text(value).replace(",", ".")
    match = re.search(r"-?\d+(?:\.\d+)?", normalized)
    if not match:
        return None
    return float(match.group(0))


def parse_datetime(value: str) -> datetime | None:
    """Parse registration datetime in DD.MM.YYYY HH:MM:SS format."""
    normalized = clean_text(value)
    if not normalized:
        return None
    try:
        return datetime.strptime(normalized, "%d.%m.%Y %H:%M:%S")
    except ValueError:
        return None


def extract_round_number(value: str) -> int | None:
    """Extract round number from strings like "1-тур"."""
    normalized = clean_text(value)
    patterns = [
        r"(\d+)\s*[-–]\s*тур",
        r"(\d+)\s*тур",
    ]
    for pattern in patterns:
        match = re.search(pattern, normalized, flags=re.IGNORECASE)
        if not match:
            continue
        number = int(match.group(1))
        if 1 <= number <= 10:
            return number
    return None


def parse_main_threshold(value: str) -> int | None:
    """Extract main threshold score from threshold text."""
    normalized = clean_text(value)
    patterns = [
        r"Негизги\s*балл\s*-\s*(\d+)",
        r"Основн\w*\s*балл\s*-\s*(\d+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, normalized, flags=re.IGNORECASE)
        if match:
            return int(match.group(1))

    fallback = re.search(r"(\d+)", normalized)
    if fallback:
        return int(fallback.group(1))
    return None


def parse_threshold_items(value: str) -> list[ThresholdItem]:
    """Extract all subject-score pairs from threshold text."""
    normalized = clean_text(value)
    pairs = re.findall(r"([A-Za-zА-Яа-яЁё.\s]+?)-\s*(\d+)", normalized)

    result: list[ThresholdItem] = []
    seen: set[str] = set()
    for raw_subject, raw_score in pairs:
        subject = clean_text(raw_subject)
        if not subject:
            continue
        lowered_subject = subject.lower()
        if lowered_subject.startswith("сабак "):
            subject = clean_text(subject[6:])
            lowered_subject = subject.lower()
        if (
            lowered_subject.startswith("негизги")
            or lowered_subject.startswith("основ")
            or lowered_subject.startswith("кошумча")
            or lowered_subject.startswith("доп")
        ):
            continue
        if subject in seen:
            continue
        result.append(ThresholdItem(subject_name=subject, min_score=int(raw_score)))
        seen.add(subject)
    return result


def parse_program_name_block(
    value: str,
) -> tuple[str, str | None, str | None]:
    """Split program text into name, specialization and study form."""
    normalized = clean_text(value)
    normalized = re.sub(r"\b\d[\d\s]*\s*сом\b", "", normalized, flags=re.I)
    normalized = clean_text(normalized)

    match = re.search(r"^(.*?)\s*\[(.*?)\]\s*\((.*?)\)", normalized)
    if not match:
        return normalized, None, None

    program_name = clean_text(match.group(1))
    specialization_name = clean_text(match.group(2))
    study_form = clean_text(match.group(3))
    return program_name, specialization_name, study_form


def parse_university_index(index_file: Path) -> dict[str, int]:
    """Parse index.html to map report file names to external university IDs."""
    if not index_file.exists():
        return {}

    soup = BeautifulSoup(index_file.read_text("utf-8", errors="ignore"), "lxml")
    mapping: dict[str, int] = {}

    for link in soup.select("a[href*='reports']"):
        href = link.get("href")
        if not href:
            continue

        parsed = urlparse(href)
        report_file = Path(parsed.path).name
        query = parse_qs(parsed.query)
        id_values = query.get("id_university")
        if not report_file or not id_values:
            continue

        try:
            mapping[report_file] = int(id_values[0])
        except ValueError:
            continue

    return mapping


def parse_report_file(file_path: Path) -> tuple[ParsedUniversity, list[ParsedProgram]]:
    """Parse one `reports*.html` file with university and program rows."""
    soup = BeautifulSoup(file_path.read_text("utf-8", errors="ignore"), "lxml")

    external_university_id: int | None = None
    for link in soup.select("a[href*='id_university=']"):
        href = link.get("href", "")
        match = re.search(r"id_university=(\d+)", href)
        if match:
            external_university_id = int(match.group(1))
            break

    university_name = ""
    title_tag = soup.select_one("h2.title_text")
    if title_tag:
        university_name = clean_text(title_tag.get_text(" ", strip=True))

    rector_name: str | None = None
    address: str | None = None
    for info_tag in soup.select("p.brown_text"):
        text = clean_text(info_tag.get_text(" ", strip=True))
        if not text:
            continue
        if "Ректор" in text and ":" in text:
            rector_name = clean_text(text.split(":", 1)[1])
        if "Адрес" in text and ":" in text:
            address = clean_text(text.split(":", 1)[1])

    website_url: str | None = None
    for link in soup.select("a[href]"):
        label = clean_text(link.get_text(" ", strip=True))
        href = link.get("href", "")
        if "Сайт" in label and href:
            website_url = href
            break

    parsed_university = ParsedUniversity(
        external_university_id=external_university_id,
        name=university_name,
        rector_name=rector_name,
        address=address,
        website_url=website_url,
    )

    programs: list[ParsedProgram] = []

    for card in soup.select("li.card-item"):
        faculty_name = ""
        faculty_tag = card.select_one("p.university-name.color-blue")
        if faculty_tag:
            faculty_name = clean_text(faculty_tag.get_text(" ", strip=True))
        faculty_name = re.sub(r"^\d+\.\s*", "", faculty_name)

        round_number = 1
        for header_cell in card.select("div.cell.plan.sm-text.opacity-5"):
            round_number_candidate = extract_round_number(
                header_cell.get_text(" ", strip=True)
            )
            if round_number_candidate is not None:
                round_number = round_number_candidate
                break

        for row_block in card.select("div.rows div.d-lg-flex"):
            value_container = None
            for container in row_block.select(":scope > div.d-flex"):
                if len(container.select("div.cell")) >= 6:
                    value_container = container
                    break

            if value_container is None:
                continue

            cells = value_container.select("div.cell")
            if len(cells) < 6:
                continue

            program_code = clean_text(cells[0].get_text(" ", strip=True)) or None
            program_text = clean_text(cells[1].get_text(" ", strip=True))
            payment_type = clean_text(cells[2].get_text(" ", strip=True)) or None
            annual_fee_som = parse_int(cells[3].get_text(" ", strip=True))
            admission_plan = parse_int(cells[4].get_text(" ", strip=True))
            threshold_text = clean_text(cells[5].get_text(" ", strip=True)) or None

            registered_count: int | None = None
            if len(cells) >= 7:
                registered_count = parse_int(cells[6].get_text(" ", strip=True))

            program_name, specialization_name, study_form = (
                parse_program_name_block(program_text)
            )

            detail_report_file: str | None = None
            detail_tag = row_block.select_one(
                "a[href*='personalcabinet_report_']"
            )
            if detail_tag and detail_tag.get("href"):
                detail_report_file = Path(detail_tag["href"]).name

            contact_file: str | None = None
            contact_tag = row_block.select_one("a[href*='contact_u-']")
            if contact_tag and contact_tag.get("href"):
                contact_file = Path(contact_tag["href"]).name

            program = ParsedProgram(
                faculty_name=faculty_name,
                program_code=program_code,
                program_name=program_name,
                specialization_name=specialization_name,
                study_form=study_form,
                payment_type=payment_type,
                annual_fee_som=annual_fee_som,
                admission_plan=admission_plan,
                threshold_text=threshold_text,
                threshold_main_score=(
                    parse_main_threshold(threshold_text or "")
                    if threshold_text
                    else None
                ),
                threshold_items=(
                    parse_threshold_items(threshold_text)
                    if threshold_text
                    else []
                ),
                registered_count=registered_count,
                round_number=round_number,
                detail_report_file=detail_report_file,
                contact_file=contact_file,
            )
            programs.append(program)

    return parsed_university, programs


def parse_detail_file(file_path: Path) -> ParsedDetailReport:
    """Parse one detailed `personalcabinet_report_*.html` file."""
    raw_html = file_path.read_text("utf-8", errors="ignore")

    if "Нет зарегистрированных абитуриентов" in raw_html:
        return ParsedDetailReport(
            file_name=file_path.name,
            round_number=1,
            program_title=None,
            admission_plan=None,
            recommended_count=None,
            admitted_confirmed=None,
            vacancies_total=None,
            registered_total=0,
            summary_text="Нет зарегистрированных абитуриентов.",
            is_empty=True,
            categories=[],
        )

    soup = BeautifulSoup(raw_html, "lxml")

    title_tag = soup.select_one("p.text-center.headerColor")
    program_title = (
        clean_text(title_tag.get_text(" ", strip=True)) if title_tag else None
    )

    header_lines = [
        clean_text(tag.get_text(" ", strip=True))
        for tag in soup.select("p.text-center.headerColor")
    ]

    admission_plan: int | None = None
    recommended_count: int | None = None
    admitted_confirmed: int | None = None
    vacancies_total: int | None = None
    registered_total: int | None = None
    summary_text: str | None = None

    if len(header_lines) >= 2:
        admission_plan = parse_int(header_lines[1])

    if len(header_lines) >= 3:
        line = header_lines[2]
        rec_match = re.search(r"Рекоменд\w*\s*:?\s*(\d+)", line, re.I)
        conf_match = re.search(r"Подтвержд\w*\s*:?\s*(\d+)", line, re.I)
        if rec_match:
            recommended_count = int(rec_match.group(1))
        if conf_match:
            admitted_confirmed = int(conf_match.group(1))

    if len(header_lines) >= 4:
        summary_text = header_lines[3]
        vac_match = re.search(r"вакант\w*\s+мест\s*:?\s*(\d+)", summary_text, re.I)
        reg_match = re.search(
            r"Зарегистрирован\w*\s*:?\s*(\d+)", summary_text, re.I
        )
        if vac_match:
            vacancies_total = int(vac_match.group(1))
        if reg_match:
            registered_total = int(reg_match.group(1))

    round_number = extract_round_number(summary_text or "")
    if round_number is None:
        text_secondary = soup.select_one("p.text-center.text-secondary")
        round_number = extract_round_number(
            text_secondary.get_text(" ", strip=True) if text_secondary else ""
        )
    if round_number is None:
        round_number = 1

    categories: list[ParsedCategory] = []

    for table in soup.select("table.table"):
        header_tag = table.select_one("thead tr th[colspan]")
        raw_category = (
            clean_text(header_tag.get_text(" ", strip=True))
            if header_tag
            else "Общий конкурс"
        )

        category_name = raw_category
        cutoff_value: float | None = None
        if ":" in raw_category:
            left, right = raw_category.split(":", 1)
            category_name = clean_text(left)
            cutoff_value = parse_float(right)

        rows: list[ParsedRankingRow] = []
        for row_tag in table.select("tbody tr"):
            rank_tag = row_tag.select_one("th")
            rank_position = parse_int(
                rank_tag.get_text(" ", strip=True) if rank_tag else ""
            )
            if rank_position is None:
                continue

            cells = row_tag.select("td")
            if len(cells) < 5:
                continue

            certificate_text = clean_text(cells[0].get_text(" ", strip=True))
            primary_score = parse_int(cells[1].get_text(" ", strip=True))
            additional_score = parse_int(cells[2].get_text(" ", strip=True))
            total_score = parse_int(cells[3].get_text(" ", strip=True))
            registration_datetime = parse_datetime(
                cells[4].get_text(" ", strip=True)
            )

            note_match = re.search(r"\[(.*?)\]", certificate_text)
            recommendation_note = (
                clean_text(note_match.group(1)) if note_match else None
            )

            is_recommended = "Реком" in certificate_text

            fingerprint_source = "|".join(
                [
                    file_path.name,
                    category_name,
                    str(rank_position),
                    str(primary_score),
                    str(additional_score),
                    str(total_score),
                    registration_datetime.isoformat()
                    if registration_datetime
                    else "",
                    "1" if is_recommended else "0",
                ]
            )
            row_fingerprint = sha256(fingerprint_source.encode("utf-8")).hexdigest()

            rows.append(
                ParsedRankingRow(
                    rank_position=rank_position,
                    primary_score=primary_score,
                    additional_score=additional_score,
                    total_score=total_score,
                    registration_datetime=registration_datetime,
                    is_recommended=is_recommended,
                    recommendation_note=recommendation_note,
                    row_fingerprint=row_fingerprint,
                )
            )

        categories.append(
            ParsedCategory(
                category_name=category_name or "Общий конкурс",
                cutoff_value=cutoff_value,
                rows=rows,
            )
        )

    return ParsedDetailReport(
        file_name=file_path.name,
        round_number=round_number,
        program_title=program_title,
        admission_plan=admission_plan,
        recommended_count=recommended_count,
        admitted_confirmed=admitted_confirmed,
        vacancies_total=vacancies_total,
        registered_total=registered_total,
        summary_text=summary_text,
        is_empty=False,
        categories=categories,
    )
