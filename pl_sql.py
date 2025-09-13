# -*- coding: utf-8 -*-
import os
import re
import json
import asyncio
import aiofiles
import aiomysql
import statistics
from bs4 import BeautifulSoup
from collections import defaultdict
from typing import Optional, Tuple

# --------- Конфиг файлов ---------
INDEX_HTML = "index.html"
REPORTS_DIR = "."
RATINGS_HTML_DIR = "downloaded"

# --------- Конфиг MySQL ---------
MYSQL_DSN = dict(
    host="127.0.0.1", port=8889,
    user="root", password="root",
    db="admissions", autocommit=True
)

FORMS = ["Бюджет", "Контракт", "Ваучер"]
SCORE_KEYS = {"main": "main_score", "extra": "extra_score", "total": "total_score"}

# --------- Утилиты ---------
def norm_space(s: Optional[str]) -> Optional[str]:
    if s is None: return None
    return re.sub(r"\s+", " ", s.replace("\n", " ")).strip()

def clean_text(tag) -> Optional[str]:
    if tag is None: return None
    return norm_space(tag.get_text(" ", strip=True) if hasattr(tag, "get_text") else str(tag))

async def read_file(path: str) -> str:
    async with aiofiles.open(path, "r", encoding="utf-8") as f:
        return await f.read()

def parse_int_safe(x: Optional[str]) -> Optional[int]:
    if not x: return None
    digits = re.sub(r"[^\d]", "", str(x))
    if not digits: return None
    try:
        return int(digits)
    except ValueError:
        return None

def parse_threshold(text: Optional[str]):
    if not text:
        return None, None, None
    main_pass = None
    req_extra = None
    extra_count = None
    m = re.search(r"Негизги балл-(\d+)", text)
    if m: main_pass = int(m.group(1))
    if "Доп. предмет не обязательно" in text:
        req_extra = False
    elif "Кошумча" in text:
        req_extra = True
    m2 = re.search(r"Кошумча\s*.-(\d+)\s*сабак", text)
    if m2: extra_count = int(m2.group(1))
    subjects = {}
    for subj, score in re.findall(r"([А-Яа-яЁёA-Za-z.\s]+)-(\d+)", text):
        subj = subj.strip()
        if subj.startswith("Негизги") or subj.startswith("Кошумча"): continue
        subjects[subj] = int(score)
    return main_pass, extra_count, subjects or None

def parse_specialty(full_text: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not full_text:
        return None, None, None
    mm = re.match(r"^(.*?)\[", full_text)
    major = mm.group(1).strip() if mm else full_text.strip()
    sm = re.search(r"\[(.*?)\]", full_text)
    specialty = sm.group(1).strip() if sm else None
    types = re.findall(r"\((.*?)\)", full_text)
    education_type = None
    if types:
        types = [t.strip() for t in types if "Ваучер" not in t]
        education_type = ", ".join(types) if types else None
    return major, specialty, education_type

def is_part_time(education_type: Optional[str]) -> int:
    if not education_type: return 0
    return 1 if re.search(r"Сырттан", education_type, flags=re.I) else 0

def parse_certificate(text: Optional[str]):
    admitted = False
    note = None
    if not text:
        return None, admitted, note
    if "(Реком" in text:
        admitted = True
        text = text.replace("(Реком)", "").strip()
    m = re.search(r"\[(.*)\]", text)
    if m:
        note = m.group(1).strip()
        text = re.sub(r"\[.*\]", "", text).strip()
    return text, admitted, note

# --------- DB helpers ---------
async def get_pool():
    return await aiomysql.create_pool(**MYSQL_DSN, maxsize=10)

async def exec_many(cur, sql, params_seq):
    for params in params_seq:
        await cur.execute(sql, params)

# Upsert University по name (TEXT) через name_hash
async def upsert_university(pool, name: str, site: Optional[str], address: Optional[str],
                            rector: Optional[str], raw_html: Optional[str]) -> int:
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            sql = """
            INSERT INTO universities(name, site, address, rector_name, raw_data, faculty_ids)
            VALUES (%s, %s, %s, %s, %s, JSON_ARRAY())
            ON DUPLICATE KEY UPDATE
              site=VALUES(site), address=VALUES(address), rector_name=VALUES(rector_name),
              raw_data=VALUES(raw_data);
            """
            await cur.execute(sql, (name, site, address, rector, raw_html))
            # получить id (LAST_INSERT_ID на дублях не меняется, поэтому запрашиваем)
            await cur.execute("SELECT id FROM universities WHERE name_hash=UNHEX(MD5(%s))", (name,))
            row = await cur.fetchone()
            return int(row[0])

async def insert_faculty(pool, name: str, raw_html: Optional[str]) -> int:
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO faculties(name, raw_data, specialty_ids) VALUES(%s,%s, JSON_ARRAY())",
                (name, raw_html)
            )
            return cur.lastrowid

async def link_university_faculty(pool, university_id: int, faculty_id: int):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT IGNORE INTO university_faculties(university_id, faculty_id) VALUES(%s,%s)",
                (university_id, faculty_id)
            )

async def insert_specialty(pool, *, code: Optional[str], specialty_name: Optional[str],
                           major_name: Optional[str], faculty_name: Optional[str],
                           university_name: Optional[str], has_contract: int,
                           has_budget: int, has_voucher: int, contract_amount: Optional[int],
                           is_part_time_flag: int, main_pass: Optional[int],
                           extra_count: Optional[int], extra_subjects: Optional[dict],
                           raw_html: Optional[str]) -> int:
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                INSERT INTO specialties (
                  code, specialty_name, major_name, faculty_name, university_name,
                  has_contract, has_budget, has_voucher, contract_amount_year,
                  is_part_time, main_pass_score, extra_pass_scores, required_extra_subjects,
                  application_ids, raw_data
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, JSON_ARRAY(), %s)
            """, (
                code, specialty_name, major_name, faculty_name, university_name,
                has_contract, has_budget, has_voucher, contract_amount,
                is_part_time_flag, main_pass,
                json.dumps(extra_subjects, ensure_ascii=False) if extra_subjects else None,
                extra_count, raw_html
            ))
            return cur.lastrowid

async def link_faculty_specialty(pool, faculty_id: int, specialty_id: int):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT IGNORE INTO faculty_specialties(faculty_id, specialty_id) VALUES(%s,%s)",
                (faculty_id, specialty_id)
            )

async def insert_application(pool, *, certificate_no: Optional[str],
                             main_score: Optional[int], extra_score: Optional[int],
                             total_score: Optional[int], category: Optional[str],
                             date_text: Optional[str], admitted: int,
                             specialty_id: int, faculty_id: int, university_id: int,
                             raw_html: Optional[str]) -> int:
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                INSERT INTO applications (
                  certificate_no, main_score, extra_score, total_score,
                  category, date_text, admitted,
                  specialty_ids, faculty_ids, university_ids, raw_data
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,
                          JSON_ARRAY(%s), JSON_ARRAY(%s), JSON_ARRAY(%s), %s)
            """, (
                certificate_no, main_score, extra_score, total_score,
                category, date_text, admitted,
                specialty_id, faculty_id, university_id, raw_html
            ))
            app_id = cur.lastrowid
            # связи
            await cur.execute("INSERT INTO specialty_applications(specialty_id, application_id) VALUES(%s,%s)",
                              (specialty_id, app_id))
            await cur.execute("INSERT INTO application_specialties(application_id, specialty_id) VALUES(%s,%s)",
                              (app_id, specialty_id))
            await cur.execute("INSERT INTO application_faculties(application_id, faculty_id) VALUES(%s,%s)",
                              (app_id, faculty_id))
            await cur.execute("INSERT INTO application_universities(application_id, university_id) VALUES(%s,%s)",
                              (app_id, university_id))
            return app_id

# Пересобираем JSON-списки *_ids из связей (после основного импорта)
async def refresh_json_lists(pool):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            # universities.faculty_ids
            await cur.execute("""
                UPDATE universities u
                JOIN (
                  SELECT university_id, JSON_ARRAYAGG(faculty_id) AS arr
                  FROM university_faculties GROUP BY university_id
                ) t ON t.university_id = u.id
                SET u.faculty_ids = t.arr
            """)
            # faculties.specialty_ids
            await cur.execute("""
                UPDATE faculties f
                JOIN (
                  SELECT faculty_id, JSON_ARRAYAGG(specialty_id) AS arr
                  FROM faculty_specialties GROUP BY faculty_id
                ) t ON t.faculty_id = f.id
                SET f.specialty_ids = t.arr
            """)
            # specialties.application_ids
            await cur.execute("""
                UPDATE specialties s
                JOIN (
                  SELECT specialty_id, JSON_ARRAYAGG(application_id) AS arr
                  FROM specialty_applications GROUP BY specialty_id
                ) t ON t.specialty_id = s.id
                SET s.application_ids = t.arr
            """)

# --------- Парсеры HTML (как у вас, но без сохранения JSON-файлов) ---------
def parse_faculties_from_report(report_html: str) -> list[dict]:
    soup = BeautifulSoup(report_html, "html.parser")
    faculties = []
    for card in soup.select("li.card-item"):
        faculty_name = clean_text(card.select_one("p.university-name"))
        directions = []
        for row in card.select(".rows.border-top, .rows:has(.d-lg-flex)"):
            cols = row.select(".cell")
            if not cols: continue
            specialty_text = clean_text(cols[1]) if len(cols) > 1 else None
            major, specialty, education_type = parse_specialty(specialty_text)
            payment_form = clean_text(cols[2]) if len(cols) > 2 else None
            payment_amount = parse_int_safe(clean_text(cols[3]) if len(cols) > 3 else None)
            plan_text = clean_text(cols[4]) if len(cols) > 4 else None
            threshold_text = clean_text(cols[5]) if len(cols) > 5 else None
            main_pass, extra_count, extra_subjects = parse_threshold(threshold_text)
            registered = clean_text(cols[6]) if len(cols) > 6 else None

            # ссылка на рейтинг
            link = row.select_one("a[href*='personalcabinet_report']")
            href = link.get("href") if link else None
            rating_file = os.path.basename(href).split("?")[0] if href else None

            directions.append({
                "raw_html": str(row),
                "code": clean_text(cols[0]) if len(cols) > 0 else None,
                "major": major,
                "specialty": specialty,
                "education_type": education_type,
                "payment_form": payment_form,
                "payment_amount": payment_amount,
                "plan": plan_text,
                "main_pass": main_pass,
                "extra_count": extra_count,
                "extra_subjects": extra_subjects,
                "registered": registered,
                "rating_file": rating_file
            })
        faculties.append({"faculty_name": faculty_name, "raw_html": str(card), "directions": directions})
    return faculties

def parse_rating_table(html: str) -> tuple[dict, list[dict]]:
    soup = BeautifulSoup(html, "html.parser")

    header = {}
    top_block = soup.select_one("div.text-right")
    if top_block:
        spans = top_block.find_all("span")
        if len(spans) >= 1: header["university"] = clean_text(spans[0])
        if len(spans) >= 2: header["director"] = clean_text(spans[1])
    program_tag = soup.select_one("p.headerColor b")
    if program_tag:
        header["program"] = clean_text(program_tag)

    rows_out = []
    # Каждая <table.table> может быть бюджет/ваучер/контракт
    for table in soup.select("table.table"):
        header_text = clean_text(table.select_one("div.cityColir"))
        is_contract = header_text is None
        for tr in table.select("tbody tr"):
            cols = [clean_text(td) for td in tr.find_all(["td", "th"])]
            if len(cols) < 2: continue
            cert_text, admitted, _note = parse_certificate(cols[1])
            if is_contract and len(cols) >= 6:
                category = cols[5]
            elif header_text:
                category = header_text.split(":")[0].strip()
            else:
                category = None
            rows_out.append({
                "raw_html": str(tr),
                "num": cols[0],
                "certificate": cert_text,
                "main_score": parse_int_safe(cols[2] if len(cols) > 2 else None),
                "extra_score": parse_int_safe(cols[3] if len(cols) > 3 else None),
                "total_score": parse_int_safe(cols[4] if len(cols) > 4 else None),
                "category": category,
                "date": cols[6] if is_contract and len(cols) >= 7 else (cols[5] if not is_contract and len(cols) >= 6 else None),
                "admitted": 1 if admitted else 0
            })
    return header, rows_out

# --------- Главный ETL ---------
async def run_pipeline():
    pool = await get_pool()

    # 1) index.html -> список университетов (с базовыми полями)
    index_html = await read_file(INDEX_HTML)
    soup = BeautifulSoup(index_html, "html.parser")
    uni_cards = soup.select("li.universities-item")

    for li in uni_cards:
        name_tag = li.select_one("a.university-name")
        if not name_tag: continue
        uni_name = clean_text(name_tag)
        report_file = name_tag["href"].split("?")[0] if name_tag.has_attr("href") else None

        addr_tag = li.find("div", string=lambda t: t and "Адрес" in t)
        address = clean_text(addr_tag.find_next("p")) if addr_tag else None

        rector_tag = li.find("div", string=lambda t: t and any(k in t for k in ["Ректор", "Начальник", "И.о.ректор"]))
        rector = clean_text(rector_tag.find_next("p")) if rector_tag else None

        site_tag = li.find("a", href=True, class_="sm-text")
        site = site_tag.get("href") if site_tag else None

        university_id = await upsert_university(pool, uni_name, site, address, rector, str(li))

        # 2) reports*.html — факультеты и направления
        if not report_file:
            continue
        report_path = os.path.join(REPORTS_DIR, report_file)
        if not os.path.exists(report_path):
            continue

        report_html = await read_file(report_path)
        faculties = parse_faculties_from_report(report_html)

        for fac in faculties:
            faculty_id = await insert_faculty(pool, fac["faculty_name"], fac["raw_html"])
            await link_university_faculty(pool, university_id, faculty_id)

            for d in fac["directions"]:
                # признаки форм
                has_contract = 1 if d["payment_form"] == "Контракт" else 0
                has_budget  = 1 if d["payment_form"] == "Бюджет"   else 0
                has_voucher = 1 if d["payment_form"] == "Ваучер"   else 0

                spec_id = await insert_specialty(
                    pool,
                    code=d["code"],
                    specialty_name=d["specialty"],
                    major_name=d["major"],
                    faculty_name=fac["faculty_name"],
                    university_name=uni_name,
                    has_contract=has_contract,
                    has_budget=has_budget,
                    has_voucher=has_voucher,
                    contract_amount=d["payment_amount"],
                    is_part_time_flag=is_part_time(d["education_type"]),
                    main_pass=d["main_pass"],
                    extra_count=d["extra_count"],
                    extra_subjects=d["extra_subjects"],
                    raw_html=d["raw_html"]
                )
                await link_faculty_specialty(pool, faculty_id, spec_id)

                # 3) На этом шаге сразу прогружаем заявки из рейтингов
                rating_file = d.get("rating_file")
                if rating_file:
                    rating_path = os.path.join(RATINGS_HTML_DIR, rating_file)
                    if os.path.exists(rating_path):
                        r_html = await read_file(rating_path)
                        header, rows = parse_rating_table(r_html)
                        for r in rows:
                            await insert_application(
                                pool,
                                certificate_no=r["certificate"],
                                main_score=r["main_score"],
                                extra_score=r["extra_score"],
                                total_score=r["total_score"],
                                category=r["category"],
                                date_text=r["date"],
                                admitted=r["admitted"],
                                specialty_id=spec_id,
                                faculty_id=faculty_id,
                                university_id=university_id,
                                raw_html=r["raw_html"]
                            )

    # 4) Пересобрать JSON-списки *_ids по связям
    await refresh_json_lists(pool)

    pool.close()
    await pool.wait_closed()
    print("✅ Импорт в MySQL завершён.")

# ======== Точка входа ========
if __name__ == "__main__":
    asyncio.run(run_pipeline())