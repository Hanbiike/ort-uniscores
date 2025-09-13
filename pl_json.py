# -*- coding: utf-8 -*-
import os
import re
import json
import asyncio
import aiofiles
import statistics
from bs4 import BeautifulSoup
from collections import defaultdict

# --------- Конфиг путей ---------
INDEX_HTML = "index.html"              # корневой индекс
REPORTS_DIR = "."                      # где лежат reports*.html
RATINGS_HTML_DIR = "downloaded"        # исходные personalcabinet_report_*.html
RESULTS_DIR = "results"                # rating_*.json, universities.json, stats.json
UNIVERSITIES_DIR = "universities"      # финальные university_*.json (без студентов)

os.makedirs(RESULTS_DIR, exist_ok=True)
os.makedirs(UNIVERSITIES_DIR, exist_ok=True)

FORMS = ["Бюджет", "Контракт", "Ваучер"]
SCORE_KEYS = {
    "main": "main_score",
    "extra": "extra_score",
    "total": "total_score",
}

# --------- Утилиты ---------
def norm_space(s: str | None) -> str | None:
    if s is None:
        return None
    return re.sub(r"\s+", " ", s.replace("\n", " ")).strip()

def clean_text(tag) -> str | None:
    if tag is None:
        return None
    if hasattr(tag, "get_text"):
        return norm_space(tag.get_text(" ", strip=True))
    return norm_space(str(tag))

async def read_file(path: str) -> str:
    async with aiofiles.open(path, "r", encoding="utf-8") as f:
        return await f.read()

async def write_json(path: str, obj) -> None:
    async with aiofiles.open(path, "w", encoding="utf-8") as f:
        await f.write(json.dumps(obj, ensure_ascii=False, indent=2))

def load_json_sync(path: str):
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def parse_int_safe(x: str | None) -> int | None:
    if not x:
        return None
    digits = re.sub(r"[^\d]", "", str(x))
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None

def safe_stats(values: list[int] | list[float]):
    if not values:
        return None
    return {
        "min": min(values),
        "avg": round(statistics.mean(values), 2),
        "max": max(values)
    }

# --------- Threshold ---------
def parse_threshold(text: str | None):
    if not text:
        return None
    res = {
        "main_score": None,
        "extra_required": None,
        "extra_count": 0,
        "subjects": {}
    }
    m = re.search(r"Негизги балл-(\d+)", text)
    if m:
        res["main_score"] = int(m.group(1))
    if "Доп. предмет не обязательно" in text:
        res["extra_required"] = False
    elif "Кошумча" in text:
        res["extra_required"] = True
    m2 = re.search(r"Кошумча\s*.-(\d+)\s*сабак", text)
    if m2:
        res["extra_count"] = int(m2.group(1))
    for subj, score in re.findall(r"([А-Яа-яЁёA-Za-z.\s]+)-(\d+)", text):
        subj = subj.strip()
        if subj.startswith("Негизги") or subj.startswith("Кошумча"):
            continue
        res["subjects"][subj] = int(score)
    return res

# --------- Specialty ---------
def parse_specialty(full_text: str | None):
    if not full_text:
        return None, None, None
    mm = re.match(r"^(.*?)\[", full_text)
    major = mm.group(1).strip() if mm else full_text.strip()
    sm = re.search(r"\[(.*?)\]", full_text)
    specialty = sm.group(1).strip() if sm else None
    types = re.findall(r"\((.*?)\)", full_text)
    if types:
        types = [t.strip() for t in types if "Ваучер" not in t]
        education_type = ", ".join(types) if types else None
    else:
        education_type = None
    return major, specialty, education_type

# --------- Сертификат ---------
def parse_certificate(text: str | None):
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

# --------- Парсинг rating HTML -> JSON ---------
async def parse_rating_file(html_path: str) -> dict:
    html = await read_file(html_path)
    soup = BeautifulSoup(html, "html.parser")

    data = {
        "file": os.path.basename(html_path),
        "university": None,
        "director": None,
        "program": None,
        "admitted_count": 0,
        "not_admitted_count": 0,
        "tables": []
    }

    top_block = soup.select_one("div.text-right")
    if top_block:
        spans = top_block.find_all("span")
        if len(spans) >= 1: data["university"] = clean_text(spans[0])
        if len(spans) >= 2: data["director"] = clean_text(spans[1])

    program_tag = soup.select_one("p.headerColor b")
    if program_tag:
        data["program"] = clean_text(program_tag)

    for table in soup.select("table.table"):
        header = clean_text(table.select_one("div.cityColir"))
        is_contract = header is None

        records = []
        for tr in table.select("tbody tr"):
            cols = [clean_text(td) for td in tr.find_all(["td", "th"])]
            if len(cols) < 2:
                continue

            cert_text, admitted, note = parse_certificate(cols[1])
            if is_contract and len(cols) >= 6:
                category = cols[5]
            elif header:
                category = header.split(":")[0].strip()
            else:
                category = None

            record = {
                "num": cols[0],
                "certificate": cert_text,
                "note": note,
                "main_score": cols[2] if len(cols) > 2 else None,
                "extra_score": cols[3] if len(cols) > 3 else None,
                "total_score": cols[4] if len(cols) > 4 else None,
                "category": category,
                "date": cols[6] if is_contract and len(cols) >= 7 else (cols[5] if not is_contract and len(cols) >= 6 else None),
                "admitted": admitted
            }

            if admitted: data["admitted_count"] += 1
            else:        data["not_admitted_count"] += 1

            records.append(record)

        data["tables"].append({"header": header, "records": records})

    return data

async def parse_all_ratings():
    tasks = []
    for fn in os.listdir(RATINGS_HTML_DIR):
        if fn.startswith("personalcabinet_report_Ranjir") and fn.endswith(".html"):
            tasks.append(asyncio.create_task(parse_rating_file(os.path.join(RATINGS_HTML_DIR, fn))))
    results = await asyncio.gather(*tasks) if tasks else []

    save_tasks = []
    for data in results:
        base = os.path.splitext(data["file"])[0]
        if "Ranjirk" in base:
            out = os.path.join(RESULTS_DIR, f"rating_r_{base}.json")
        elif "Ranjirb" in base:
            out = os.path.join(RESULTS_DIR, f"rating_b_{base}.json")
        else:
            continue
        save_tasks.append(write_json(out, data))
    if save_tasks:
        await asyncio.gather(*save_tasks)

# --------- Университеты (index + reports) ---------
async def parse_universities_index() -> list[dict]:
    html = await read_file(INDEX_HTML)
    soup = BeautifulSoup(html, "html.parser")

    universities = []
    for li in soup.select("li.universities-item"):
        name_tag = li.select_one("a.university-name")
        if not name_tag:
            continue

        uni = {
            "name": clean_text(name_tag),
            "report_file": name_tag["href"].split("?")[0],
            "address": None,
            "rector": None,
            "site": None,
            "faculties": []
        }

        addr_tag = li.find("div", string=lambda t: t and "Адрес" in t)
        if addr_tag:
            uni["address"] = clean_text(addr_tag.find_next("p"))

        rector_tag = li.find("div", string=lambda t: t and any(k in t for k in ["Ректор", "Начальник", "И.о.ректор"]))
        if rector_tag:
            uni["rector"] = clean_text(rector_tag.find_next("p"))

        site_tag = li.find("a", href=True, class_="sm-text")
        if site_tag:
            uni["site"] = site_tag.get("href")

        universities.append(uni)
    return universities

def parse_faculties_from_report(report_html: str) -> list[dict]:
    soup = BeautifulSoup(report_html, "html.parser")
    faculties = []
    for card in soup.select("li.card-item"):
        faculty_name = clean_text(card.select_one("p.university-name"))

        directions = []
        for row in card.select(".rows.border-top, .rows:has(.d-lg-flex)"):
            cols = row.select(".cell")
            if not cols:
                continue

            specialty_text = clean_text(cols[1]) if len(cols) > 1 else None
            major, specialty, education_type = parse_specialty(specialty_text)

            payment_form = clean_text(cols[2]) if len(cols) > 2 else None
            payment_amount = clean_text(cols[3]) if len(cols) > 3 else None

            threshold_text = clean_text(cols[5]) if len(cols) > 5 else None
            threshold_parsed = parse_threshold(threshold_text)

            direction = {
                "code": clean_text(cols[0]) if len(cols) > 0 else None,
                "major": major,
                "specialty": specialty,
                "education_type": education_type,
                "payment_form": payment_form,              # временно
                "payment_amount": payment_amount,
                "plan": clean_text(cols[4]) if len(cols) > 4 else None,
                "threshold": threshold_parsed,
                "registered": clean_text(cols[6]) if len(cols) > 6 else None,
                "rating_json": None
            }

            # ссылка -> имя rating json
            link = row.select_one("a[href*='personalcabinet_report']")
            if link:
                href = link.get("href")
                base = os.path.basename(href)
                if "Ranjirk" in base:
                    direction["rating_json"] = os.path.join(RESULTS_DIR, f"rating_r_{os.path.splitext(base)[0]}.json")
                elif "Ranjirb" in base:
                    direction["rating_json"] = os.path.join(RESULTS_DIR, f"rating_b_{os.path.splitext(base)[0]}.json")

            directions.append(direction)

        faculties.append({"faculty_name": faculty_name, "directions": directions})
    return faculties

async def build_universities_json():
    universities = await parse_universities_index()

    tasks = []
    map_idx_path = []
    for i, uni in enumerate(universities):
        rp = uni.get("report_file")
        if rp and os.path.exists(os.path.join(REPORTS_DIR, rp)):
            path = os.path.join(REPORTS_DIR, rp)
            tasks.append(asyncio.create_task(read_file(path)))
            map_idx_path.append((i, path))

    if tasks:
        htmls = await asyncio.gather(*tasks)
        for (i, _), html in zip(map_idx_path, htmls):
            universities[i]["faculties"] = parse_faculties_from_report(html)

    # Полный свод (ничего не теряем: name/address/rector/site/report_file/faculties)
    await write_json(os.path.join(RESULTS_DIR, "universities.json"), universities)

# --------- Извлечение баллов admitted из rating_json в 3-х разрезах ---------
def extract_scores_from_rating(rating_data: dict):
    """
    Возвращает кортеж словарей:
    - overall: {'main':[], 'extra':[], 'total':[]}
    - by_cat:  {'main':{'Бишкек':[], ...}, 'extra':{...}, 'total':{...}}
    """
    overall = {k: [] for k in SCORE_KEYS}  # k in ['main','extra','total']
    by_cat = {k: defaultdict(list) for k in SCORE_KEYS}

    for tbl in rating_data.get("tables", []):
        for rec in tbl.get("records", []):
            if not rec.get("admitted"):
                continue
            cat = rec.get("category") or "Неизвестно"
            for kind, field in SCORE_KEYS.items():
                val = parse_int_safe(rec.get(field))
                if val is None:
                    continue
                overall[kind].append(val)
                by_cat[kind][cat].append(val)

    return overall, by_cat

# --------- Статистика для группы записей одного кода (несколько форм) ---------
def compute_direction_group_stats(group_entries: list[dict], rating_cache: dict):
    # инициализация накопителей
    has_form = {f: False for f in FORMS}
    # overall
    overall_scores = {k: [] for k in SCORE_KEYS}
    # by form
    by_form_scores = {f: {k: [] for k in SCORE_KEYS} for f in FORMS}
    # by form + category
    by_form_cat = {f: {k: defaultdict(list) for k in SCORE_KEYS} for f in FORMS}
    # contract payments
    contract_amounts = []

    for entry in group_entries:
        pf = entry.get("payment_form")
        if pf not in FORMS:
            continue
        has_form[pf] = True

        # контрактные суммы (могут отсутствовать)
        if pf == "Контракт":
            val = parse_int_safe(entry.get("payment_amount"))
            if val and val > 0:
                contract_amounts.append(val)

        rpath = entry.get("rating_json")
        if not rpath or not os.path.exists(rpath):
            continue
        if rpath not in rating_cache:
            rating_cache[rpath] = load_json_sync(rpath) or {}
        rating_data = rating_cache[rpath]

        ov, byc = extract_scores_from_rating(rating_data)

        # накопление
        for kind in SCORE_KEYS:
            overall_scores[kind].extend(ov[kind])
            by_form_scores[pf][kind].extend(ov[kind])
            for cat, lst in byc[kind].items():
                by_form_cat[pf][kind][cat].extend(lst)

    # агрегаты
    stats_overall = {kind: safe_stats(overall_scores[kind]) for kind in SCORE_KEYS}
    stats_by_form = {}
    for f in FORMS:
        if has_form[f]:
            stats_by_form[f] = {kind: safe_stats(by_form_scores[f][kind]) for kind in SCORE_KEYS}
        else:
            stats_by_form[f] = "Форма отсутствует"
    stats_by_form_cat = {}
    for f in FORMS:
        if has_form[f]:
            stats_by_form_cat[f] = {
                kind: {cat: safe_stats(vals) for cat, vals in by_form_cat[f][kind].items()}
                for kind in SCORE_KEYS
            }
        else:
            stats_by_form_cat[f] = "Форма отсутствует"

    contract_payment = safe_stats([v for v in contract_amounts if v and v > 0])
    flags = {
        "has_contract": has_form["Контракт"],
        "has_budget": has_form["Бюджет"],
        "has_voucher": has_form["Ваучер"]
    }

    # сырьё для агрегации уровней выше
    raw = {
        "overall_scores": overall_scores,                   # dict kind -> list
        "by_form_scores": by_form_scores,                   # form -> kind -> list
        "by_form_cat": by_form_cat,                         # form -> kind -> cat -> list
        "contract_amounts": [v for v in contract_amounts if v and v > 0]
    }
    return flags, stats_overall, stats_by_form, stats_by_form_cat, contract_payment, raw

# --------- Сборка university_*.json + накопление для глобальной статистики/рейтингов ---------
async def build_university_files_and_collect_global():
    all_unis = load_json_sync(os.path.join(RESULTS_DIR, "universities.json")) or []

    # глобальные накопители (по всем универам)
    GLOBAL = {
        "overall_scores": {k: [] for k in SCORE_KEYS},
        "by_form_scores": {f: {k: [] for k in SCORE_KEYS} for f in FORMS},
        "by_form_cat": {f: {k: defaultdict(list) for k in SCORE_KEYS} for f in FORMS},
        "contract_amounts": [],
        # рейтинги-накопители
        "universities": {k: [] for k in SCORE_KEYS},      # list of (uni_name, avg_kind)
        "faculties_global": {k: [] for k in SCORE_KEYS},  # list of (uni_name, faculty_name, avg_kind)
        "directions_global": {k: [] for k in SCORE_KEYS}  # list of (uni_name, faculty_name, code, avg_kind)
    }

    rating_cache = {}

    for uni in all_unis:
        uni_name = uni["name"]
        uni_name_safe = (uni_name.replace(" ", "_").replace('"', "").replace("«", "").replace("»", "").replace("/", "_"))
        out_path = os.path.join(UNIVERSITIES_DIR, f"university_{uni_name_safe}.json")

        # накопители по универу
        uni_overall_scores = {k: [] for k in SCORE_KEYS}
        uni_by_form_scores = {f: {k: [] for k in SCORE_KEYS} for f in FORMS}
        uni_by_form_cat = {f: {k: defaultdict(list) for k in SCORE_KEYS} for f in FORMS}
        uni_contract_amounts = []

        faculties_out = []

        for fac in uni.get("faculties", []):
            fac_name = fac.get("faculty_name")

            # сгруппировать направления по коду
            groups = defaultdict(list)
            for d in fac.get("directions", []):
                code = d.get("code") or "Без кода"
                groups[code].append(d)

            # накопители по факультету
            fac_overall_scores = {k: [] for k in SCORE_KEYS}
            fac_by_form_scores = {f: {k: [] for k in SCORE_KEYS} for f in FORMS}
            fac_by_form_cat = {f: {k: defaultdict(list) for k in SCORE_KEYS} for f in FORMS}
            fac_contract_amounts = []

            directions_out = []

            for code, entries in groups.items():
                base = entries[0]
                flags, s_overall, s_by_form, s_by_form_cat, s_contract_payment, raw = compute_direction_group_stats(
                    entries, rating_cache
                )

                # рейтинги направлений (по каждому виду баллов — берём avg total/main/extra)
                for kind in SCORE_KEYS:
                    avg_k = s_overall[kind]["avg"] if s_overall.get(kind) else None
                    if avg_k is not None:
                        GLOBAL["directions_global"][kind].append((uni_name, fac_name, code, avg_k))

                # накопление -> факультет
                for kind in SCORE_KEYS:
                    fac_overall_scores[kind].extend(raw["overall_scores"][kind])
                for f in FORMS:
                    for kind in SCORE_KEYS:
                        fac_by_form_scores[f][kind].extend(raw["by_form_scores"][f][kind])
                        for cat, lst in raw["by_form_cat"][f][kind].items():
                            fac_by_form_cat[f][kind][cat].extend(lst)
                fac_contract_amounts.extend(raw["contract_amounts"])

                # узел направления (без студентов) + флаги + контрактные суммы
                directions_out.append({
                    "code": code,
                    "major": base.get("major"),
                    "specialty": base.get("specialty"),
                    "education_type": base.get("education_type"),
                    "has_contract": flags["has_contract"],
                    "has_budget": flags["has_budget"],
                    "has_voucher": flags["has_voucher"],
                    "contract_payment": s_contract_payment,
                    "stats": {
                        "overall_scores": s_overall,                 # {'main':{..}, 'extra':{..}, 'total':{..}}
                        "scores_by_form": s_by_form,                 # {'Бюджет': {'main':..,'extra':..,'total':..} | "Форма отсутствует"}
                        "scores_by_form_category": s_by_form_cat     # {'Бюджет': {'main':{'Бишкек':..}, 'extra':.., 'total':..} | "Форма отсутствует"}
                    }
                })

            # агрегаты факультета
            fac_stats = {
                "overall_scores": {k: safe_stats(fac_overall_scores[k]) for k in SCORE_KEYS},
                "scores_by_form": {f: {k: safe_stats(fac_by_form_scores[f][k]) for k in SCORE_KEYS} for f in FORMS},
                "scores_by_form_category": {
                    f: {k: {cat: safe_stats(vals) for cat, vals in fac_by_form_cat[f][k].items()} for k in SCORE_KEYS}
                    for f in FORMS
                },
                "contract_payment": safe_stats([v for v in fac_contract_amounts if v and v > 0])
            }

            # рейтинг факультетов (по каждому виду баллов)
            for kind in SCORE_KEYS:
                avg_k = fac_stats["overall_scores"][kind]["avg"] if fac_stats["overall_scores"][kind] else None
                if avg_k is not None:
                    GLOBAL["faculties_global"][kind].append((uni_name, fac_name, avg_k))

            faculties_out.append({
                "faculty_name": fac_name,
                "stats": fac_stats,
                "directions": directions_out
            })

            # накопление -> универ
            for kind in SCORE_KEYS:
                uni_overall_scores[kind].extend(fac_overall_scores[kind])
            for f in FORMS:
                for kind in SCORE_KEYS:
                    uni_by_form_scores[f][kind].extend(fac_by_form_scores[f][kind])
                    for cat, vals in fac_by_form_cat[f][kind].items():
                        uni_by_form_cat[f][kind][cat].extend(vals)
            uni_contract_amounts.extend([v for v in fac_contract_amounts if v and v > 0])

        # агрегаты университета
        uni_stats = {
            "overall_scores": {k: safe_stats(uni_overall_scores[k]) for k in SCORE_KEYS},
            "scores_by_form": {f: {k: safe_stats(uni_by_form_scores[f][k]) for k in SCORE_KEYS} for f in FORMS},
            "scores_by_form_category": {
                f: {k: {cat: safe_stats(vals) for cat, vals in uni_by_form_cat[f][k].items()} for k in SCORE_KEYS}
                for f in FORMS
            },
            "contract_payment": safe_stats([v for v in uni_contract_amounts if v and v > 0])
        }

        # рейтинги университетов (по каждому виду баллов)
        for kind in SCORE_KEYS:
            avg_k = uni_stats["overall_scores"][kind]["avg"] if uni_stats["overall_scores"][kind] else None
            GLOBAL["universities"][kind].append((uni_name, avg_k))

        # накопление -> глобал
        for kind in SCORE_KEYS:
            GLOBAL["overall_scores"][kind].extend(uni_overall_scores[kind])
        for f in FORMS:
            for kind in SCORE_KEYS:
                GLOBAL["by_form_scores"][f][kind].extend(uni_by_form_scores[f][kind])
                for cat, vals in uni_by_form_cat[f][kind].items():
                    GLOBAL["by_form_cat"][f][kind][cat].extend(vals)
        GLOBAL["contract_amounts"].extend([v for v in uni_contract_amounts if v and v > 0])

        # запись файла университета (без студентов)
        uni_out = {
            "name": uni_name,
            "address": uni.get("address"),
            "rector": uni.get("rector"),
            "site": uni.get("site"),
            "stats": uni_stats,
            "faculties": faculties_out
        }
        await write_json(out_path, uni_out)
        print(f"✅ Сохранён университет: {out_path}")

    return GLOBAL

# --------- Формирование results/stats.json (ГЛОБАЛКА + РЕЙТИНГИ) ---------
async def build_stats_json(GLOBAL):
    global_stats = {
        "overall_scores": {k: safe_stats(GLOBAL["overall_scores"][k]) for k in SCORE_KEYS},
        "scores_by_form": {f: {k: safe_stats(GLOBAL["by_form_scores"][f][k]) for k in SCORE_KEYS} for f in FORMS},
        "scores_by_form_category": {
            f: {k: {cat: safe_stats(vals) for cat, vals in GLOBAL["by_form_cat"][f][k].items()} for k in SCORE_KEYS}
            for f in FORMS
        },
        "contract_payment": safe_stats(GLOBAL["contract_amounts"])
    }

    # рейтинги по КАЖДОМУ виду баллов
    def rank_unis(items):   # items: list[(name, avg)]
        return [{"university": n, "avg_score": a} for (n, a) in sorted(
            [x for x in items if x[1] is not None], key=lambda x: x[1], reverse=True
        )]

    def rank_fac(items):    # items: list[(uni, fac, avg)]
        return [{"university": u, "faculty": f, "avg_score": a} for (u, f, a) in sorted(
            items, key=lambda x: x[2], reverse=True
        ) if a is not None]

    def rank_dir(items):    # items: list[(uni, fac, code, avg)]
        return [{"university": u, "faculty": f, "code": c, "avg_score": a} for (u, f, c, a) in sorted(
            items, key=lambda x: x[3], reverse=True
        ) if a is not None]

    rankings = {}
    for kind in SCORE_KEYS:  # main / extra / total
        rankings[kind] = {
            "universities_by_avg_score": rank_unis(GLOBAL["universities"][kind]),
            "faculties_by_avg_score":    rank_fac(GLOBAL["faculties_global"][kind]),
            "directions_by_avg_score":   rank_dir(GLOBAL["directions_global"][kind]),
        }

    out = {
        "global": global_stats,
        "rankings": rankings,
        "notes": {
            "score_kinds": {"main": "main_score", "extra": "extra_score", "total": "total_score"},
            "forms": FORMS,
            "contract_payment_stat_only_for_contract": True
        }
    }
    await write_json(os.path.join(RESULTS_DIR, "stats.json"), out)
    print(f"✅ Статистика сохранена: {os.path.join(RESULTS_DIR, 'stats.json')}")

# --------- Главный пайплайн ---------
async def main():
    # 1) HTML рейтингов -> JSON
    await parse_all_ratings()

    # 2) Университеты с полными полями + faculties/directions (results/universities.json)
    await build_universities_json()

    # 3) Университетские файлы без студентов, только агрегаты; собрать глобальные накопители и рейтинги
    GLOBAL = await build_university_files_and_collect_global()

    # 4) Глобальная stats.json (только общий уровень + рейтинги), во всех 3-х видах баллов
    await build_stats_json(GLOBAL)

if __name__ == "__main__":
    asyncio.run(main())