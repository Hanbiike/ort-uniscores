import os
import re
import json
from bs4 import BeautifulSoup

def clean_text(tag):
    """Извлекает текст без переносов строк и с нормализацией пробелов"""
    if not tag:
        return None
    text = tag.get_text(" ", strip=True)
    text = text.replace("\n", " ")
    text = re.sub(r"\s+", " ", text)  # убираем лишние пробелы
    return text.strip()

def parse_threshold(text: str):
    """Парсит строку threshold в структурированные данные"""
    if not text:
        return None

    result = {
        "main_score": None,
        "extra_required": None,
        "extra_count": 0,
        "subjects": {}
    }

    # Основной балл
    main_match = re.search(r"Негизги балл-(\d+)", text)
    if main_match:
        result["main_score"] = int(main_match.group(1))

    # Доп. предмет обязателен / нет
    if "Доп. предмет не обязательно" in text:
        result["extra_required"] = False
    elif "Кошумча" in text:
        result["extra_required"] = True

    # Количество обязательных предметов
    count_match = re.search(r"Кошумча\s*.-(\d+)\s*сабак", text)
    if count_match:
        result["extra_count"] = int(count_match.group(1))

    # Предметы с баллами
    for subj, score in re.findall(r"([А-Яа-яЁёA-Za-z.\s]+)-(\d+)", text):
        subj = subj.strip()
        score = int(score)
        if not subj.startswith("Негизги") and not subj.startswith("Кошумча"):
            result["subjects"][subj] = score

    return result

def parse_specialty(full_text: str):
    """Делит строку specialty на major, specialty, education_type, voucher"""
    if not full_text:
        return None, None, None, False

    voucher = "Ваучер" in full_text

    # major = всё до [
    major_match = re.match(r"^(.*?)\[", full_text)
    major = major_match.group(1).strip() if major_match else full_text.strip()

    # specialty = внутри [ ]
    specialty_match = re.search(r"\[(.*?)\]", full_text)
    specialty = specialty_match.group(1).strip() if specialty_match else None

    # education_type = всё в скобках (), кроме Ваучер
    types = re.findall(r"\((.*?)\)", full_text)
    if types:
        types = [t.strip() for t in types if "Ваучер" not in t]
        education_type = ", ".join(types) if types else None
    else:
        education_type = None

    return major, specialty, education_type, voucher

def parse_universities(index_path):
    with open(index_path, "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f, "html.parser")

    universities = []
    for li in soup.select("li.universities-item"):
        name_tag = li.select_one("a.university-name")
        if not name_tag:
            continue

        uni = {
            "name": clean_text(name_tag),
            "report_file": name_tag["href"].split("?")[0],  # типа reportsXXXX.html
            "address": None,
            "rector": None,
            "site": None,
            "faculties": []
        }

        # адрес
        addr_tag = li.find("div", string=lambda t: t and "Адрес" in t)
        if addr_tag:
            uni["address"] = clean_text(addr_tag.find_next("p"))

        # ректор / начальник
        rector_tag = li.find("div", string=lambda t: t and any(k in t for k in ["Ректор", "Начальник", "И.о.ректор"]))
        if rector_tag:
            uni["rector"] = clean_text(rector_tag.find_next("p"))

        # сайт
        site_tag = li.find("a", href=True, class_="sm-text")
        if site_tag:
            uni["site"] = site_tag.get("href")

        universities.append(uni)
    return universities

def parse_faculties(report_path):
    with open(report_path, "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f, "html.parser")

    faculties = []
    for card in soup.select("li.card-item"):
        faculty_name = clean_text(card.select_one("p.university-name"))

        directions = []
        for row in card.select(".rows.border-top, .rows:has(.d-lg-flex)"):
            cols = row.select(".cell")
            if not cols:
                continue

            specialty_text = clean_text(cols[1]) if len(cols) > 1 else None
            major, specialty, education_type, voucher = parse_specialty(specialty_text)

            threshold_text = clean_text(cols[5]) if len(cols) > 5 else None
            threshold_parsed = parse_threshold(threshold_text)

            direction = {
                "code": clean_text(cols[0]) if len(cols) > 0 else None,
                "major": major,
                "specialty": specialty,
                "education_type": education_type,
                "voucher": voucher,
                "payment_form": clean_text(cols[2]) if len(cols) > 2 else None,
                "payment_amount": clean_text(cols[3]) if len(cols) > 3 else None,
                "plan": clean_text(cols[4]) if len(cols) > 4 else None,
                "threshold": threshold_parsed,
                "registered": clean_text(cols[6]) if len(cols) > 6 else None,
                "rating_json": None
            }

            # строим имя для JSON вместо ссылки на HTML
            link = row.select_one("a[href*='personalcabinet_report']")
            if link:
                href = link.get("href")
                base = os.path.basename(href)
                if "Ranjirk" in base:
                    direction["rating_json"] = f"results/rating_r_{os.path.splitext(base)[0]}.json"
                elif "Ranjirb" in base:
                    direction["rating_json"] = f"results/rating_b_{os.path.splitext(base)[0]}.json"

            directions.append(direction)

        faculties.append({
            "faculty_name": faculty_name,
            "directions": directions
        })
    return faculties

def main():
    index_path = "index.html"
    universities = parse_universities(index_path)

    for uni in universities:
        report_file = uni["report_file"]
        if os.path.exists(report_file):
            uni["faculties"] = parse_faculties(report_file)
        else:
            uni["faculties"] = []

    with open("universities.json", "w", encoding="utf-8") as f:
        json.dump(universities, f, ensure_ascii=False, indent=2)

    print("✅ universities.json готов (threshold структурирован)")

if __name__ == "__main__":
    main()