import os
import re
import json
from bs4 import BeautifulSoup

INPUT_DIR = "downloaded"   # где лежат html
OUTPUT_DIR = "results"     # куда сохраняем json

os.makedirs(OUTPUT_DIR, exist_ok=True)

def clean_text(tag):
    if not tag:
        return None
    text = tag.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def parse_certificate(text):
    """
    Очищает сертификат:
    - убирает (Реком)
    - отделяет комментарий в [] в note
    """
    admitted = False
    note = None

    if not text:
        return None, admitted, note

    # проверяем "Реком"
    if "(Реком" in text:
        admitted = True
        text = text.replace("(Реком)", "").strip()

    # комментарий в []
    match = re.search(r"\[(.*)\]", text)
    if match:
        note = match.group(1).strip()
        text = re.sub(r"\[.*\]", "", text).strip()

    return text, admitted, note

def parse_rating(html_file):
    with open(html_file, "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f, "html.parser")

    data = {
        "file": os.path.basename(html_file),
        "university": None,
        "director": None,
        "program": None,
        "plan": None,
        "recommended": None,
        "confirmed": None,
        "total_places": None,
        "total_registered": None,
        "admitted_count": 0,
        "not_admitted_count": 0,
        "tables": []
    }

    # Университет и руководитель
    top_block = soup.select_one("div.text-right")
    if top_block:
        spans = top_block.find_all("span")
        if len(spans) >= 1:
            data["university"] = clean_text(spans[0])
        if len(spans) >= 2:
            data["director"] = clean_text(spans[1])

    # Программа
    program_tag = soup.select_one("p.headerColor b")
    if program_tag:
        data["program"] = clean_text(program_tag)

    # План / рекомендовано / подтверждено / итоги
    for p in soup.find_all("p"):
        txt = clean_text(p)
        if not txt:
            continue
        if "План набора" in txt:
            data["plan"] = txt
        elif "Рекомендовано" in txt:
            data["recommended"] = txt
        elif "Подтверждено" in txt:
            data["confirmed"] = txt
        elif "Итого вакантных мест" in txt:
            data["total_places"] = txt
        elif "Итого зарегистрированных" in txt:
            data["total_registered"] = txt

    # Таблицы
    for table in soup.select("table.table"):
        header = clean_text(table.select_one("div.cityColir"))

        # категория = часть до двоеточия
        category_value = None
        if header:
            category_value = header.split(":")[0].strip()

        records = []
        for tr in table.select("tbody tr"):
            cols = [clean_text(td) for td in tr.find_all(["td", "th"])]
            if not cols or len(cols) < 2:
                continue

            cert_text, admitted, note = parse_certificate(cols[1])

            record = {
                "num": cols[0],
                "certificate": cert_text,
                "note": note,
                "main_score": cols[2] if len(cols) > 2 else None,
                "extra_score": cols[3] if len(cols) > 3 else None,
                "total_score": cols[4] if len(cols) > 4 else None,
                "category": category_value,
                "date": None,
                "admitted": admitted
            }

            if len(cols) == 6:
                record["date"] = cols[5]
            elif len(cols) >= 7:
                record["date"] = cols[6]

            # считаем статистику
            if admitted:
                data["admitted_count"] += 1
            else:
                data["not_admitted_count"] += 1

            records.append(record)

        data["tables"].append({
            "header": header,
            "records": records
        })

    return data

def main():
    for file in os.listdir(INPUT_DIR):
        if file.startswith("personalcabinet_report_Ranjir") and file.endswith(".html"):
            full_path = os.path.join(INPUT_DIR, file)
            parsed = parse_rating(full_path)

            # имя json
            if "Ranjirk" in file:
                out_name = f"rating_r_{os.path.splitext(file)[0]}.json"
            elif "Ranjirb" in file:
                out_name = f"rating_b_{os.path.splitext(file)[0]}.json"
            else:
                continue

            out_path = os.path.join(OUTPUT_DIR, out_name)
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(parsed, f, ensure_ascii=False, indent=2)

            print(f"✅ Сохранено: {out_path}")

if __name__ == "__main__":
    main()