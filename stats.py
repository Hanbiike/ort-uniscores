import os
import json
import statistics
from collections import defaultdict

INPUT_DIR = "universities"   # где лежат university_*.json
OUTPUT_FILE = "stats.json"


def safe_stats(values):
    """Считает min/avg/max если список не пустой"""
    if not values:
        return None
    return {
        "min": min(values),
        "avg": round(statistics.mean(values), 2),
        "max": max(values)
    }


def process_students(students):
    """Считает статистику по списку студентов (только admitted == true)"""
    admitted = [s for s in students if s.get("admitted")]
    scores = [int(s["total_score"]) for s in admitted if s.get("total_score")]

    by_category = defaultdict(list)
    for s in admitted:
        if s.get("total_score"):
            cat = s.get("category") or "Неизвестно"
            by_category[cat].append(int(s["total_score"]))

    return {
        "scores": safe_stats(scores),
        "by_category": {c: safe_stats(vals) for c, vals in by_category.items()}
    } if admitted else None


def process_payment_amount(directions):
    """Для контрактов собирает все суммы оплаты"""
    amounts = []
    for d in directions:
        if d.get("payment_form") == "Контракт":
            try:
                val = int(d.get("payment_amount", "0"))
                if val > 0:
                    amounts.append(val)
            except ValueError:
                continue
    return safe_stats(amounts)


def group_directions_by_code(directions):
    """Группирует направления по коду, а внутри по форме оплаты"""
    grouped = defaultdict(lambda: {"forms": {}})
    for d in directions:
        code = d.get("code") or "Без кода"
        pf = d.get("payment_form") or "Неизвестно"

        stats = process_students(d.get("students", []))
        grouped[code]["code"] = code
        grouped[code]["major"] = d.get("major")
        grouped[code]["specialty"] = d.get("specialty")

        grouped[code]["forms"][pf] = {
            "stats": stats or "Нет поступивших"
        }

        # если контракт — добавляем оплату
        if pf == "Контракт":
            try:
                val = int(d.get("payment_amount", "0"))
                if val > 0:
                    grouped[code]["forms"][pf]["payment_amount"] = val
            except ValueError:
                pass

    return list(grouped.values())


def build_stats(universities):
    global_stats = {"Бюджет": [], "Контракт": [], "Ваучер": []}
    results = []

    for uni in universities:
        uni_stats = {"name": uni["name"], "faculties": []}
        uni_scores = {"Бюджет": [], "Контракт": [], "Ваучер": []}

        for fac in uni.get("faculties", []):
            fac_stats = {"faculty_name": fac["faculty_name"], "directions": []}
            fac_scores = {"Бюджет": [], "Контракт": [], "Ваучер": []}

            grouped_dirs = group_directions_by_code(fac.get("directions", []))

            for gd in grouped_dirs:
                fac_stats["directions"].append(gd)

                # собираем в факультет/универ/глобал
                for pf, form_data in gd["forms"].items():
                    if isinstance(form_data["stats"], dict) and form_data["stats"]["scores"]:
                        vals = [v for v in [form_data["stats"]["scores"]["min"],
                                            form_data["stats"]["scores"]["avg"],
                                            form_data["stats"]["scores"]["max"]] if v is not None]
                        if vals:
                            fac_scores[pf].extend(vals)
                            uni_scores[pf].extend(vals)
                            global_stats[pf].extend(vals)

            # сводка по факультету
            fac_stats["summary"] = {pf: safe_stats(fac_scores[pf]) or "Нет данных" for pf in fac_scores}
            fac_stats["payment_amounts"] = process_payment_amount(fac.get("directions", []))
            uni_stats["faculties"].append(fac_stats)

        # сводка по университету
        uni_stats["summary"] = {pf: safe_stats(uni_scores[pf]) or "Нет данных" for pf in uni_scores}
        uni_stats["payment_amounts"] = process_payment_amount(
            [d for f in uni.get("faculties", []) for d in f.get("directions", [])]
        )
        results.append(uni_stats)

    # глобальная сводка
    global_summary = {pf: safe_stats(global_stats[pf]) or "Нет данных" for pf in global_stats}
    return {"global": global_summary, "universities": results}


def main():
    universities = []
    for file in os.listdir(INPUT_DIR):
        if file.startswith("university_") and file.endswith(".json"):
            with open(os.path.join(INPUT_DIR, file), "r", encoding="utf-8") as f:
                universities.append(json.load(f))

    stats = build_stats(universities)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    print(f"✅ Статистика сохранена в {OUTPUT_FILE}")


if __name__ == "__main__":
    main()