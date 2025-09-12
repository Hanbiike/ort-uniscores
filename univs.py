import os
import json

INPUT_DIR = "results"      # тут лежат rating_r_*.json и rating_b_*.json
OUTPUT_DIR = "universities"     # сохраняем туда же

def load_json(path):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None

def build_university_tree():
    # загружаем universities.json (структура без абитуриентов)
    with open(os.path.join(OUTPUT_DIR, "universities.json"), "r", encoding="utf-8") as f:
        universities = json.load(f)

    for uni in universities:
        uni_name_safe = uni["name"].replace(" ", "_").replace('"', "").replace("«", "").replace("»", "")
        out_path = os.path.join(OUTPUT_DIR, f"university_{uni_name_safe}.json")

        uni_full = {
            "name": uni["name"],
            "address": uni.get("address"),
            "rector": uni.get("rector"),
            "site": uni.get("site"),
            "faculties": []
        }

        for fac in uni["faculties"]:
            fac_block = {
                "faculty_name": fac["faculty_name"],
                "directions": []
            }
            for dirn in fac["directions"]:
                dir_block = {
                    "code": dirn["code"],
                    "major": dirn["major"],
                    "specialty": dirn["specialty"],
                    "education_type": dirn["education_type"],
                    "voucher": dirn["voucher"],
                    "payment_form": dirn["payment_form"],
                    "payment_amount": dirn["payment_amount"],
                    "plan": dirn["plan"],
                    "threshold": dirn["threshold"],
                    "registered": dirn["registered"],
                    "students": []
                }

                rating_json = dirn.get("rating_json")
                if rating_json and os.path.exists(rating_json):
                    rating_data = load_json(rating_json)
                    if rating_data:
                        # собираем все записи из всех таблиц
                        for tbl in rating_data.get("tables", []):
                            for rec in tbl.get("records", []):
                                dir_block["students"].append(rec)

                fac_block["directions"].append(dir_block)
            uni_full["faculties"].append(fac_block)

        # сохраняем университет отдельно
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(uni_full, f, ensure_ascii=False, indent=2)

        print(f"✅ Сохранён университет: {out_path}")

def main():
    build_university_tree()

if __name__ == "__main__":
    main()