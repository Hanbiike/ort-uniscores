# ort-uniscores

Сайт-зеркало результатов поступления КР + стартовая реализация backend-а
для агрегации данных и расчета шанса поступления.

## Backend MVP

Новый backend находится в `backend/`:

- ETL парсинг HTML и загрузка в MySQL
- FastAPI endpoint-ы для вузов/программ и оценки шанса

Быстрый старт:

```bash
cd backend
cp .env.example .env
pip install -r requirements.txt
mysql -u <user> -p <db_name> < sql/schema.sql
python -m etl.cli --site-root ..
uvicorn app.main:app --reload
```

## Static Frontend (без backend API)

Готовый статичный интерфейс находится в `frontend-static/`.

1) Экспорт данных из MySQL в JSON:

```bash
cd backend
python tools/export_static_dataset.py \
	--db-url "mysql+pymysql://root:root@127.0.0.1:8889/ort_uniscores?charset=utf8mb4" \
	--output ../frontend-static/data/dataset.json
```

2) Запуск статичного сайта:

```bash
cd ../docs
python -m http.server 5500
```

Открыть в браузере: `http://127.0.0.1:5500`
