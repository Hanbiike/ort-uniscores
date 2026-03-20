# ORT Aggregator Backend (Start Implementation)

Этот backend реализует стартовый MVP-слой:
- парсинг `reports*.html` и `downloaded/personalcabinet_report_*.html`;
- загрузку нормализованных данных в MySQL;
- API для справочников и rule-based оценки шанса поступления.

## 1) Установка

```bash
cd backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 2) Настройка окружения

```bash
cp .env.example .env
```

Укажите в `.env` корректный `DATABASE_URL`.

## 3) Создание схемы MySQL

Создайте БД, затем примените SQL:

```bash
mysql -u <user> -p <database_name> < sql/schema.sql
```

## 4) Импорт HTML в БД

Из папки `backend`:

```bash
python -m etl.cli --site-root ..
```

Если нужно явно передать DSN:

```bash
python -m etl.cli --site-root .. --db-url "mysql+pymysql://user:password@127.0.0.1:3306/ort_uniscores?charset=utf8mb4"
```

## 5) Запуск API

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Доступные endpoint-ы

- `GET /health`
- `GET /universities?q=<строка>&limit=<N>`
- `GET /universities/{university_id}/programs?q=<строка>&limit=<N>`
- `GET /programs/{program_id}/rounds`
- `POST /chance/evaluate`

Пример тела для `POST /chance/evaluate`:

```json
{
  "program_id": 123,
  "total_score": 210,
  "round_number": 1,
  "category_name": "Бишкек"
}
```

## Текущее ограничение

- Реализован стартовый rule-based расчет шанса без ML.
- Номера сертификатов не сохраняются в БД.
- В архиве наблюдается в основном `t-1`; схема поддерживает будущие туры.

## Статичный фронтенд без backend API

Есть отдельный фронт в `../frontend-static`, который работает без FastAPI.

1) Экспортируйте данные из MySQL в JSON:

```bash
python tools/export_static_dataset.py \
  --db-url "mysql+pymysql://root:root@127.0.0.1:8889/ort_uniscores?charset=utf8mb4" \
  --output ../frontend-static/data/dataset.json
```

2) Поднимите статичный сервер:

```bash
cd ../frontend-static
python -m http.server 5500
```

3) Откройте в браузере:

`http://127.0.0.1:5500`
