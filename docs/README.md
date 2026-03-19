# UNISCORES Static Frontend

Этот фронтенд работает полностью статично:

- не требует запуска FastAPI;
- читает данные из `data/dataset.json`;
- показывает вузы, программы, туры и 3 типа статистики:
  по основному баллу, по дополнительным баллам (если есть доп),
  по общему баллу (если есть доп).

## 1. Экспорт данных из MySQL

Из папки `backend` выполните:

```bash
/opt/homebrew/bin/python3.10 tools/export_static_dataset.py \
  --db-url "mysql+pymysql://root:root@127.0.0.1:8889/ort_uniscores?charset=utf8mb4" \
  --output ../frontend-static/data/dataset.json
```

Если у вас другой хост/порт/пользователь/пароль, поменяйте `--db-url`.

## 2. Запуск статичного сайта

Из папки `frontend-static`:

```bash
python3 -m http.server 5500
```

Откройте в браузере:

`http://127.0.0.1:5500`

## 3. Обновление данных

Повторно запустите экспорт из шага 1, затем обновите страницу в браузере.
