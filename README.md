# Event Tracker & Daily Active Users (DAU) Calculator

Проєкт для трекінгу подій користувачів та аналітики.

## 🚀 Запуск проекту

Проєкт запускається через Docker Compose:

```bash
docker-compose up --build
```

Активація .venv для тестування без докера.

```bash
python -m venv .venv
```

Windows
```
.venv\Scripts\activate
```
Linux/macOS
```
source .venv/bin/activate
```

Встановити залежності
```bash
pip install -r requirements.txt
```

Імпорт Events з CSV:
```
python -m cli.import_events data/events_sample.csv
```

Performance Benchmark:
```
python benchmark.py
```
- Методика: створив 100k подій, 1000 унікальних користувачів, вставив у базу та порахував DAU
- Результати:
  - Inserted 100k events in 37.33 sec
  - DAU computed in 0.02 sec
- Вузькі місця:
  - Bulk insert працює повільно через стандартні сесії SQLAlchemy
- Оптимізації:
  - Використати bulk_insert_mappings
  - Додати індекси на `user_id` та `occurred_at`
  - Кешувати DAU на день(при потребі)

Testing:
```
python -m pytest -v app/tests
```

Імпорт Events з CSV:
```
python -m cli.import_events data/events_sample.csv
```
