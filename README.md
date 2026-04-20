# 2GIS Reviews Scraper — FastAPI Service

Асинхронный сервис поиска и сбора отзывов 2ГИС с хранением в PostgreSQL.

## Архитектура

```
POST /search/preview   ──→  search HTML  +  parallel firm-page fetch
                              (no reviews scrape, ~5–15s for 20 branches)
                              returns list of {id, name, address}
                                         │
                              user picks one or many
                                         │
                                         ▼
POST /search/scrape    ──→  SearchTask(pending)
   body: [ids, city]         ──→  BackgroundTasks.add_task
                                         │
                                         ▼
                              run_scrape_task(task_id, branches):
                                1. for each branch (concurrent, capped):
                                   - scrape_branch()  (address + reviews API)
                                   - UPSERT в БД
                                   - inc progress
                                2. status=completed

GET /tasks/{id}           ←──────────────  polling
GET /tasks/{id}/results   ←─── (status=completed)
GET /reviews/{uuid}       ←─── прямой доступ
```

## Схема БД

| Таблица | Назначение |
|---|---|
| `companies` | Уникальная организация (бренд). Ключ — имя (из `official_answer.org_name`). |
| `branches` | Конкретная точка. `gis_branch_id` = `/firm/<id>`. |
| `reviews` | Отзывы. `gis_review_id` из 2ГИС. В `raw` (JSONB) — полный исходник. |
| `search_tasks` | Журнал запусков с прогрессом и статусом. |
| `search_task_branches` | Многие-ко-многим: какую задачу какие филиалы покрыли. |

Идемпотентность: UPSERT на `gis_branch_id` и `gis_review_id`. Повторный запуск
запроса обновит тексты/рейтинги изменившихся отзывов.

## Установка

```bash
python -m venv .venv
# Windows PowerShell
.venv\Scripts\Activate.ps1
# Linux/Mac
# source .venv/bin/activate

pip install -r requirements.txt

cp .env.example .env
# Правь DATABASE_URL под свой Postgres

# Postgres должен быть запущен и БД 'twogis' создана:
# createdb twogis
# или через psql: CREATE DATABASE twogis;

uvicorn app.main:app --reload
```

Таблицы создаются автоматически на старте через `Base.metadata.create_all`.
**В проде замени на Alembic** — `create_all` не умеет миграций.

> **⚠️ Если БД была создана на v1.0:** колонка `search_tasks.query` стала nullable
> в v2.0. `create_all` НЕ меняет существующие колонки — нужно вручную:
> ```sql
> ALTER TABLE search_tasks ALTER COLUMN query DROP NOT NULL;
> ```
> Либо, для dev: `DROP TABLE search_tasks, search_task_branches CASCADE;` и
> перезапустить — `create_all` заново построит таблицы. Данные потеряются.

Swagger UI: http://localhost:8000/docs

## Использование

### 1. Поиск кандидатов (быстро, синхронно)

```bash
curl -X POST http://localhost:8000/search/preview \
  -H "Content-Type: application/json" \
  -d '{"query": "Underground", "city": "astana", "max_results": 20}'
```

Ответ — список кандидатов с именем и адресом, без отзывов:
```json
{
  "query": "Underground",
  "city": "astana",
  "count": 11,
  "branches": [
    {
      "gis_branch_id": 70000001041329707,
      "firm_url": "https://2gis.kz/astana/firm/70000001041329707",
      "name": "Underground gym, фитнес-клуб",
      "address": "Улица Жанибека Тархана, 17, 1 этаж, Целинный м-н, Байконыр район, Астана"
    },
    ...
  ]
}
```

### 2. Пользователь выбирает → запуск сбора

```bash
curl -X POST http://localhost:8000/search/scrape \
  -H "Content-Type: application/json" \
  -d '{
    "city": "astana",
    "gis_branch_ids": [70000001041329707, 70000001085622713],
    "query": "Underground"
  }'
```

Ответ (HTTP 202) — `task_id` для опроса:
```json
{
  "task_id": "3a7b8c9d-...",
  "status": "pending",
  "query": "Underground",
  "city": "astana"
}
```

### 3. Опрос прогресса

```bash
curl http://localhost:8000/tasks/3a7b8c9d-...
```

```json
{
  "task_id": "3a7b8c9d-...",
  "status": "running",
  "total_branches_found": 2,
  "branches_completed": 1,
  "total_reviews_collected": 1438,
  ...
}
```

### 4. Получение результатов

После `status=completed`:

```bash
curl http://localhost:8000/tasks/3a7b8c9d-.../results
```
```

Вернет все филиалы и отзывы в структуре, соответствующей старым JSON-дампам
оригинального скрипта. Передай `?include_reviews=false` чтобы получить только
метаданные филиалов.

### 4. Один отзыв

```bash
curl http://localhost:8000/reviews/a1b2c3d4-...
```

`review_url` в ответе — прямая ссылка на отзыв в 2ГИС:
`https://2gis.kz/reviews/{branch_id}/review/{review_id}`

## Известные ограничения

### 1. Поиск — HTML-скрап
`search_branches()` парсит HTML страницы `2gis.kz/{city}/search/{query}` и
извлекает все `/firm/<id>` ссылки. Это **хрупкий** компонент:
- Если 2ГИС поменяет разметку — возвращает 0 результатов
- Может упасть на bot detection при больших объемах

**Долгосрочный fix:** получить рабочий ключ в [dev.2gis.com](https://dev.2gis.com)
и заменить на Catalog API:
```
GET https://catalog.api.2gis.com/3.0/items?q={query}&key={key}
```
Это будет 1 http-вызов вместо HTML-скрапа. Заменить только тело
`search_branches()`, всё остальное останется.

### 2. BackgroundTasks = in-memory
Если процесс FastAPI умрет во время задачи — задача зависнет в статусе `running`.
`branches_completed` в БД покажет, где остановились. Частичные данные сохранены.

**Для прода:** arq (Redis), Celery, или Dramatiq. Схема БД уже готова — просто
замени `bg.add_task(...)` на `queue.enqueue(...)`.

Или: добавь в startup-хуке сброс `running → failed` для старых задач.

### 3. Удаленные отзывы не трекаются
Если отзыв удалили в 2ГИС, он останется в БД (UPSERT не delete). Для трекинга
удалений нужен soft-delete флаг и сверка "то, что было в прошлом scrape vs сейчас".

### 4. company_name может быть пустым
Если у филиала нет ни одного `official_answer` — `company_name` = None, и мы
пишем заглушку `"Неизвестная компания (branch_id=...)"`. Можно извлекать
настоящее имя с firm-страницы — это работа на +30 строк в scraper.

### 5. Rate limiting
Конкуренция ограничена `MAX_CONCURRENT_BRANCHES=5`. Внутри одного филиала —
последовательные запросы со sleep 1–2 сек. При агрессивном сборе 2ГИС может
начать возвращать 429 — тогда снижай `MAX_CONCURRENT_BRANCHES` или увеличивай
`RATE_LIMIT_SLEEP_*`.

## Структура

```
app/
  __init__.py
  config.py          # pydantic-settings из .env
  database.py        # async engine, Base, get_session
  models.py          # SQLAlchemy 2.0 typed models
  schemas.py         # Pydantic v2 request/response
  scraper.py         # чистая async логика скрапа (без БД)
  tasks.py           # оркестратор + upsert в БД
  main.py            # FastAPI routes
requirements.txt
.env.example
```

Разделение `scraper.py` vs `tasks.py` — намеренное: scraper ничего не знает о БД,
tasks знает только как сохранять. Это позволяет тестировать скрап без Postgres и
заменять хранилище не трогая скрап.
