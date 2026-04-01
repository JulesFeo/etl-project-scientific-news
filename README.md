# Multi-Source Science & Tech ETL Pipeline

ETL-процесс для инкрементальной загрузки научных статей из трех источников в нормализованную SQLite базу данных. Оркестрация через Apache Airflow (Docker), уведомления в Telegram.

## Источники данных
[OpenAlex](https://docs.openalex.org/) 
[arXiv](https://info.arxiv.org/help/api/) 
[PubMed](https://www.ncbi.nlm.nih.gov/home/develop/api/)

## Структура проекта

```
├── config/
│   └── config.yaml              # Конфигурация источников + Telegram
├── dags/
│   └── science_etl_dag.py       # Airflow DAG (ежедневный запуск)
├── src/
│   ├── extractors/
│   │   ├── base.py              # BaseExtractor (ABC + retry)
│   │   ├── openalex.py          # OpenAlex: cursor pagination, inverted abstract
│   │   ├── arxiv.py             # arXiv: feedparser, rate limit 3 сек
│   │   └── pubmed.py            # PubMed: esearch + efetch, MeSH tags
│   ├── transform.py             # Нормализация 3 форматов, дедупликация
│   ├── load.py                  # Нормализованная схема SQLite
│   ├── pipeline.py              # Оркестратор (цикл по enabled-источникам)
│   ├── notify.py                # Telegram-уведомления
│   └── logger.py                # Логирование с etl_id
├── data/                        # БД (создается автоматически)
├── logs/                        # Логи (создается автоматически)
├── main.py                      # Точка входа (ручной запуск)
├── Dockerfile                   # Образ Airflow + зависимости
├── docker-compose.yaml          # Airflow: webserver, scheduler, postgres
├── .env                         # Логин/пароль Airflow UI
├── requirements.txt
└── README.md
```

---

## Развертывание через Docker + Airflow

### Предварительные требования

- Docker и Docker Compose установлены
- Telegram 
- !ОБЯЗАТЕЛЬНО! включить ВПН перед запуском докера у себя на ПК

### Шаг 1. Запустить Airflow

```bash
docker compose build
docker compose up -d
```

При первом запуске Docker:
- Соберет образ с зависимостями проекта
- Поднимет PostgreSQL (для метаданных Airflow)
- Инициализирует базу Airflow и создаст пользователя
- Запустит веб-сервер и шедулер

Дождитесь, пока все сервисы запустятся (1-2 минуты):

```bash
docker compose ps
```

Все сервисы должны быть в статусе `running` или `healthy`.

### Шаг 4. Открыть Airflow UI

Перейдите в браузере по адресу:

```
http://localhost:8080
```
Логин и пароль (а также все данные по ТГ скину в письме)

### Шаг 5. Включить DAG

1. В списке DAG найдите **`science_tech_etl`**
2. Нажмите переключатель (toggle) слева для активации
3. DAG будет запускаться **каждый день автоматически**
4. Для немедленного запуска нажмите кнопку **Trigger DAG** (▶)

### Шаг 6. Проверить результат

После выполнения DAG:

- **Telegram**: бот отправит сообщение со сводкой (количество статей по источникам + список с названиями и ссылками)
- **Airflow UI**: можно посмотреть логи каждого таска в разделе Graph/Logs
- **База данных**: данные сохраняются в `data/science_etl.db`

Если новых статей нет, бот отправит сообщение: *"No new articles for this date."*

## Ручной запуск (без Docker/Airflow)

### 1. Установка зависимостей

```bash
python -m venv venv
# Linux/macOS:
source venv/bin/activate
# Windows:
venv\Scripts\activate

pip install -r requirements.txt
```

### 2. Запуск ETL

```bash
python main.py
```

---

## Конфигурация

| Параметр | Описание | По умолчанию |
|---|---|---|
| `sources.openalex.per_page` | Записей на страницу OpenAlex | `25` |
| `sources.openalex.max_records` | Максимум записей за запуск | `50` |
| `sources.openalex.language` | Фильтр языка | `en` |
| `sources.arxiv.categories` | Категории arXiv | `["cs.AI", "cs.LG"]` |
| `sources.arxiv.max_results` | Максимум препринтов | `50` |
| `sources.arxiv.request_delay` | Задержка между запросами (сек) | `3` |
| `sources.pubmed.query` | Поисковый запрос PubMed | `artificial intelligence OR machine learning` |
| `sources.pubmed.max_records` | Максимум статей | `50` |
| `sources.pubmed.email` | Email для NCBI (вежливость) | `etl-pipeline@example.com` |
| `sources.*.enabled` | Включить/выключить источник | `true` |
| `database.path` | Путь к SQLite | `data/science_etl.db` |
| `etl.default_start_date` | Начальная дата первого запуска | `2026-03-25` |
| `retry.max_attempts` | Максимум ретраев | `3` |
| `logging.level` | Уровень логирования | `INFO` |
| `telegram.bot_token` | Токен Telegram-бота | -- |
| `telegram.chat_id` | ID чата для уведомлений | -- |
| `telegram.enabled` | Включить уведомления | `true` |

то, что указано по умолчанию было описано в документации к апи сайтов

## Telegram-уведомления

Бот отправляет сообщение после каждого запуска ETL:

```
ETL Report  2026-03-29
etl_id: a1b2c3d4...

[+] openalex: success (15 articles)
  1. Attention Is All You Need  (ссылка)
  2. BERT: Pre-training of Deep...  (ссылка)
  ...

[+] arxiv: success (10 articles)
  1. On the Convergence of...  (ссылка)
  ...

[=] pubmed: no_new_data (0 articles)
    No new articles for this date.

Total loaded: 25
```

Если сообщение длинное (>4096 символов), оно автоматически разбивается на части.

## Проверка результатов (SQL)

```bash
# Все статьи с источниками
sqlite3 data/science_etl.db "
  SELECT s.name, COUNT(*) as cnt
  FROM articles a JOIN sources s ON a.source_id = s.id
  GROUP BY s.name;
"

# Последние 10 статей
sqlite3 data/science_etl.db "
  SELECT a.title, a.published_at, a.url
  FROM articles a
  ORDER BY a.published_at DESC LIMIT 10;
"

# История ETL-запусков
sqlite3 data/science_etl.db "
  SELECT etl_id, source_name, records_loaded, status, started_at
  FROM etl_runs ORDER BY started_at DESC;
"
```

## Схема базы данных

### Основные таблицы

**sources** -- реестр источников данных

| Поле | Тип | Описание |
|---|---|---|
| `id` | INTEGER (PK) | Автоинкремент |
| `name` | TEXT (UNIQUE) | Имя источника (openalex, arxiv, pubmed) |
| `base_url` | TEXT | Базовый URL API |

**articles** -- статьи из всех источников

| Поле | Тип | Описание |
|---|---|---|
| `id` | TEXT (PK) | Составной: `{source}:{external_id}` |
| `source_id` | INTEGER (FK) | Ссылка на sources |
| `external_id` | TEXT | ID в источнике (OpenAlex ID / arXiv ID / PMID) |
| `title` | TEXT | Заголовок |
| `abstract` | TEXT | Аннотация |
| `url` | TEXT | Ссылка |
| `doi` | TEXT | DOI |
| `language` | TEXT | Язык |
| `published_at` | TEXT | Дата публикации |
| `sentiment_score` | REAL | Зарезервировано (NULL) |
| `etl_id` | TEXT | ID запуска ETL |
| `loaded_at` | TEXT | Когда загружено |

### Связующие таблицы

**authors** / **article_authors** -- авторы статей (M:N, с позицией автора)

**tags** / **article_tags** -- теги/топики (M:N, с score). Источники тегов: OpenAlex topics, arXiv categories, PubMed MeSH terms + keywords.

**etl_runs** -- история запусков (PK: etl_id + source_name)
