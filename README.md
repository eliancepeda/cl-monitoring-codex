# CL Monitoring

Локальный single-user read-only companion app for Crawlab.

Проект безопасно читает данные из Crawlab, строит локальную truth layer в SQLite и показывает минимальный dashboard. Он не делает write-операции в Crawlab, не хранит live токены в репозитории и не даёт браузеру ходить в Crawlab напрямую.

## Что это за инструмент

Это локальный инструмент наблюдения за своими пауками и расписаниями в Crawlab.

Он собирает локальную картину из `tasks`, `schedules`, `logs` и при необходимости `results`: что сейчас крутится, что зависло, что закончилось с проблемой и что восстановилось вручную. Итоговая правда для UI строится локально из нормализации, deterministic parser/status logic, SQLite и dashboard.

## Основные гарантии

- Все live обращения к Crawlab идут только через `ReadonlyCrawlabClient`.
- Клиент делает только `GET` по allowlist.
- Браузер и UI читают только локальную SQLite.
- Приложение по умолчанию и по design contract слушает только `127.0.0.1`.
- `python -m cl_monitoring.app` теперь является реальным local service entrypoint для штатного operator flow.

## Как теперь работает запуск

Обычный v1 запуск один:

```bash
./.venv/bin/python -m cl_monitoring.app
```

Этот запуск всегда поднимает web server. Дальше режим выбирается по env:

- Если заданы и `CRAWLAB_BASE_URL`, и `CRAWLAB_TOKEN`, приложение стартует в live local-service mode:
  - загружает settings из defaults, `.env`, затем process env;
  - нормализует `CRAWLAB_BASE_URL` и безопасно убирает хвост `/api`, если он пришёл из env;
  - открывает локальную SQLite и гарантирует schema/WAL;
  - создаёт единственный `ReadonlyCrawlabClient`;
  - выполняет initial `poller.sync_once(force=True)` до готовности сервиса;
  - запускает background poller и затем начинает обслуживать UI.
- Если обе live переменные отсутствуют, приложение стартует в SQLite-only fallback mode:
  - открывает локальную SQLite;
  - не создаёт Crawlab client;
  - не запускает poller;
  - показывает dashboard по локальной БД.
- Если задана только одна из live переменных, startup падает с явной configuration error. Тихого downgrade в offline mode здесь нет.

При clean shutdown live mode делает обратный порядок: останавливает background poller, закрывает `ReadonlyCrawlabClient`, затем закрывает SQLite writer connection.

## Runtime env contract

Штатный runtime contract теперь такой:

| Переменная | Назначение | Обязательность |
| --- | --- | --- |
| `CRAWLAB_BASE_URL` | базовый URL Crawlab | обязателен только для live mode |
| `CRAWLAB_TOKEN` | runtime токен Crawlab | обязателен только для live mode |
| `CL_MONITORING_DB_PATH` | путь к локальной SQLite | опционально |
| `APP_PORT` | локальный HTTP порт | опционально |

Что важно:

- `APP_HOST` больше не является operator setting: bind фиксирован на `127.0.0.1`.
- Для нормального runtime path больше не нужен ad hoc экспорт `CRAWLAB_API_TOKEN`.
- `.env.example` и runtime app теперь говорят на одном token key: `CRAWLAB_TOKEN`.
- Если в `CRAWLAB_BASE_URL` пришёл адрес с хвостом `/api`, runtime сам нормализует его до базового host URL.

## Быстрый старт

1. Создать виртуальное окружение и поставить зависимости.

```bash
python3.11 -m venv .venv
./.venv/bin/pip install -e ".[dev]"
```

2. Подготовить `.env`.

```bash
cp .env.example .env
```

3. Для live local-service mode заполнить в `.env` минимум:

```dotenv
CRAWLAB_BASE_URL=https://your-crawlab-host
CRAWLAB_TOKEN=replace_me
```

Опционально можно задать:

```dotenv
CL_MONITORING_DB_PATH=/absolute/path/to/cl-monitoring.sqlite3
APP_PORT=8787
```

4. Запустить сервис.

```bash
./.venv/bin/python -m cl_monitoring.app
```

5. Открыть `http://127.0.0.1:8787/`.

## SQLite-only fallback mode

Если нужен только просмотр уже существующей локальной БД без live Crawlab, оставьте unset обе переменные:

- `CRAWLAB_BASE_URL`
- `CRAWLAB_TOKEN`

Тогда тот же запуск:

```bash
./.venv/bin/python -m cl_monitoring.app
```

поднимет только локальный web service поверх SQLite. Если файла БД ещё нет, он будет создан и UI откроется с пустым локальным состоянием.

## Типичный operator flow

Штатный flow для владельца проекта теперь такой:

1. Подготовить `.env` с `CRAWLAB_BASE_URL` и `CRAWLAB_TOKEN`.
2. Запустить `./.venv/bin/python -m cl_monitoring.app`.
3. Дождаться успешного startup: initial sync выполняется до ready state.
4. Открыть локальный UI на `127.0.0.1`.
5. Оставить тот же процесс работать: он держит и web server, и background poller.

Отдельный штатный poller daemon для обычного v1 flow не нужен.

## Компоненты

- `src/integrations/crawlab/readonly_client.py` - единственный live client к Crawlab.
- `src/cl_monitoring/crawlab/client.py` - thin runtime re-export того же client.
- `src/cl_monitoring/settings.py` - единый runtime settings layer.
- `src/cl_monitoring/db/engine.py` - SQLite connection management и WAL.
- `src/cl_monitoring/db/repo.py` - локальный repository.
- `src/cl_monitoring/sync/poller.py` - background sync и reparsing.
- `src/cl_monitoring/web/routes.py` - server-rendered dashboard routes.
- `src/cl_monitoring/app.py` - app factory, runtime lifecycle и entrypoint.

## Проверки

Основные команды проверки:

```bash
./.venv/bin/pytest -q
./.venv/bin/ruff check src tests
./.venv/bin/mypy src tests
```

Для test suite live Crawlab обычно не нужен: основная часть parser/status/runtime coverage остаётся offline.

## Ограничения v1

- Нет write-операций против Crawlab.
- Нет `run/restart/cancel` actions.
- Нет direct browser-side Crawlab calls.
- Нет multi-user auth.
- Нет `/nodes` logic.
- Нет новых UI экранов и action buttons сверх текущего minimal dashboard.
- Marker rollout всё ещё зависит от отдельного внешнего producer-side шага и не закрывается одним только T13.
